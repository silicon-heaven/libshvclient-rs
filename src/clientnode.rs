// The file originates from https://github.com/silicon-heaven/shv-rs/blob/e740fd301dc65f3412ad1154595bf61ee5632aba/src/shvnode.rs
// struct ShvNode has been adapted to support async process_request accepting RpcCommand channel and a shared state params

use crate::client::{RequestHandler, ClientCommandSender, MethodsGetter};
use crate::runtime::spawn_task;
use crate::AppState;
use log::{error, debug};
use shvrpc::rpcdiscovery::{DirParam, LsParam};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::{metamethod, RpcMessage, RpcMessageMetaTags};
use shvproto::rpcvalue;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::format;
use std::sync::Arc;
// Reexport for use in the macros
pub use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
pub use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
pub use shvproto::{RpcValue, Value};


fn dir<'a>(methods: impl IntoIterator<Item = &'a MetaMethod>, param: DirParam) -> RpcValue {
    let mut result = RpcValue::null();
    let mut lst = rpcvalue::List::new();
    for mm in methods {
        match param {
            DirParam::Brief => {
                lst.push(mm.to_rpcvalue(metamethod::DirFormat::IMap));
            }
            DirParam::Full => {
                lst.push(mm.to_rpcvalue(metamethod::DirFormat::Map));
            }
            DirParam::Exists(ref method_name) => {
                if mm.name == method_name {
                    result = mm.to_rpcvalue(metamethod::DirFormat::IMap);
                    break;
                }
            }
        }
    }
    if result.is_null() {
        lst.into()
    } else {
        result
    }
}

#[derive(Debug)]
pub(crate) enum RequestResult {
    Response(RpcValue),
    Error(RpcError),
}

pub(crate) fn process_local_dir_ls<V>(
    mounts: &BTreeMap<String, V>,
    frame: &RpcFrame,
) -> Option<RequestResult> {
    let method = frame.method().unwrap_or_default();
    if !(method == METH_DIR || method == METH_LS) {
        return None;
    }
    let shv_path = frame.shv_path().unwrap_or_default();
    let mount = find_longest_path_prefix(mounts, shv_path);
    let is_mount_point = mount.is_some();
    let children_on_path = children_on_path(mounts, shv_path);
    let is_leaf = match &children_on_path {
        None => is_mount_point,
        Some(dirs) => dirs.is_empty(),
    };
    if children_on_path.is_none() && !is_mount_point {
        // path doesn't exist
        return Some(RequestResult::Error(RpcError::new(
            RpcErrorCode::MethodNotFound,
            format!("Invalid shv path: {}", shv_path),
        )));
    }
    let is_real_node = mount.is_some_and(|(_, rest)|
        rest.is_empty() || children_on_path.is_none()
    );
    if method == METH_DIR && !is_real_node {
        // dir in the middle of the tree must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let dir = dir(DIR_LS_METHODS, rpcmsg.param().into());
            return Some(RequestResult::Response(dir));
        } else {
            return Some(RequestResult::Error(RpcError::new(
                RpcErrorCode::InvalidRequest,
                "Cannot convert RPC frame to RPC message".to_string(),
            )));
        }
    }
    if method == METH_LS && !is_leaf {
        // ls on not-leaf node must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let ls = ls_children_to_result(children_on_path, rpcmsg.param().into());
            return Some(ls);
        } else {
            return Some(RequestResult::Error(RpcError::new(
                RpcErrorCode::InvalidRequest,
                "Cannot convert RPC frame to RPC message".to_string(),
            )));
        }
    }
    None
}
fn ls_children_to_result(children: Option<Vec<String>>, param: LsParam) -> RequestResult {
    match children {
        None => RequestResult::Error(RpcError::new(
                RpcErrorCode::MethodCallException,
                "Invalid shv path",
        )),
        Some(dirs) =>
            match param {
                LsParam::List => {
                    let res: rpcvalue::List = dirs.iter().map(RpcValue::from).collect();
                    RequestResult::Response(res.into())
                },
                LsParam::Exists(path) =>
                    RequestResult::Response(dirs.contains(&path).into()),
            }
    }
}
pub fn children_on_path<V>(mounts: &BTreeMap<String, V>, path: impl AsRef<str>) -> Option<Vec<String>> {
    let path = path.as_ref();
    let mut dirs: Vec<String> = Vec::new();
    let mut unique_dirs: HashSet<String> = HashSet::new();
    let mut dir_exists = mounts.contains_key(path);
    for (key, _) in mounts.range(path.to_owned()..) {
        if key.starts_with(path) {
            if path.is_empty() || (key.len() > path.len() && key.as_bytes()[path.len()] == (b'/')) {
                dir_exists = true;
                let dir_rest_start = if path.is_empty() { 0 } else { path.len() + 1 };
                let mut updirs = key[dir_rest_start..].split('/');
                if let Some(dir) = updirs.next() {
                    if !dir.is_empty() && !unique_dirs.contains(dir) {
                        dirs.push(dir.to_string());
                        unique_dirs.insert(dir.to_string());
                    }
                }
            }
        } else {
            break;
        }
    }
    if dir_exists {
        Some(dirs)
    } else {
        None
    }
}

/// Helper trait for uniform access to some common methods of BTreeMap<String, V> and HashMap<String, V>
pub trait StringMapView<V> {
    fn contains_key_(&self, key: &str) -> bool;
}

impl<V> StringMapView<V> for BTreeMap<String, V> {
    fn contains_key_(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

impl<V> StringMapView<V> for HashMap<String, V> {
    fn contains_key_(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

pub fn find_longest_path_prefix<'a, V>(
    map: &impl StringMapView<V>,
    shv_path: &'a str,
) -> Option<(&'a str, &'a str)> {
    let mut path = shv_path;
    let mut rest = "";
    loop {
        if map.contains_key_(path) {
            return Some((path, rest));
        }
        if path.is_empty() {
            break;
        }
        if let Some(slash_ix) = path.rfind('/') {
            path = &shv_path[..slash_ix];
            rest = &shv_path[(slash_ix + 1)..];
        } else {
            path = "";
            rest = shv_path;
        };
    }
    None
}

pub struct Route<T> {
    pub handler: RequestHandler<T>,
    pub methods: Vec<String>,
}

impl<T> Route<T> {
    pub fn new<I>(methods: I, handler: RequestHandler<T>) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        Self {
            handler,
            methods: methods.into_iter().map(|x| x.into()).collect(),
        }
    }
}

type StaticNodeHandlers<T> = BTreeMap<String, Arc<RequestHandler<T>>>;

struct FixedNode<'a, T> {
    methods: Vec<&'a MetaMethod>,
    handlers: StaticNodeHandlers<T>,
}

impl<'a, T> FixedNode<'a, T> {
    fn new(methods: impl IntoIterator<Item = &'a MetaMethod>, routes: impl IntoIterator<Item = Route<T>>) -> Self {
        let methods = DIR_LS_METHODS.into_iter().chain(methods).collect::<Vec<&MetaMethod>>();
        let handlers = Self::add_routes(&methods, routes);
        Self {
            methods,
            handlers,
        }
    }

    fn add_routes(methods: &[&'a MetaMethod], routes: impl IntoIterator<Item = Route<T>>) -> StaticNodeHandlers<T> {
        if let Some(dup_method) = methods.iter().enumerate().find_map(|(i,mm)| methods[i+1..].iter().find(|m| m.name == mm.name)) {
            panic!("Duplicate method '{}' in a static node definition", dup_method.name);
        }
        let mut handlers: StaticNodeHandlers<T> = Default::default();
        fn is_signal(method: &MetaMethod) -> bool {
            method.flags & (Flag::IsSignal as u32) != 0u32
        }
        for route in routes {
            if route.methods.iter().any(|m| m == METH_DIR) {
                panic!("Custom implementation of 'dir', which is handled by the library");
            }
            let handler = Arc::new(route.handler);
            route.methods.iter().for_each(|m| {
                methods
                    .iter()
                    .find(|dm| dm.name == m && !is_signal(dm))
                    .unwrap_or_else(|| panic!("Invalid method {m}"));
                handlers.insert(m.clone(), handler.clone());
            });
        }
        if let Some(unhandled_method) = methods.iter().find(|mm| !is_signal(mm)
                                                            && ![METH_DIR, METH_LS].contains(&mm.name)
                                                            && !handlers.contains_key(mm.name))
        {
            panic!("No handler found for method '{}' of a static node", unhandled_method.name);
        }
        handlers
    }
}

struct DynamicNode<T> {
    methods: MethodsGetter<T>,
    handler: RequestHandler<T>,
}

pub trait ConstantNode {
    fn methods(&self) -> Vec<&MetaMethod>;
    fn process_request(&self, request: &RpcMessage) -> Option<Result<RpcValue, RpcError>>;
}

// NOTE: Implementing Steady and Dynamic nodes using async trait would allow to
// remove Constant variant. Steady node would have only one handler for the whole node.

enum NodeVariant<'a, T> {
    Fixed(FixedNode<'a, T>),
    Dynamic(Arc<DynamicNode<T>>),
    Constant(Box<dyn ConstantNode + Send + Sync>),
}

pub struct ClientNode<'a, T>(NodeVariant<'a, T>);

impl<'a, T: Send + Sync + 'static> ClientNode<'a, T> {
    pub fn fixed(methods: impl IntoIterator<Item = &'a MetaMethod>, routes: impl IntoIterator<Item = Route<T>>) -> Self {
        Self(NodeVariant::Fixed(FixedNode::new(methods, routes)))
    }

    pub fn dynamic(methods: MethodsGetter<T>, handler: RequestHandler<T>) -> Self {
        Self(NodeVariant::Dynamic(Arc::new(DynamicNode { methods, handler })))
    }

    // NOTE: Not included in the public API. Constant nodes are meant
    // for implementation of special nodes like .app and .device and
    // should not be needed outside of the library.
    pub(crate) fn constant<N>(node: N) -> Self
    where
        N: ConstantNode + Send + Sync + 'static,
    {
        Self(NodeVariant::Constant(Box::new(node)))
    }

    pub(crate) async fn process_request(&self, request: RpcMessage, mount_path: String, client_cmd_tx: ClientCommandSender<T>, app_state: &Option<AppState<T>>) {
        match &self.0 {
            NodeVariant::Fixed(node) => {
                let methods = if request.shv_path().unwrap_or_default().is_empty() {
                    node.methods.as_slice()
                } else {
                    // Static nodes do not have any own children. Any child nodes are
                    // resolved on the mounts tree level in `process_local_dir_ls()`.
                    &[]
                };
                if resolve_request_access(&request, &mount_path, &client_cmd_tx, methods) {
                    let Some(method) = request.method() else {
                        panic!("BUG: Request method should be Some after access check.");
                    };
                    if method == self::METH_DIR {
                        let result = dir(methods.iter().copied(), request.param().into());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else if let Some(handler) = node.handlers.get(method) {
                        spawn_task(handler.0(request, client_cmd_tx, app_state.clone()));
                    } else if method == self::METH_LS {
                        let result = default_ls(request.param());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else {
                        panic!("BUG: Unhandled method '{mount_path}:{method}()' should have been caught in the node constructor");
                    }
                }
            },
            NodeVariant::Dynamic(node) => {
                let app_state = app_state.clone();
                let shv_path = request.shv_path().unwrap_or_default().to_owned();
                let node = node.clone();
                spawn_task(async move {
                    let methods = node.methods.0(shv_path, app_state.clone()).await
                        .map_or_else(
                            || Cow::from(&[]),
                            |m| if m.is_empty() {
                                Cow::from(&DIR_LS_METHODS)
                            } else {
                                DIR_LS_METHODS.into_iter().chain(m.iter().copied()).collect()
                            });
                    if resolve_request_access(&request, &mount_path, &client_cmd_tx, &methods) {
                        match request.method() {
                            Some(self::METH_DIR) => {
                                let result = dir(methods.iter().copied(), request.param().into());
                                send_response(request, client_cmd_tx, Ok(result));
                            }
                            Some(_) =>
                                node.handler.0(request, client_cmd_tx, app_state).await,
                            _ =>
                                panic!("BUG: Request method should be Some after access check."),
                        };
                    }
                });
            },
            NodeVariant::Constant(node) => {
                let methods = if request.shv_path().unwrap_or_default().is_empty() {
                    DIR_LS_METHODS.into_iter().chain(node.methods()).collect()
                } else {
                    // Static nodes do not have any own children. Any child nodes are
                    // resolved on the mounts tree level in `process_local_dir_ls()`.
                    Cow::from(&[])
                };
                if resolve_request_access(&request, &mount_path, &client_cmd_tx, &methods) {
                    let Some(method) = request.method() else {
                        panic!("BUG: Request method should be Some after access check.");
                    };
                    if method == self::METH_DIR {
                        let result = dir(methods.iter().copied(), request.param().into());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else if let Some(result) = node.process_request(&request) {
                        send_response(request, client_cmd_tx, result);
                    } else if method == self::METH_LS {
                        let result = default_ls(request.param());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else {
                        panic!("BUG: Unhandled method '{mount_path}:{method}()' should have been caught in the node constructor");
                    }
                }
            },
        }
    }
}

fn resolve_request_access<T>(request: &RpcMessage, mount_path: &String, client_cmd_tx: &ClientCommandSender<T>, methods: &[&MetaMethod]) -> bool {

    let shv_path = request.shv_path().unwrap_or_default();
    let check_request_access = || {
        let method = request.method().unwrap_or_default();
        let full_path = if shv_path.is_empty() {
            mount_path
        } else {
            &format!("{mount_path}/{shv_path}")
        };
        let Some(mm) = methods.iter().find(|mm| mm.name == method) else {
            return Err(RpcError::new(RpcErrorCode::MethodNotFound,
                                     format!("Unknown method on path '{full_path}:{method}()'")));
        };
        let Some(rq_level) = request.access_level() else {
            return Err(RpcError::new(RpcErrorCode::InvalidRequest, "Undefined access level"));
        };
        if rq_level >= mm.access as i32 {
            Ok(())
        } else {
            Err(RpcError::new(
                    RpcErrorCode::PermissionDenied,
                    format!("Insufficient permissions. \
                            Method '{full_path}:{method}()' \
                            called with access level {:?}, required {} ({:?})",
                            rq_level,
                            mm.access as i32,
                            mm.access,
                            )
                    )
               )
        }
    };

    let Err(err) = check_request_access() else {
        return true;
    };
    let mut resp = request.prepare_response()
        .expect("should be able to prepare response");
    debug!("Check request access on path `{}` / `{}`, error: {}",
          mount_path,
          shv_path,
          err);
    resp.set_error(err);
    let _ = client_cmd_tx.send_message(resp);
    false
}

pub fn send_response<T>(request: RpcMessage, client_cmd_tx: ClientCommandSender<T>, result: Result<RpcValue, RpcError>) {
    match request.prepare_response() {
        Err(err) => {
            error!("Cannot prepare response. Error: {err}, request: {request}");
        }
        Ok(mut resp) => {
            match result {
                Ok(result) => resp.set_result(result),
                Err(err) => resp.set_error(err),
            };
            if let Err(e) = client_cmd_tx.send_message(resp) {
                error!("Cannot send response. Error: {e}, request: {request}");
            }
        }
    }
}

pub fn default_ls(rq_param: Option<&RpcValue>) -> RpcValue {
    match LsParam::from(rq_param) {
        LsParam::List => rpcvalue::List::new().into(),
        LsParam::Exists(_path) => false.into(),
    }
}


pub const METH_DIR: &str = "dir";
pub const METH_LS: &str = "ls";
pub const METH_GET: &str = "get";
pub const METH_SET: &str = "set";
pub const SIG_CHNG: &str = "chng";
pub const METH_PING: &str = "ping";

pub(crate) const DIR_LS_METHODS: [&MetaMethod; 2] = [
    &MetaMethod {
        name: METH_DIR,
        flags: Flag::None as u32,
        access: AccessLevel::Browse,
        param: "DirParam",
        result: "DirResult",
        signals: &[],
        description: "",
    },
    &MetaMethod {
        name: METH_LS,
        flags: Flag::None as u32,
        access: AccessLevel::Browse,
        param: "LsParam",
        result: "LsResult",
        signals: &[],
        description: "",
    }
];

pub const META_METHOD_GET: MetaMethod = MetaMethod {
        name: METH_GET,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Read,
        param: "",
        result: "",
        signals: &[],
        description: "",
    };

pub const META_METHOD_SET: MetaMethod = MetaMethod {
        name: METH_SET,
        flags: Flag::IsSetter as u32,
        access: AccessLevel::Write,
        param: "",
        result: "",
        signals: &[],
        description: "",
    };

pub const META_METHOD_SIG_CHNG: MetaMethod = MetaMethod {
        name: SIG_CHNG,
        flags: Flag::IsSignal as u32,
        access: AccessLevel::Read,
        param: "",
        result: "",
        signals: &[],
        description: "",
    };

pub const PROPERTY_METHODS: [&MetaMethod; 3] = [
    &META_METHOD_GET,
    &META_METHOD_SET,
    &META_METHOD_SIG_CHNG,
];


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ls_mounts() {
        let mut mounts = BTreeMap::new();
        mounts.insert("".into(), ());
        mounts.insert("a".into(), ());
        mounts.insert("a/1".into(), ());
        mounts.insert("a/123".into(), ());
        mounts.insert("a/xyz".into(), ());
        mounts.insert("b/2/C".into(), ());
        mounts.insert("b/2/D".into(), ());
        mounts.insert("b/3/E".into(), ());
        assert_eq!(
            super::children_on_path(&mounts, ""),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            super::children_on_path(&mounts, "a"),
            Some(vec!["1".to_string(), "123".to_string(), "xyz".to_string()])
        );
        assert_eq!(
            super::children_on_path(&mounts, "a/1"),
            Some(vec![])
        );
        assert_eq!(
            super::children_on_path(&mounts, "a/xy"),
            None
        );
        assert_eq!(
            super::children_on_path(&mounts, "b/2"),
            Some(vec!["C".to_string(), "D".to_string()])
        );
    }

    async fn dummy_handler(_: RpcMessage, _: ClientCommandSender<()>, _: Option<AppState<()>>) {}

    #[test]
    fn accept_valid_routes() {
        ClientNode::fixed(PROPERTY_METHODS,
                            [Route::new([METH_GET, METH_SET, METH_LS], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    fn accept_valid_routes_without_ls() {
        ClientNode::fixed(PROPERTY_METHODS,
                            [Route::new([METH_GET, METH_SET], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_sig_chng_route() {
        ClientNode::fixed(PROPERTY_METHODS,
                            [Route::new([METH_GET, METH_SET, METH_LS, SIG_CHNG], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_custom_dir_handler() {
        ClientNode::fixed(PROPERTY_METHODS,
                            [Route::new([METH_GET, METH_SET, METH_DIR], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_invalid_method_route() {
        ClientNode::fixed(PROPERTY_METHODS, [Route::new(["invalidMethod"], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_unhandled_method() {
        ClientNode::fixed(PROPERTY_METHODS, [Route::new([METH_GET], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_duplicate_method() {
        let duplicate_methods = PROPERTY_METHODS.into_iter().chain(DIR_LS_METHODS);
        ClientNode::fixed(duplicate_methods, [Route::new([METH_GET, METH_SET, METH_LS], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    fn create_fixed_node() {
        let node: crate::clientnode::ClientNode<'_, ()> = crate::fixed_node!{
            device_handler<()>(request, _tx) {
                "echo" [IsGetter, Browse, "", ""] (param: i32) => {
                    Some(Ok(param.into()))
                }
            }
        };

        let NodeVariant::Fixed(FixedNode { methods, handlers }) = node.0 else {
            panic!("Not a fixed node");
        };
        assert_eq!(methods.len(), 3, "Expected 3 methods");
        assert_eq!(methods[0].name, "dir");
        assert_eq!(methods[1].name, "ls");
        assert_eq!(methods[2].name, "echo");
        assert_eq!(handlers.len(), 1, "Expected 1 handler");

    }

    #[test]
    fn longest_path_prefix() {
        let map = BTreeMap::from([
            ("".to_string(), ()),
            ("foo".to_string(), ()),
            ("foo/bar".to_string(), ()),
            ("x".to_string(), ())
        ]);
        assert_eq!(find_longest_path_prefix(&map, ""), Some(("", "")));
        assert_eq!(find_longest_path_prefix(&map, "blah"), Some(("", "blah")));
        assert_eq!(find_longest_path_prefix(&map, "foo/blah"), Some(("foo", "blah")));
        assert_eq!(find_longest_path_prefix(&map, "/"), Some(("", "")));
    }

    fn make_request_frame(path: &str, method: &str, param: Option<RpcValue>) -> RpcFrame {
        RpcMessage::new_request(path, method, param)
            .to_frame()
            .unwrap()
    }

    #[test]
    fn local_dir_ls_with_root() {
        let mounts = BTreeMap::from([
            ("".to_string(), ()),
            ("foo/x".to_string(), ()),
            ("foo/bar".to_string(), ()),
            ("x".to_string(), ())
        ]);

        // dir
        assert!(process_local_dir_ls(&mounts, &make_request_frame("", METH_DIR, None)).is_none());
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("blah", METH_DIR, None))
            .is_none()
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("foo", METH_DIR, None))
            .is_some_and(|res| {
                let RequestResult::Response(resp) = res else {
                    panic!("Not a response");
                };
                dir(DIR_LS_METHODS, DirParam::Brief) == resp
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x", METH_DIR, None)).is_none());
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x/y", METH_DIR, None)).is_none());

        // ls
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["foo".into(), "x".into()]), LsParam::List), res), (RequestResult::Response(a), RequestResult::Response(b)) if a == b)
            })
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("blah", METH_LS, None))
            .is_none()
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("foo", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["bar".into(), "x".into()]), LsParam::List), res), (RequestResult::Response(a), RequestResult::Response(b)) if a == b)
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x", METH_LS, None)).is_none());
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x/y", METH_LS, None)).is_none());
    }

    #[test]
    fn local_dir_ls_without_root() {
        let mounts = BTreeMap::from([
            ("foo/x".to_string(), ()),
            ("foo/bar".to_string(), ()),
            ("x".to_string(), ())
        ]);

        // dir
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("", METH_DIR, None))
            .is_some_and(|res| {
                let RequestResult::Response(resp) = res else {
                    panic!("Not a response");
                };
                dir(DIR_LS_METHODS, DirParam::Brief) == resp
            })
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("blah", METH_DIR, None))
            .is_some_and(|resp| matches!(resp, RequestResult::Error(_)))
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("foo", METH_DIR, None))
            .is_some_and(|res| {
                let RequestResult::Response(resp) = res else {
                    panic!("Not a response");
                };
                dir(DIR_LS_METHODS, DirParam::Brief) == resp
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x", METH_DIR, None)).is_none());
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x/y", METH_DIR, None)).is_none());

        // ls
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["foo".into(), "x".into()]), LsParam::List), res), (RequestResult::Response(a), RequestResult::Response(b)) if a == b)
            })
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("blah", METH_LS, None))
            .is_some_and(|resp| matches!(resp, RequestResult::Error(_)))
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("foo", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["bar".into(), "x".into()]), LsParam::List), res), (RequestResult::Response(a), RequestResult::Response(b)) if a == b)
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x", METH_LS, None)).is_none());
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x/y", METH_LS, None)).is_none());
    }
}

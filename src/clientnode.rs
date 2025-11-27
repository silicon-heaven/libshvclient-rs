use crate::client::ClientCommandSender;
use crate::runtime::spawn_task;
use async_trait::async_trait;
use futures::future::BoxFuture;
use log::{error, debug};
use shvrpc::rpcdiscovery::{DirParam, LsParam};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::util::{children_on_path, find_longest_path_prefix};
use shvrpc::{metamethod, RpcMessage, RpcMessageMetaTags};
use shvproto::rpcvalue;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::format;
use std::sync::Arc;
// Reexport for use in the macros
pub use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
pub use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
pub use shvproto::{RpcValue, Value};


fn dir<'a>(methods: impl IntoIterator<Item = &'a MetaMethod>, param: impl Into<DirParam>) -> RpcValue {
    match param.into() {
        DirParam::Brief => {
            methods
                .into_iter()
                .map(|m| m.to_rpcvalue(metamethod::DirFormat::IMap))
                .collect::<Vec<_>>()
                .into()
        }
        DirParam::Full => {
            methods
                .into_iter()
                .map(|m| m.to_rpcvalue(metamethod::DirFormat::Map))
                .collect::<Vec<_>>()
                .into()
        }
        DirParam::Exists(ref method_name) => {
            methods
                .into_iter()
                .find(|m| m.name.as_ref() == method_name)
                .map(|m| m.to_rpcvalue(metamethod::DirFormat::IMap))
                .unwrap_or(false.into())
        }
    }
}

pub type RequestResult = Result<RpcValue, RpcError>;

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
        return Some(RequestResult::Err(RpcError::new(
            RpcErrorCode::MethodNotFound,
            format!("Invalid shv path: {shv_path}"),
        )));
    }

    // Note: `tree` here means our mountpoint tree. The path can still become a part of the tree
    // via a dyanmic node.
    let is_in_tree = children_on_path.is_some();
    let is_direct_mountpoint = mount.is_some_and(|(_, rest)| rest.is_empty());

    if method == METH_DIR && is_in_tree && !is_direct_mountpoint {
        // dir in the middle of the tree must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let dir = dir(DIR_LS_METHODS, rpcmsg.param());
            return Some(RequestResult::Ok(dir));
        } else {
            return Some(RequestResult::Err(RpcError::new(
                RpcErrorCode::InvalidRequest,
                "Cannot convert RPC frame to RPC message".to_string(),
            )));
        }
    }
    if method == METH_LS && !is_leaf {
        // ls on not-leaf node must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let ls = ls_children_to_result(children_on_path, rpcmsg.param());
            return Some(ls);
        } else {
            return Some(RequestResult::Err(RpcError::new(
                RpcErrorCode::InvalidRequest,
                "Cannot convert RPC frame to RPC message".to_string(),
            )));
        }
    }
    None
}

fn ls_children_to_result(children: Option<Vec<String>>, param: impl Into<LsParam>) -> RequestResult {
    match children {
        None => RequestResult::Err(RpcError::new(
                RpcErrorCode::MethodCallException,
                "Invalid shv path",
        )),
        Some(dirs) =>
            match param.into() {
                LsParam::List => {
                    let res: rpcvalue::List = dirs.iter().map(RpcValue::from).collect();
                    RequestResult::Ok(res.into())
                },
                LsParam::Exists(path) =>
                    RequestResult::Ok(dirs.contains(&path).into()),
            }
    }
}

#[async_trait]
pub trait StaticNode: Send + Sync + 'static {
    fn methods(&self) -> &'static [MetaMethod];
    async fn process_request(&self, request: RpcMessage, client_cmd_tx: ClientCommandSender) -> Option<RequestResult>;
}

pub struct StaticNodeWrapper(Arc<dyn StaticNode>);

pub struct RequestHandler(pub(crate) Arc<dyn Fn(RpcMessage, ClientCommandSender) -> BoxFuture<'static, RequestHandlerResult> + Sync + Send>);
pub struct MethodHandler(pub(crate) Box<dyn FnOnce(RpcMessage, ClientCommandSender) -> BoxFuture<'static, MethodHandlerResult<RpcValue>> + Sync + Send>);
pub struct LsHandler(pub(crate) Box<dyn FnOnce(RpcMessage, ClientCommandSender) -> BoxFuture<'static, LsHandlerResult> + Sync + Send>);

pub enum MethodHandlerType {
    Dir,
    Ls(LsHandler),
    Method {
        name: Cow<'static, str>,
        handler: MethodHandler,
    },
}

pub struct ResolvedRequest {
    pub methods: MetaMethods,
    pub handler: MethodHandlerType,
}

pub type RequestHandlerResult = Result<ResolvedRequest, RpcError>;
pub type MethodHandlerResult<T> = Option<Result<T, RpcError>>;
pub type LsHandlerResult = MethodHandlerResult<Vec<String>>;

pub type MetaMethods = Cow<'static, [MetaMethod]>;

pub enum ClientNode {
    Static(StaticNodeWrapper),
    Dynamic(RequestHandler),
}

impl RequestHandler {
    pub fn new<F, Fut>(func: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = RequestHandlerResult> + Send + Sync + 'static
    {
        Self(Arc::new(move |rq, tx| Box::pin(func(rq, tx))))
    }
}

impl MethodHandler {
    pub fn new<F, Fut, T>(func: F) -> Self
    where
        F: FnOnce(RpcMessage, ClientCommandSender) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = MethodHandlerResult<T>> + Send + Sync + 'static,
        T: Into<RpcValue>,
    {
        Self(Box::new(move |rq, tx| Box::pin(async move {
            func(rq, tx).await.map(|res| res.map(|val| val.into()))
        })))
    }
}

impl LsHandler {
    pub fn new<F, Fut>(func: F) -> Self
    where
        F: FnOnce(RpcMessage, ClientCommandSender) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = LsHandlerResult> + Send + Sync + 'static
    {
        Self(Box::new(move |rq, tx| Box::pin(func(rq, tx))))
    }
}


impl ClientNode {
    pub fn new_static(node: impl StaticNode) -> Self {
        Self::Static(StaticNodeWrapper(Arc::new(node)))
    }

    pub fn new_dynamic(handler: RequestHandler) -> Self {
        Self::Dynamic(handler)
    }

    pub(crate) async fn process_request(&self, request: RpcMessage, mount_path: String, client_cmd_tx: ClientCommandSender) {
        match &self {
            Self::Static(StaticNodeWrapper(node)) => {
                // TODO: Implement process_request for Node variant types and use it for tests
                let methods  = if request.shv_path().unwrap_or_default().is_empty() {
                    DIR_LS_METHODS.iter().chain(node.methods()).collect()
                } else {
                    // Static nodes do not have any own children. Any child nodes are
                    // resolved on the mounts tree level in `process_local_dir_ls()`.
                    Cow::from(&[])
                };
                if !resolve_request_access(&request, &mount_path, &client_cmd_tx, methods.iter().copied()) {
                    return
                }
                let Some(method) = request.method() else {
                    panic!("Request method should be Some after access check.");
                };
                if method == self::METH_DIR {
                    let result = dir(methods.iter().copied(), request.param());
                    send_response(&request, &client_cmd_tx, Ok(result));
                } else if method == self::METH_LS {
                    let result = default_ls(request.param());
                    send_response(&request, &client_cmd_tx, Ok(result));
                } else {
                    let node = node.clone();
                    spawn_task(async move {
                        if let Some(result) = node.process_request(request.clone(), client_cmd_tx.clone()).await {
                            send_response(&request, &client_cmd_tx, result);
                        }
                    }).detach();
                }
            },
            Self::Dynamic(RequestHandler(request_handler)) => {
                let request_handler = request_handler.clone();
                spawn_task(async move {
                    match request_handler(request.clone(), client_cmd_tx.clone()).await {
                        Ok(ResolvedRequest { methods, handler }) => {
                            fn get_method<'a>(methods: &'a MetaMethods, name: &str) -> Option<(usize, &'a MetaMethod)> {
                                methods
                                    .iter()
                                    .enumerate()
                                    .find(|(_, mm)| mm.name == name)
                            }
                            fn extract_second<A, B>(tuple: (A, B)) -> B { tuple.1 }
                            match handler {
                                MethodHandlerType::Dir => {
                                    // TODO: use also for StaticNode methods
                                    let all_methods = |methods: MetaMethods| if methods.is_empty() {
                                        Cow::from(DIR_LS_METHODS)
                                    } else {
                                        // Make sure that `dir` and `ls` are at the start of the result list. Use either app-provided or default definition.
                                        let dir_ls_methods = match (get_method(&methods, METH_DIR), get_method(&methods, METH_LS)) {
                                            (None, None) => [META_METHOD_DIR, META_METHOD_LS],
                                            (None, Some((_, mm_ls))) => [META_METHOD_DIR, mm_ls.to_owned()],
                                            (Some((_, mm_dir)), None) => [mm_dir.to_owned(), META_METHOD_LS],
                                            (Some((0, _mm_dir)), Some((1, _mm_ls))) => {
                                                // The methods are already in the correct order, so we can
                                                // return them right away.
                                                return methods
                                            }
                                            (Some((_, mm_dir)), Some((_, mm_ls))) => [mm_dir.to_owned(), mm_ls.to_owned()],
                                        };
                                        dir_ls_methods
                                            .into_iter()
                                            .chain(methods
                                                .into_owned()
                                                .into_iter()
                                                .filter(|mm| mm.name != METH_DIR && mm.name != METH_LS)
                                            )
                                            .collect()
                                    };
                                    let mm_dir = get_method(&methods, METH_DIR)
                                        .map_or(static_ref::META_METHOD_DIR, extract_second);
                                    let response = check_request_access_for_method(&request, &mount_path, mm_dir)
                                        .map(|_| dir(all_methods(methods).as_ref(), request.param()));
                                    send_response(&request, &client_cmd_tx, response);
                                }
                                MethodHandlerType::Ls(LsHandler(ls_handler)) => {
                                    let mm_ls = get_method(&methods, METH_LS)
                                        .map_or(static_ref::META_METHOD_LS, extract_second);
                                    if let Err(err) = check_request_access_for_method(&request, &mount_path, mm_ls) {
                                        send_response(&request, &client_cmd_tx, Err(err));
                                        return;
                                    }
                                    if let Some(ls_result) = ls_handler(request.clone(), client_cmd_tx.clone()).await {
                                        let response = ls_result
                                            .and_then(|children| ls_children_to_result(Some(children), request.param()));
                                        send_response(&request, &client_cmd_tx, response);
                                    }
                                },
                                MethodHandlerType::Method { name, handler: MethodHandler(handler) } => {
                                    let Some((_, mm)) = get_method(&methods, &name) else {
                                        let err = rpc_error_unknown_method_on_path(full_shv_path(mount_path, request.shv_path().unwrap_or_default()), name);
                                        send_response(&request, &client_cmd_tx, Err(err));
                                        return;
                                    };
                                    if let Err(err) = check_request_access_for_method(&request, &mount_path, mm) {
                                        send_response(&request, &client_cmd_tx, Err(err));
                                        return;
                                    }

                                    if let Some(result) = handler(request.clone(), client_cmd_tx.clone()).await {
                                        send_response(&request, &client_cmd_tx, result);
                                    }
                                }
                            }
                        }
                        Err(err) => send_response(&request, &client_cmd_tx, Err(err)),
                    }
                }).detach();
            },
        }
    }
}

pub fn rpc_error_unknown_method_on_path(path: impl Display, method: impl Display) -> RpcError {
    RpcError::new(
        RpcErrorCode::MethodNotFound,
        format!("Unknown method on path '{path}:{method}()'")
    )
}

pub fn full_shv_path<'a>(mount_path: impl Into<Cow<'a, str>> + Display, shv_path: impl Into<Cow<'a, str>>) -> String {
    let shv_path = shv_path.into();
    if shv_path.is_empty() {
        mount_path.into().to_string()
    } else {
        format!("{mount_path}/{shv_path}")
    }
}

fn check_request_access_for_method(rq: &RpcMessage, mount_path: impl AsRef<str>, method: &MetaMethod) -> Result<(), RpcError> {
    let Some(rq_level) = rq.access_level() else {
        return Err(RpcError::new(RpcErrorCode::InvalidRequest, "Undefined access level"));
    };
    if rq_level >= method.access as i32 {
        Ok(())
    } else {
        // Send a neutral error message so an unauthorized user wouldn't even know
        // that this path:method exists.
        let path = full_shv_path(mount_path.as_ref(), rq.shv_path().unwrap_or_default());
        Err(rpc_error_unknown_method_on_path(path, &method.name))

        // Err(RpcError::new(
        //         RpcErrorCode::PermissionDenied,
        //         format!("Insufficient permissions. \
        //             Method '{full_path}:{method}()' \
        //             called with access level {:?}, required {} ({:?})",
        //             rq_level,
        //             method.access as i32,
        //             method.access,
        //         )
        // )
        // )
    }
}

fn resolve_request_access<'a>(request: &RpcMessage, mount_path: &String, client_cmd_tx: &ClientCommandSender, methods: impl IntoIterator<Item = &'a MetaMethod>) -> bool {

    let shv_path = request.shv_path().unwrap_or_default();
    let check_request_access = || {
        let method = request.method().unwrap_or_default();
        let Some(mm) = methods.into_iter().find(|mm| mm.name == method) else {
            let path = full_shv_path(mount_path, request.shv_path().unwrap_or_default());
            return Err(rpc_error_unknown_method_on_path(path, method))
        };
        check_request_access_for_method(request, mount_path, mm)
    };

    let Err(err) = check_request_access() else {
        return true;
    };
    let mut resp = request.prepare_response()
        .expect("should be able to prepare response");
    debug!("Check request access on path `{mount_path}` / `{shv_path}`, error: {err}");
    resp.set_error(err);
    let _ = client_cmd_tx.send_message(resp);
    false
}

pub fn send_response(request: &RpcMessage, client_cmd_tx: &ClientCommandSender, result: Result<RpcValue, RpcError>) {
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

pub(crate) const DIR_LS_METHODS: &[MetaMethod] = &[META_METHOD_DIR, META_METHOD_LS];

pub mod static_ref {
    use shvrpc::metamethod::MetaMethod;

    pub static META_METHOD_DIR: &MetaMethod = &super::META_METHOD_DIR;
    pub static META_METHOD_LS: &MetaMethod = &super::META_METHOD_LS;
}

pub const META_METHOD_DIR: MetaMethod = MetaMethod::new_static(
    METH_DIR,
    Flag::None as u32,
    AccessLevel::Browse,
    "DirParam",
    "DirResult",
    &[],
    "",
);

pub const META_METHOD_LS: MetaMethod = MetaMethod::new_static(
    METH_LS,
    Flag::None as u32,
    AccessLevel::Browse,
    "LsParam",
    "LsResult",
    &[],
    "",
);

pub const META_METHOD_GET: MetaMethod = MetaMethod::new_static(
    METH_GET,
    Flag::IsGetter as u32,
    AccessLevel::Read,
    "",
    "",
    &[],
    "",
);

pub const META_METHOD_SET: MetaMethod = MetaMethod::new_static(
    METH_SET,
    Flag::IsSetter as u32,
    AccessLevel::Write,
    "",
    "",
    &[],
    "",
);

pub const META_METHOD_SIG_CHNG: MetaMethod = MetaMethod::new_static(
    SIG_CHNG,
    Flag::IsSignal as u32,
    AccessLevel::Read,
    "",
    "",
    &[],
    "",
);

pub const PROPERTY_METHODS: &[MetaMethod] = &[
    META_METHOD_GET,
    META_METHOD_SET,
    META_METHOD_SIG_CHNG,
];


#[cfg(test)]
mod tests {
    use super::*;
    // #[test]
    // fn process_request_static_node() {
    //     let node = crate::static_node!{
    //         TestStaticNode(request, _tx) {
    //             "echo" [IsGetter, Browse, "", ""] (param: i32) => {
    //                 Some(Ok(param.into()))
    //             }
    //         }
    //     };
    //
    //     let methods = node.methods();
    //     assert_eq!(methods.len(), 3, "Expected 3 methods");
    //     assert_eq!(methods[0].name, "dir");
    //     assert_eq!(methods[1].name, "ls");
    //     assert_eq!(methods[2].name, "echo");
    // }

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
                let RequestResult::Ok(resp) = res else {
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
                matches!((ls_children_to_result(Some(vec!["foo".into(), "x".into()]), LsParam::List), res), (Ok(a), Ok(b)) if a == b)
            })
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("blah", METH_LS, None))
            .is_none()
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("foo", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["bar".into(), "x".into()]), LsParam::List), res), (Ok(a), Ok(b)) if a == b)
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x", METH_LS, None)).is_none());
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x/y", METH_LS, None)).is_none());
    }

    #[test]
    fn local_dir_ls_without_root() {
        let mounts = BTreeMap::from([
            ("foo".to_string(), ()),
            ("foo/x/y".to_string(), ()),
            ("foo/bar".to_string(), ()),
            ("z".to_string(), ())
        ]);

        // dir
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("", METH_DIR, None))
            .is_some_and(|res| {
                let Ok(resp) = res else {
                    panic!("Not a response");
                };
                dir(DIR_LS_METHODS, DirParam::Brief) == resp
            })
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("blah", METH_DIR, None))
            .is_some_and(|resp| resp.is_err())
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("foo", METH_DIR, None)).is_none());
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x", METH_DIR, None))
            .is_some_and(|res| {
                let Ok(resp) = res else {
                    panic!("Not a response");
                };
                dir(DIR_LS_METHODS, DirParam::Brief) == resp
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x/y", METH_DIR, None)).is_none());

        // ls
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["foo".into(), "z".into()]), LsParam::List), res), (Ok(a), Ok(b)) if a == b)
            })
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("blah", METH_LS, None))
            .is_some_and(|resp| resp.is_err())
        );
        assert!(
            process_local_dir_ls(&mounts, &make_request_frame("foo", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["bar".into(), "x".into()]), LsParam::List), res), (Ok(a), Ok(b)) if a == b)
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x", METH_LS, None))
            .is_some_and(|res| {
                matches!((ls_children_to_result(Some(vec!["y".into()]), LsParam::List), res), (Ok(a), Ok(b)) if a == b)
            })
        );
        assert!(process_local_dir_ls(&mounts, &make_request_frame("foo/x/y", METH_LS, None)).is_none());
    }
}

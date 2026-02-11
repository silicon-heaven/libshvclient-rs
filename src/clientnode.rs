use crate::ClientCommandSender;
use crate::runtime::spawn_task;
use async_trait::async_trait;
use futures::future::BoxFuture;
use log::error;
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
pub use shvrpc::metamethod::{AccessLevel, Flags, MetaMethod};
pub use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
pub use shvproto::{RpcValue, Value};

fn builtin_dir<'a>(methods: impl IntoIterator<Item = &'a MetaMethod>, param: impl Into<DirParam>) -> RpcValue {
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
                .map_or_else(|| false.into(), |m| m.to_rpcvalue(metamethod::DirFormat::IMap))
        }
    }
}

pub type RequestResult = Result<RpcValue, RpcError>;

pub(crate) fn process_local_dir_ls<V>(
    mounts: &BTreeMap<String, V>,
    frame: &RpcFrame,
) -> Option<RequestResult> {
    if frame.access_level().is_none() {
            return Some(Err(RpcError::new(RpcErrorCode::InvalidRequest, "Undefined access level")));
    }
    let method = frame.method().unwrap_or_default();
    if !(method == METH_DIR || method == METH_LS) {
        return None;
    }
    let shv_path = frame.shv_path().unwrap_or_default();
    let mount = find_longest_path_prefix(mounts, shv_path);
    let is_mount_point = mount.is_some();
    let children_on_path = children_on_path(mounts, shv_path);
    let is_leaf = children_on_path.as_ref().map_or(is_mount_point, Vec::is_empty);
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
            let dir = builtin_dir(DIR_LS_METHODS, rpcmsg.param());
            return Some(RequestResult::Ok(dir));
        }
        return Some(RequestResult::Err(RpcError::new(
            RpcErrorCode::InvalidRequest,
            "Cannot convert RPC frame to RPC message".to_string(),
        )));
    }
    if method == METH_LS && !is_leaf {
        // ls on not-leaf node must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let ls = ls_children_to_result(children_on_path, rpcmsg.param());
            return Some(ls);
        }
        return Some(RequestResult::Err(RpcError::new(
            RpcErrorCode::InvalidRequest,
            "Cannot convert RPC frame to RPC message".to_string(),
        )));
    }
    None
}

fn ls_children_to_result(children: Option<Vec<String>>, param: impl Into<LsParam>) -> RequestResult {
    children.map_or_else(|| RequestResult::Err(RpcError::new(
                RpcErrorCode::MethodCallException,
                "Invalid shv path",
    )), |dirs| match param.into() {
        LsParam::List => {
            let res: rpcvalue::List = dirs.iter().map(RpcValue::from).collect();
            RequestResult::Ok(res.into())
        },
        LsParam::Exists(path) =>
            RequestResult::Ok(dirs.contains(&path).into()),
    })
}

#[async_trait]
pub trait StaticNode: Send + Sync + 'static {
    fn methods(&self) -> &'static [MetaMethod];
    async fn process_request(&self, request: RpcMessage, client_cmd_tx: ClientCommandSender) -> Option<RequestResult>;
}

#[derive(Clone)]
pub struct StaticNodeHandler(Arc<dyn StaticNode>);

#[derive(Clone)]
pub struct DynamicNodeHandler(pub(crate) Arc<dyn Fn(RpcMessage, ClientCommandSender) -> BoxFuture<'static, RequestHandlerResult> + Send + Sync>);

pub(crate) struct MethodHandler(pub(crate) Box<dyn FnOnce() -> BoxFuture<'static, Option<MethodHandlerResult<RpcValue>>> + Send>);
pub(crate) struct LsHandler(pub(crate) Box<dyn FnOnce() -> BoxFuture<'static, Option<LsHandlerResult>> + Send>);

enum MethodHandlerType {
    Dir,
    Ls(LsHandler),
    Method(MethodHandler),
}

pub struct ResolvedRequest {
    methods: MetaMethods,
    handler: MethodHandlerType,
}

pub enum Method {
    Dir(DirMethodResolver),
    Ls(LsMethodResolver),
    Other(MethodResolver),
}

impl Method {
    pub fn from_request(request: &RpcMessage) -> Self {
        match request.method().unwrap_or_default() {
            METH_DIR => Method::Dir(DirMethodResolver(Priv)),
            METH_LS => Method::Ls(LsMethodResolver(Priv)),
            method => Method::Other(MethodResolver(method.into())),
        }
    }
}

struct Priv;

pub struct DirMethodResolver(Priv);
pub struct LsMethodResolver(Priv);
pub struct MethodResolver(String);

impl DirMethodResolver {
    #[expect(clippy::unnecessary_wraps, reason = "Better ergonomics")]
    pub fn resolve(&self, methods: impl Into<MetaMethods>) -> RequestHandlerResult {
        Ok(ResolvedRequest {
            methods: methods.into(),
            handler: MethodHandlerType::Dir
        })
    }
}

impl LsMethodResolver {
    #[expect(clippy::unnecessary_wraps, reason = "Better ergonomics")]
    pub fn resolve<F, Fut>(&self, methods: impl Into<MetaMethods>, handler: F) -> RequestHandlerResult
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = LsHandlerResult> + Send + 'static,
    {
        Ok(ResolvedRequest {
            methods: methods.into(),
            handler: MethodHandlerType::Ls(LsHandler::new(async move || Some(handler().await))),
        })
    }

    #[expect(clippy::unnecessary_wraps, reason = "Better ergonomics")]
    pub fn resolve_opt<F, Fut>(&self, methods: impl Into<MetaMethods>, handler: F) -> RequestHandlerResult
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Option<LsHandlerResult>> + Send + 'static,
    {
        Ok(ResolvedRequest {
            methods: methods.into(),
            handler: MethodHandlerType::Ls(LsHandler::new(handler)),
        })
    }
}

impl MethodResolver {
    #[expect(clippy::unnecessary_wraps, reason = "Better ergonomics")]
    pub fn resolve<F, Fut, T>(&self, methods: impl Into<MetaMethods>, handler: F) -> RequestHandlerResult
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = MethodHandlerResult<T>> + Send + 'static,
        T: Into<RpcValue>,
    {
        Ok(ResolvedRequest {
            methods: methods.into(),
            handler: MethodHandlerType::Method(MethodHandler::new(async move || Some(handler().await))),
        })
    }

    #[expect(clippy::unnecessary_wraps, reason = "Better ergonomics")]
    pub fn resolve_opt<F, Fut, T>(&self, methods: impl Into<MetaMethods>, handler: F) -> RequestHandlerResult
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Option<MethodHandlerResult<T>>> + Send + 'static,
        T: Into<RpcValue>,
    {
        Ok(ResolvedRequest {
            methods: methods.into(),
            handler: MethodHandlerType::Method(MethodHandler::new(handler)),
        })
    }

    pub fn method(&self) -> &str {
        self.0.as_str()
    }
}

pub struct UnresolvedRequest;

impl UnresolvedRequest {
    pub fn to_rpcerror(&self, rq: &RpcMessage) -> RpcError {
        rpc_error_unknown_method_on_path(
            rq.shv_path().unwrap_or_default(),
            rq.method().unwrap_or_default()
        )
    }
}

pub fn err_unresolved_request() -> RequestHandlerResult {
    Err(UnresolvedRequest)
}

pub type RequestHandlerResult = Result<ResolvedRequest, UnresolvedRequest>;
pub type MethodHandlerResult<T> = Result<T, RpcError>;
pub type LsHandlerResult = MethodHandlerResult<Vec<String>>;

pub type MetaMethods = Cow<'static, [MetaMethod]>;

pub enum ClientNode {
    Static(StaticNodeHandler),
    Dynamic(DynamicNodeHandler),
}

#[async_trait]
trait NodeHandler {
    async fn process_request(
        &self,
        request: &RpcMessage,
        mount_path: &str,
        client_cmd_tx: &ClientCommandSender,
    ) -> Option<Result<RpcValue, RpcError>>;
}

#[async_trait]
impl NodeHandler for StaticNodeHandler {
    async fn process_request(
        &self,
        request: &RpcMessage,
        mount_path: &str,
        client_cmd_tx: &ClientCommandSender,
    ) -> Option<Result<RpcValue, RpcError>>
    {
        let Self(node) = self;

        let is_path_of_this_node = request.shv_path().unwrap_or_default().is_empty();
        let methods = if is_path_of_this_node {
            DIR_LS_METHODS.iter().chain(node.methods()).collect()
        } else {
            // Static nodes don't have children; children resolved in process_local_dir_ls
            Cow::from(&[])
        };

        let granted_method = match check_request_access(
            request,
            mount_path,
            methods.iter().copied())
        {
            Ok(method) => method,
            Err(err) => return Some(Err(err)),
        };

        match granted_method {
            crate::clientnode::METH_DIR =>
               Some(Ok(builtin_dir(methods.iter().copied(), request.param()))),
            crate::clientnode::METH_LS =>
                Some(Ok(builtin_ls(request.param()))),
            _ =>
                node.process_request(request.clone(), client_cmd_tx.clone()).await,
        }
    }
}

impl DynamicNodeHandler {
    pub fn new<F, Fut>(func: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = RequestHandlerResult> + Send + 'static
    {
        Self(Arc::new(move |rq, tx| Box::pin(func(rq, tx))))
    }
}

#[async_trait]
impl NodeHandler for DynamicNodeHandler {
    async fn process_request(
        &self,
        request: &RpcMessage,
        mount_path: &str,
        client_cmd_tx: &ClientCommandSender,
    ) -> Option<Result<RpcValue, RpcError>>
    {
        let Self(request_handler) = self;
        match request_handler(request.clone(), client_cmd_tx.clone()).await {
            Ok(ResolvedRequest { methods, handler }) => {
                fn get_method<'a>(methods: &'a MetaMethods, name: &str) -> Option<(usize, &'a MetaMethod)> {
                    methods
                        .iter()
                        .enumerate()
                        .find(|(_, mm)| mm.name == name)
                }
                fn extract_second_field<A, B>(tuple: (A, B)) -> B { tuple.1 }
                match handler {
                    MethodHandlerType::Dir => {
                        let all_methods = |methods: MetaMethods| if methods.is_empty() {
                            Cow::Borrowed(DIR_LS_METHODS)
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
                            .map_or(static_ref::META_METHOD_DIR, extract_second_field);
                        let response = check_request_access_for_method(request, mount_path, mm_dir)
                            .map(|()| builtin_dir(all_methods(methods).as_ref(), request.param()));
                        Some(response)
                    }
                    MethodHandlerType::Ls(LsHandler(ls_handler)) => {
                        let mm_ls = get_method(&methods, METH_LS)
                            .map_or(static_ref::META_METHOD_LS, extract_second_field);
                        if let Err(err) = check_request_access_for_method(request, mount_path, mm_ls) {
                            return Some(Err(err));
                        }
                        ls_handler()
                            .await
                            .map(|ls_result| {
                                let children = ls_result?;
                                ls_children_to_result(Some(children), request.param())
                            })
                    },
                    MethodHandlerType::Method(MethodHandler(method_handler)) => {
                        let method = request.method().unwrap_or_default();
                        let Some((_, mm)) = get_method(&methods, method) else {
                            let err = rpc_error_unknown_method_on_path(
                                full_shv_path(mount_path, request.shv_path().unwrap_or_default()),
                                method
                            );
                            return Some(Err(err));
                        };
                        if let Err(err) = check_request_access_for_method(request, mount_path, mm) {
                            return Some(Err(err));
                        }
                        method_handler().await
                    }
                }
            }
            Err(err) => Some(Err(err.to_rpcerror(request))),
        }
    }
}

impl MethodHandler {
    pub fn new<F, Fut, T>(func: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Option<MethodHandlerResult<T>>> + Send + 'static,
        T: Into<RpcValue>,
    {
        Self(Box::new(move || Box::pin(async move {
            func().await.map(|res| res.map(Into::into))
        })))
    }
}

impl LsHandler {
    pub fn new<F, Fut>(func: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Option<LsHandlerResult>> + Send + 'static
    {
        Self(Box::new(move || Box::pin(func())))
    }
}

impl ClientNode {
    pub fn new_static(node: impl StaticNode) -> Self {
        Self::Static(StaticNodeHandler(Arc::new(node)))
    }

    pub fn new_dynamic<F, Fut>(func: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = RequestHandlerResult> + Send + 'static
    {
        Self::Dynamic(DynamicNodeHandler::new(func))
    }

    pub(crate) async fn process_request(&self, request: RpcMessage, mount_path: String, client_cmd_tx: ClientCommandSender) {
        fn spawn_task_for_handler(
            handler: impl NodeHandler + Send + 'static,
            request: RpcMessage,
            mount_path: String,
            client_cmd_tx: ClientCommandSender
        ) {
            spawn_task(async move {
                if let Some(result) = handler.process_request(&request, &mount_path, &client_cmd_tx).await {
                    send_response(&request, &client_cmd_tx, result);
                }
            }).detach();
        }

        match &self {
            Self::Static(node_handler) =>
                spawn_task_for_handler(node_handler.clone(), request, mount_path, client_cmd_tx),
            Self::Dynamic(node_handler) =>
                spawn_task_for_handler(node_handler.clone(), request, mount_path, client_cmd_tx),
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
    let rq_level = rq.access_level().unwrap_or_default();
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

fn check_request_access<'a, 'r>(
    request: &'r RpcMessage,
    mount_path: impl AsRef<str>,
    methods: impl IntoIterator<Item = &'a MetaMethod>
) -> Result<&'r str, RpcError> {
    let method = request.method().unwrap_or_default();
    let Some(mm) = methods.into_iter().find(|mm| mm.name == method) else {
        let path = full_shv_path(mount_path.as_ref(), request.shv_path().unwrap_or_default());
        return Err(rpc_error_unknown_method_on_path(path, method))
    };
    check_request_access_for_method(request, mount_path, mm).map(|()| method)
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

pub fn builtin_ls(rq_param: Option<&RpcValue>) -> RpcValue {
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
    Flags::None,
    AccessLevel::Browse,
    "DirParam",
    "DirResult",
    &[],
    "",
);

pub const META_METHOD_LS: MetaMethod = MetaMethod::new_static(
    METH_LS,
    Flags::None,
    AccessLevel::Browse,
    "LsParam",
    "LsResult",
    &[],
    "",
);

pub const META_METHOD_GET: MetaMethod = MetaMethod::new_static(
    METH_GET,
    Flags::IsGetter,
    AccessLevel::Read,
    "",
    "",
    &[],
    "",
);

pub const META_METHOD_SET: MetaMethod = MetaMethod::new_static(
    METH_SET,
    Flags::IsSetter,
    AccessLevel::Write,
    "",
    "",
    &[],
    "",
);

pub const PROPERTY_METHODS: &[MetaMethod] = &[
    META_METHOD_GET,
    META_METHOD_SET,
];


#[cfg(test)]
mod tests {
    use shvrpc::rpcdiscovery::{DirResult, MethodInfo};

    use super::*;
    #[test]
    fn process_request_static_node() {
        let node = crate::static_node!{
            TestStaticNode(request, _tx) {
                "echo" [IsGetter, Read, "Int", "Int"] (param: i32) => {
                    Some(Ok(param.into()))
                }
                "unhandled" [IsGetter | IsSetter, Write, "", ""]  => {
                    None
                }
            }
        };

        let methods = node.methods();
        assert_eq!(methods.len(), 2, "Expected 2 methods");

        use shvrpc::metamethod::{Flags, AccessLevel};

        let method_echo = &methods[0];
        assert_eq!(method_echo.name, "echo");
        assert_eq!(method_echo.flags, Flags::IsGetter);
        assert_eq!(method_echo.access, AccessLevel::Read);
        assert_eq!(method_echo.param, "Int");
        assert_eq!(method_echo.result, "Int");

        let method_unhandled = &methods[1];
        assert_eq!(method_unhandled.name, "unhandled");
        assert_eq!(method_unhandled.flags, Flags::IsGetter | Flags::IsSetter);
        assert_eq!(method_unhandled.access, AccessLevel::Write);
        assert_eq!(method_unhandled.param, "");
        assert_eq!(method_unhandled.result, "");

        crate::runtime::block_on(async move {
            let node_handler = StaticNodeHandler(Arc::new(node));
            let (ccs, _ccr) = futures::channel::mpsc::unbounded();
            let ccs = ClientCommandSender::from_raw(ccs);

            let make_request = |shvpath, method, param: Option<RpcValue>, access_level| {
                let mut rq = RpcMessage::new_request(shvpath, method);
                if let Some(param) = param {
                    rq.set_param(param);
                }
                rq.set_access_level(access_level);
                rq
            };

            // echo: Invalid path
            let res = node_handler.process_request(
                &make_request(
                    "subpath",
                    "echo",
                    Some(32.into()),
                    AccessLevel::Read,
                ),
                "mount/path",
                &ccs,
            ).await;
            let res = res.unwrap_or_else(|| panic!("Result should be Some"));
            assert!(res.is_err_and(|err| err.code == RpcErrorCode::MethodNotFound.into()));

            // echo: Insufficient permission / method not found
            let res = node_handler.process_request(
                &make_request(
                    "",
                    "echo",
                    Some("blah".into()),
                    AccessLevel::Browse,
                ),
                "",
                &ccs,
            ).await;
            let res = res.unwrap_or_else(|| panic!("Result should be Some"));
            let Err(err) = res else {
                panic!("Result should be an error");
            };
            assert_eq!(err.code, RpcErrorCode::MethodNotFound.into());

            // echo: Invalid param
            let res = node_handler.process_request(
                &make_request(
                    "",
                    "echo",
                    Some("blah".into()),
                    AccessLevel::Read,
                ),
                "",
                &ccs,
            ).await;
            let res = res.unwrap_or_else(|| panic!("Result should be Some"));
            let Err(err) = res else {
                panic!("Result should be an error");
            };
            assert_eq!(err.code, RpcErrorCode::InvalidParam.into());

            // echo: Success
            let res = node_handler.process_request(
                &make_request(
                    "",
                    "echo",
                    Some(42.into()),
                    AccessLevel::Read,
                ),
                "",
                &ccs,
            ).await;
            let res = res.unwrap_or_else(|| panic!("Result should be Some"));
            assert!(res.is_ok_and(|val| val == 42.into()));

            // dir (exists)
            let res = node_handler.process_request(
                &make_request(
                    "",
                    "dir",
                    Some("echo".into()),
                    AccessLevel::Browse,
                ),
                "",
                &ccs,
            ).await;
            let res: bool = res
                .unwrap_or_else(|| panic!("Result should be Some"))
                .map_or_else(|_| panic!("Result should be Ok"), DirResult::try_from)
                .unwrap_or_else(|_| panic!("Result should be a DirResult"))
                .try_into()
                .unwrap_or_else(|_| panic!("Result should be a bool"));
            assert!(res);

            // dir (list)
            let res = node_handler.process_request(
                &make_request(
                    "",
                    "dir",
                    None,
                    AccessLevel::Browse,
                ),
                "",
                &ccs,
            ).await;
            let res: Vec<MethodInfo> = res
                .unwrap_or_else(|| panic!("Result should be Some"))
                .map_or_else(|_| panic!("Result should be Ok"), DirResult::try_from)
                .unwrap_or_else(|_| panic!("Result should be a DirResult"))
                .try_into()
                .unwrap_or_else(|_| panic!("Result should be a list of methods"));

            assert_eq!(res.len(), 4);
            assert_eq!(res[0].name, METH_DIR);
            assert_eq!(res[1].name, METH_LS);
            assert_eq!(res[2].name, "echo");
            assert_eq!(res[3].name, "unhandled");

            // unhandled method
            let res = node_handler.process_request(
                &make_request(
                    "",
                    "unhandled",
                    None,
                    AccessLevel::Config,
                ),
                "",
                &ccs,
            ).await;
            assert!(res.is_none());
        });
    }

    #[test]
    fn longest_path_prefix() {
        #[expect(clippy::zero_sized_map_values, reason = "Fine for tests")]
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
        let mut rq = RpcMessage::new_request(path, method);
        if let Some(param) = param {
            rq.set_param(param);
        }
        rq.set_access_level(AccessLevel::Read);
        rq.to_frame().unwrap()
    }

    #[test]
    fn local_dir_ls_with_root() {
        #[expect(clippy::zero_sized_map_values, reason = "Fine for tests")]
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
                builtin_dir(DIR_LS_METHODS, DirParam::Brief) == resp
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
        #[expect(clippy::zero_sized_map_values, reason = "Fine for tests")]
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
                builtin_dir(DIR_LS_METHODS, DirParam::Brief) == resp
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
                builtin_dir(DIR_LS_METHODS, DirParam::Brief) == resp
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

use crate::connection::{spawn_connection_task, ConnectionCommand, ConnectionEvent, ConnectionFailedKind};
use crate::clientnode::{process_local_dir_ls, ClientNode, RequestResult, Route, METH_DIR, METH_LS, METH_PING};
use async_broadcast::RecvError;
use futures::future::BoxFuture;
use futures::stream::{self, FuturesUnordered};
use futures::{select, Future, FutureExt, Stream, StreamExt};
use futures::channel::mpsc::{TrySendError, UnboundedSender};
use futures_time::task::sleep;
use futures_time::time::Duration;
use log::*;
use shvrpc::client::ClientConfig;
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpc::{Glob, ShvRI, SubscriptionParam};
use shvrpc::rpcdiscovery::{DirParam, DirResult, LsParam, LsResult, MethodInfo};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode, RqId};
use shvrpc::util::find_longest_path_prefix;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use shvproto::RpcValue;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

const METH_SUBSCRIBE: &str = "subscribe";
const METH_UNSUBSCRIBE: &str = "unsubscribe";

pub type Sender<K> = futures::channel::mpsc::UnboundedSender<K>;
pub type Receiver<K> = futures::channel::mpsc::UnboundedReceiver<K>;

type BroadcastReceiver<K> = async_broadcast::Receiver<K>;

mod private {
    static SUBSCRIPTION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    pub(super) fn next_subscription_id() -> u64 {
        SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub trait Sealed { }
}
use private::next_subscription_id;

pub struct Subscriber<T> {
    notifications_rx: Receiver<RpcFrame>,
    // For unsubscribe on drop
    client_cmd_tx: Sender<ClientCommand<T>>,
    ri: ShvRI,
    subscription_id: u64,
}

impl<T> Subscriber<T> {
    pub fn path_signal(&self) -> (&str, &str) {
        (self.ri.path(), self.ri.signal().unwrap_or("*"))
    }
    pub fn ri(&self) -> &ShvRI {
        &self.ri
    }
}

impl<T> futures::Stream for Subscriber<T> {
    type Item = RpcFrame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().notifications_rx.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.notifications_rx.size_hint()
    }
}

impl<T> Drop for Subscriber<T> {
    fn drop(&mut self) {
        if let Err(err) = self.client_cmd_tx.unbounded_send(
            ClientCommand::Unsubscribe { subscription_id: self.subscription_id, }) {
            warn!("Cannot unsubscribe `{}`: {err}", &self.ri);
        };
    }
}

#[derive(Clone,Debug)]
pub enum CallRpcMethodErrorKind {
    // The receive channel got closed before the response received
    ConnectionClosed,
    // Received frame could not be parsed to an RpcMessage
    InvalidMessage(String),
    // Got an error instead of a result
    RpcError(RpcError),
    // Could not convert result to target data type
    ResultTypeMismatch(String),
}

impl std::fmt::Display for CallRpcMethodErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let err_msg = match self {
            CallRpcMethodErrorKind::ConnectionClosed => "Connection closed",
            CallRpcMethodErrorKind::InvalidMessage(msg) => msg,
            CallRpcMethodErrorKind::RpcError(err) => &err.to_string(),
            CallRpcMethodErrorKind::ResultTypeMismatch(msg) => msg,
        };
        write!(f, "{err_msg}")
    }
}

#[derive(Clone,Debug)]
pub struct CallRpcMethodError {
    path: String,
    method: String,
    error: CallRpcMethodErrorKind,
}

impl CallRpcMethodError {
    pub fn new(path: &str, method: &str, error: CallRpcMethodErrorKind) -> Self {
        Self {
            path: path.to_owned(),
            method: method.to_owned(),
            error
        }
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn method(&self) -> &str {
        &self.method
    }
    pub fn error(&self) -> &CallRpcMethodErrorKind {
        &self.error
    }

}

impl std::fmt::Display for CallRpcMethodError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RPC call on path `{path}`, method `{method}`, error: {error}",
            path = self.path,
            method = self.method,
            error = self.error,
        )
    }
}

pub enum RpcCallResponse<T> {
    Success(T),
    Delay(f64),
}

pub struct ClientCommandSender<T> {
    pub(crate) sender: Sender<ClientCommand<T>>,
}

impl<T> Clone for ClientCommandSender<T> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

impl<T> ClientCommandSender<T> {
    #[cfg(feature = "mocking")]
    pub fn from_raw(sender: Sender<ClientCommand<T>>) -> Self {
        Self { sender }
    }

    pub fn terminate_client(&self) {
        self.sender
            .unbounded_send(ClientCommand::TerminateClient)
            .unwrap_or_else(|e| error!("Failed to send TerminateClient command: {e}"));
    }

    pub fn mount_node(&self, path: impl AsRef<str>, node: ClientNode<'static, T>) {
        self.sender
            .unbounded_send(ClientCommand::MountNode { path: path.as_ref().into(), node })
            .unwrap_or_else(|e| error!("Failed to send MountNode command: {e}"));
    }

    pub fn unmount_node(&self, path: impl AsRef<str>) {
        self.sender
            .unbounded_send(ClientCommand::UnmountNode { path: path.as_ref().into() })
            .unwrap_or_else(|e| error!("Failed to send UnmountNode command: {e}"));
    }

    pub fn do_rpc_call(
        &self,
        shvpath: impl AsRef<str>,
        method: impl AsRef<str>,
        param: Option<RpcValue>,
        timeout: Option<Duration>,
    ) -> Result<Receiver<RpcFrame>, TrySendError<ClientCommand<T>>>
    {
        let (response_sender, response_receiver) = futures::channel::mpsc::unbounded();
        self.sender.unbounded_send(ClientCommand::RpcCall {
            request: RpcMessage::new_request(shvpath.as_ref(), method.as_ref(), param),
            response_sender,
            timeout,
        })
        .map(|_| response_receiver)
    }

    pub async fn call_dir(&self, path: &str, param: DirParam, timeout: Option<Duration>) -> Result<DirResult, CallRpcMethodError> {
        self.call_dir_into(path, param, timeout).await
    }

    pub async fn call_dir_brief(&self, path: &str, timeout: Option<Duration>) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
        self.call_dir_into(path, DirParam::Brief, timeout).await
    }

    pub async fn call_dir_full(&self, path: &str, timeout: Option<Duration>) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
        self.call_dir_into(path, DirParam::Full, timeout).await
    }

    pub async fn call_dir_exists(&self, path: &str, method: &str, timeout: Option<Duration>) -> Result<bool, CallRpcMethodError> {
        self.call_dir_into(path, DirParam::Exists(method.into()), timeout).await
    }

    async fn call_dir_into<R, E>(&self, path: &str, param: DirParam, timeout: Option<Duration>) -> Result<R, CallRpcMethodError>
    where
        R: TryFrom<DirResult, Error = E>,
        E: std::fmt::Display,
    {
        self.call_rpc_method(path, METH_DIR, Some(RpcValue::from(param)), timeout, None::<fn(_)>)
            .await
            .and_then(|dir_res|
                R::try_from(dir_res).map_err(|e|
                    CallRpcMethodError::new(
                        path,
                        METH_DIR,
                        CallRpcMethodErrorKind::ResultTypeMismatch(e.to_string())
                    )
                )
            )
    }

    pub async fn call_ls(&self, path: &str, param: LsParam, timeout: Option<Duration>) -> Result<LsResult, CallRpcMethodError> {
        self.call_ls_into(path, param, timeout).await
    }

    pub async fn call_ls_exists(&self, path: &str, dirname: &str, timeout: Option<Duration>) -> Result<bool, CallRpcMethodError> {
        self.call_ls_into(path, LsParam::Exists(dirname.into()), timeout).await
    }

    pub async fn call_ls_list(&self, path: &str, timeout: Option<Duration>) -> Result<Vec<String>, CallRpcMethodError> {
        self.call_ls_into(path, LsParam::List, timeout).await
    }

    async fn call_ls_into<R, E>(&self, path: &str, param: LsParam, timeout: Option<Duration>) -> Result<R, CallRpcMethodError>
    where
        R: TryFrom<LsResult, Error = E>,
        E: std::fmt::Display,
    {
        self.call_rpc_method(path, METH_LS, Some(RpcValue::from(param)), timeout, None::<fn(_)>)
            .await
            .and_then(|ls_res|
                R::try_from(ls_res).map_err(|e|
                    CallRpcMethodError::new(
                        path,
                        METH_LS,
                        CallRpcMethodErrorKind::ResultTypeMismatch(e.to_string())
                    )
                )
            )
    }

    pub fn call_rpc_method_stream<R, E>(
        &self,
        path: impl AsRef<str>,
        method: impl AsRef<str>,
        param: Option<RpcValue>,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = Result<RpcCallResponse<R>, CallRpcMethodError>> + Send>>
    where
        R: for<'a> TryFrom<&'a RpcValue, Error = E>,
        E: std::fmt::Display,
    {
        let path = path.as_ref();
        let method = method.as_ref();
        let make_error = {
            let path = path.to_string();
            let method = method.to_string();
            move |error_kind: CallRpcMethodErrorKind| {
                CallRpcMethodError::new(&path, &method, error_kind)
            }
        };

        use CallRpcMethodErrorKind::*;
        let call = self.do_rpc_call(path, method, param, timeout)
            .map_err(|err| {
                warn!("Cannot send RPC request to the client core. \
                    Path: `{path}`, method: `{method}`, error: {err}");
                make_error(ConnectionClosed)
            });
        match call {
            Err(err) => Box::pin(stream::once(async { Err(err) })),
            Ok(receiver) => {
                let mapped = receiver
                    .map(move |frame|
                        frame
                        .to_rpcmesage()
                        .map_err(|e| make_error(InvalidMessage(e.to_string())))
                        .and_then(|rpcmsg|
                            rpcmsg
                            .response()
                            .map_err(|e| make_error(RpcError(e)))
                            .and_then(|resp| match resp {
                                shvrpc::rpcmessage::Response::Success(rpc_value) =>
                                    R::try_from(rpc_value)
                                    .map(RpcCallResponse::Success)
                                    .map_err(|e| make_error(ResultTypeMismatch(e.to_string()))),
                                shvrpc::rpcmessage::Response::Delay(progress) =>
                                    Ok(RpcCallResponse::Delay(progress)),
                            })
                        )
                    );
                Box::pin(mapped)
            }
        }
    }

    pub async fn call_rpc_method<R, E, F>(
        &self,
        path: impl AsRef<str>,
        method: impl AsRef<str>,
        param: Option<RpcValue>,
        timeout: Option<Duration>,
        progress_notifier: Option<F>,
    ) -> Result<R, CallRpcMethodError>
    where
        R: for<'a> TryFrom<&'a RpcValue, Error = E>,
        E: std::fmt::Display,
        F: Fn(f64) + Send,
    {
        let path = path.as_ref();
        let method = method.as_ref();

        let mut receiver = self.call_rpc_method_stream(path, method, param, timeout);
        while let Some(result) = receiver.next().await {
            match result? {
                RpcCallResponse::Delay(progress) => {
                    if let Some(progress_notify) = &progress_notifier {
                        progress_notify(progress);
                    }
                    continue
                }
                RpcCallResponse::Success(result) => return Ok(result),
            }
        }
        Err(CallRpcMethodError::new(path, method, CallRpcMethodErrorKind::ConnectionClosed))
    }

    pub fn send_message(&self, message: RpcMessage) -> Result<(), TrySendError<ClientCommand<T>>> {
        self.sender.unbounded_send(ClientCommand::SendMessage { message })
    }

    pub async fn subscribe(&self, ri: ShvRI) -> Result<Subscriber<T>, CallRpcMethodError> {
        let subscription_id = next_subscription_id();
        let (notifications_tx, notifications_rx) = futures::channel::mpsc::unbounded();

        let make_error = |error_kind: CallRpcMethodErrorKind| {
            CallRpcMethodError::new("", METH_SUBSCRIBE, error_kind)
        };
        use CallRpcMethodErrorKind::*;

        self.sender
            .unbounded_send(
                ClientCommand::Subscribe {
                    ri: ri.clone(),
                    subscription_id,
                    notifications_tx,
                }
            )
            .map_err(|_| make_error(ConnectionClosed))?;

        // The Subscriber is created at this point, because if an error occurs during the subscriber
        // response processing below, the drop() on Subscriber will send ClientCommand::Unsubscribe.
        let mut subscriber = Subscriber {
            notifications_rx,
            client_cmd_tx: self.sender.clone(),
            ri,
            subscription_id
        };

        // Wait for the subscribe response
        subscriber.notifications_rx
            .next()
            .await
            .ok_or_else(|| make_error(ConnectionClosed))?
            .to_rpcmesage()
            .map_err(|e| make_error(InvalidMessage(e.to_string())))?
            .response()
            .map_err(|e| make_error(RpcError(e)))?
            .success()
            .ok_or_else(|| make_error(InvalidMessage("Expected a single successful result or an error response to a subscribe call".into())))?;

        Ok(subscriber)
    }
}


pub enum ClientCommand<T> {
    SendMessage {
        message: RpcMessage,
    },
    RpcCall {
        request: RpcMessage,
        response_sender: Sender<RpcFrame>,
        timeout: Option<Duration>,
    },
    Subscribe {
        ri: ShvRI,
        subscription_id: u64,
        notifications_tx: Sender<RpcFrame>,
    },
    Unsubscribe {
        subscription_id: u64,
    },
    MountNode  {
        path: String,
        node: ClientNode<'static, T>,
    },
    UnmountNode {
        path: String,
    },
    TerminateClient,
}

#[derive(Clone,Debug)]
pub struct RpcCall<'a> {
    path: &'a str,
    method: &'a str,
    param: Option<RpcValue>,
    timeout: Option<Duration>,
}

impl<'a> RpcCall<'a> {
    pub fn new(path: &'a str, method: &'a str) -> Self {
        Self { path, method, param: None, timeout: None }
    }

    pub fn param(mut self, param: impl Into<RpcValue>) -> Self {
        self.param = Some(param.into());
        self
    }

    pub fn timeout(mut self, timeout: impl Into<Duration>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }

    pub async fn exec<T, R, E>(self, client_cmd_sender: &ClientCommandSender<T>) -> Result<R, CallRpcMethodError>
    where
        R: for<'r> TryFrom<&'r RpcValue, Error = E>,
        E: std::fmt::Display,
    {
        client_cmd_sender.call_rpc_method(self.path, self.method, self.param, self.timeout, None::<fn(_)>).await
    }

    pub async fn exec_with_progress<T, R, E>(self, client_cmd_sender: &ClientCommandSender<T>, progress_notifier: impl Fn(f64) + Send + 'static) -> Result<R, CallRpcMethodError>
    where
        R: for<'r> TryFrom<&'r RpcValue, Error = E>,
        E: std::fmt::Display,
    {
        client_cmd_sender.call_rpc_method(self.path, self.method, self.param, self.timeout, Some(progress_notifier)).await
    }

    pub fn stream<T, R, E>(self, client_cmd_sender: &ClientCommandSender<T>) -> Pin<Box<dyn Stream<Item = Result<RpcCallResponse<R>, CallRpcMethodError>> + Send>>
    where
        R: for<'r> TryFrom<&'r RpcValue, Error = E>,
        E: std::fmt::Display,
    {
        client_cmd_sender.call_rpc_method_stream(self.path, self.method, self.param, self.timeout)
    }

}

#[derive(Debug)]
pub struct RpcCallLsList<'a> {
    path: &'a str,
    timeout: Option<Duration>,
}

impl<'a> RpcCallLsList<'a> {
    pub fn new(path: &'a str) -> Self {
        Self { path, timeout: None }
    }

    pub fn timeout(mut self, timeout: impl Into<Duration>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }

    pub async fn exec<T>(self, client_cmd_sender: &ClientCommandSender<T>) -> Result<Vec<String>, CallRpcMethodError> {
        client_cmd_sender.call_ls_list(self.path, self.timeout).await
    }
}

#[derive(Debug)]
pub struct RpcCallLsExists<'a> {
    path: &'a str,
    dirname: &'a str,
    timeout: Option<Duration>,
}

impl<'a> RpcCallLsExists<'a> {
    pub fn new(path: &'a str, dirname: &'a str) -> Self {
        Self { path, dirname, timeout: None }
    }

    pub fn timeout(mut self, timeout: impl Into<Duration>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }

    pub async fn exec<T>(self, client_cmd_sender: &ClientCommandSender<T>) -> Result<bool, CallRpcMethodError> {
        client_cmd_sender.call_ls_exists(self.path, self.dirname, self.timeout).await
    }
}

#[derive(Debug)]
pub struct RpcCallDirList<'a> {
    path: &'a str,
    timeout: Option<Duration>,
}

impl<'a> RpcCallDirList<'a> {
    pub fn new(path: &'a str) -> Self {
        Self { path, timeout: None }
    }

    pub fn timeout(mut self, timeout: impl Into<Duration>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }

    pub async fn exec_brief<T>(self, client_cmd_sender: &ClientCommandSender<T>) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
        client_cmd_sender.call_dir_brief(self.path, self.timeout).await
    }

    pub async fn exec_full<T>(self, client_cmd_sender: &ClientCommandSender<T>) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
        client_cmd_sender.call_dir_full(self.path, self.timeout).await
    }
}

#[derive(Debug)]
pub struct RpcCallDirExists<'a> {
    path: &'a str,
    method: &'a str,
    timeout: Option<Duration>,
}

impl<'a> RpcCallDirExists<'a> {
    pub fn new(path: &'a str, method: &'a str) -> Self {
        Self { path, method, timeout: None }
    }

    pub fn timeout(mut self, timeout: impl Into<Duration>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }

    pub async fn exec<T>(self, client_cmd_sender: &ClientCommandSender<T>) -> Result<bool, CallRpcMethodError> {
        client_cmd_sender.call_dir_exists(self.path, self.method, self.timeout).await
    }
}

pub const BROKER_APP_NODE: &str = ".broker/app";
pub const BROKER_CLIENT_NODE: &str = ".broker/client";
pub const BROKER_CURRENT_CLIENT_NODE: &str = ".broker/currentClient";

pub type MetaMethods = Cow<'static, [&'static MetaMethod]>;

// The wrapping struct itself is descriptive
#[allow(clippy::type_complexity)]
pub struct MethodsGetter<T>(pub(crate) Box<dyn Fn(String, Option<AppState<T>>) -> BoxFuture<'static, Option<MetaMethods>> + Sync + Send>);

impl<T> MethodsGetter<T> {
    pub fn new<F, Fut>(func: F) -> Self
    where
        F: Fn(String, Option<AppState<T>>) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = Option<MetaMethods>> + Send + 'static,
    {
        Self(Box::new(move |path, data| Box::pin(func(path, data))))
    }
}

// The wrapping struct itself is descriptive
#[allow(clippy::type_complexity)]
pub struct RequestHandler<T>(pub(crate) Box<dyn Fn(RpcMessage, ClientCommandSender<T>, Option<AppState<T>>) -> BoxFuture<'static, ()> + Sync + Send>);

impl<T> RequestHandler<T> {
    pub fn stateful<F, Fut>(func: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender<T>, Option<AppState<T>>) -> Fut + Sync + Send + 'static,
        Fut: Future<Output=()> + Send + 'static
    {
        Self(Box::new(move |req, tx, data| Box::pin(func(req, tx, data))))
    }

    pub fn stateless<F, Fut>(func: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender<T>) -> Fut + Sync + Send + 'static,
        Fut: Future<Output=()> + Send + 'static
    {
        Self(Box::new(move |req, tx, _data| Box::pin(func(req, tx))))
    }
}

#[derive(Debug, Clone)]
pub enum ShvApiVersion {
    V2,
    V3,
}

#[derive(Clone)]
pub enum ClientEvent {
    ConnectionFailed(ConnectionFailedKind),
    Connected(ShvApiVersion),
    Disconnected,
}

#[derive(Clone)]
pub struct ClientEventsReceiver(BroadcastReceiver<ClientEvent>);

impl futures::Stream for ClientEventsReceiver {
    type Item = ClientEvent;

   fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
       self.get_mut().0.poll_next_unpin(cx)
   }
   fn size_hint(&self) -> (usize, Option<usize>) {
       self.0.size_hint()
   }
}

impl ClientEventsReceiver {
    pub async fn wait_for_event(&mut self) -> Result<ClientEvent, RecvError> {
        loop {
            match self.0.recv().await {
                Ok(evt) => break Ok(evt),
                Err(async_broadcast::RecvError::Overflowed(cnt)) => {
                    warn!("Client event receiver missed {cnt} event(s)!");
                }
                err => break err,
            }
        }
    }

    pub fn recv_event(&mut self) -> Pin<Box<async_broadcast::Recv<'_, ClientEvent>>> {
        self.0.recv()
    }

    #[cfg(feature = "mocking")]
    pub fn from_raw(recv: BroadcastReceiver<ClientEvent>) -> Self {
        Self(recv)
    }
}

pub struct AppState<T: ?Sized>(Arc<T>);

impl<T> AppState<T> {
    pub fn new(data: T) -> Self {
        Self(Arc::new(data))
    }
}

impl<T: ?Sized> std::ops::Deref for AppState<T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> Clone for AppState<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: ?Sized> From<Arc<T>> for AppState<T> {
    fn from(value: Arc<T>) -> Self {
        Self(value)
    }
}


#[derive(Debug)]
struct SubscriptionEntry {
    /// Notifications can be forwarded to the subscriber (the subscription response has been received)
    confirmed: bool,
    subscr_id: u64,
    glob: Glob,
    sender: Sender<RpcFrame>,
}

impl SubscriptionEntry {
    fn matches(&self, ri: &ShvRI, api_version: &ShvApiVersion) -> bool {
        match api_version {
            ShvApiVersion::V2 => {
                let subscr_signal = self.glob.as_ri().signal();
                let signal_matches = subscr_signal.is_some_and(|s| s == "*") || subscr_signal == ri.signal();
                let path_matches = ri
                    .path()
                    .strip_prefix(self.glob.as_ri().path())
                    .is_some_and(|rem| rem.is_empty() || rem.starts_with('/'));
                path_matches && signal_matches
            }
            ShvApiVersion::V3 => self.glob.match_shv_ri(ri),
        }
    }
}

#[derive(Debug, Default)]
struct Subscriptions(Vec<SubscriptionEntry>);

enum SubscriptionRequest {
    Subscribe,
    Unsubscribe,
}

fn create_subscription_request(ri: &ShvRI, req_type: SubscriptionRequest, api_version: &ShvApiVersion) -> RpcMessage {
    let method = match req_type {
        SubscriptionRequest::Subscribe => METH_SUBSCRIBE,
        SubscriptionRequest::Unsubscribe => METH_UNSUBSCRIBE,
    };
    match api_version {
        ShvApiVersion::V2 =>
            RpcMessage::new_request(
                BROKER_APP_NODE,
                method,
                Some({
                    let mut map = shvproto::Map::new();
                    map.insert("signal".to_string(), ri.signal().map(|s| if s == "*" { "" } else { s }).into());
                    map.insert("paths".to_string(),ri.path().into());
                    map.into()
                })
            ),
        ShvApiVersion::V3 =>
            RpcMessage::new_request(
                BROKER_CURRENT_CLIENT_NODE,
                method,
                Some(SubscriptionParam { ri: ri.clone(), ttl: None }.to_rpcvalue()),
            ),
    }
}

impl Subscriptions {
    fn new() -> Self {
        Default::default()
    }

    fn clear(&mut self) {
        self.0.clear();
    }

    fn add(
        &mut self,
        api_version: &ShvApiVersion,
        ri: ShvRI,
        subscr_id: u64,
        sender: Sender<RpcFrame>,
    ) -> Result<Option<RpcMessage>, String> {
        // Patch RI for SHV2
        let ri = match api_version {
            ShvApiVersion::V2 => ShvRI::from_path_method_signal(ri.path(), "*", ri.signal())?,
            ShvApiVersion::V3 => ri,
        };
        if !ri.has_signal() {
            return Err("Empty signal field in subscription RI".into());
        }
        let glob = ri.to_glob()?;
        let subscriptions = &mut self.0;
        if subscriptions.iter().any(|subscr| subscr.subscr_id == subscr_id) {
            panic!("Tried to add a subscription with already existing ID: {subscr_id}. RI: {ri}. Dump: {self:?}");
        }
        let subscribed_new_ri = !subscriptions.iter().any(|subscr | subscr.glob.as_ri() == &ri);
        let opt_subscription_request = subscribed_new_ri.then(||
            create_subscription_request(&ri, SubscriptionRequest::Subscribe, api_version)
        );
        subscriptions.push(SubscriptionEntry { confirmed: false, subscr_id, glob, sender, });
        Ok(opt_subscription_request)
    }

    // Returns Some if the last subscription with such RI has been removed
    fn remove(&mut self, api_version: &ShvApiVersion, subscr_id: u64) -> Option<RpcMessage> {
        let Some(pos) = self.0.iter().position(|subscr| subscr.subscr_id == subscr_id) else {
            // NOTE: On broker Disconnect all subscriptions are cleared.
            // If there is any NotificationsReceiver that gets dropped,
            // it will try to remove the subscription again.
            debug!("Remove non-existing subscription ID: {subscr_id}. Dump: {self:?}");
            return None;
        };
        let removed_ri = self.0.swap_remove(pos).glob.as_ri().clone();
        let is_removed_ri_last = !self.0.iter().any(|subscr| subscr.glob.as_ri() == &removed_ri);
        is_removed_ri_last.then(|| create_subscription_request(&removed_ri, SubscriptionRequest::Unsubscribe, api_version))
    }
}

pub trait ClientVariant: private::Sealed { }

pub enum Plain { }
impl ClientVariant for Plain { }
impl private::Sealed for Plain { }

pub enum Full { }
impl ClientVariant for Full { }
impl private::Sealed for Full { }

pub struct Client<V: ClientVariant, T> {
    mounts: BTreeMap<String, ClientNode<'static, T>>,
    app_state: Option<AppState<T>>,
    rpc_call_timeout: Duration,
    variant_marker: PhantomData<V>,
}

const RPC_CALL_DEFAULT_TIMEOUT_SECS: u64 = 10;

impl Client<Plain, ()> {
    pub fn new_plain() -> Self {
        Self {
            mounts: Default::default(),
            app_state: Default::default(),
            rpc_call_timeout: Duration::from_secs(RPC_CALL_DEFAULT_TIMEOUT_SECS),
            variant_marker: PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static> Default for Client<Full, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static> Client<Full, T> {
    pub fn new() -> Self {
        Self {
            mounts: Default::default(),
            app_state: Default::default(),
            rpc_call_timeout: Duration::from_secs(RPC_CALL_DEFAULT_TIMEOUT_SECS),
            variant_marker: PhantomData,
        }
    }

    pub fn app(self, app_node: crate::appnodes::DotAppNode) -> Self {
        self.mount(".app", ClientNode::constant(app_node))
    }

    pub fn device(self, device_node: crate::appnodes::DotDeviceNode) -> Self {
        self.mount(".device", ClientNode::constant(device_node))
    }

    pub fn mount<P: Into<String>>(mut self, path: P, node: ClientNode<'static, T>) -> Self {
        self.mounts.insert(path.into(), node);
        self
    }

    pub fn mount_fixed<P, M, R>(mut self, path: P, defined_methods: M, routes: R) -> Self
    where
        P: Into<String>,
        M: IntoIterator<Item = &'static MetaMethod>,
        R: IntoIterator<Item = Route<T>>,
    {
        self.mounts.insert(path.into(), ClientNode::fixed(defined_methods, routes));
        self
    }

    pub fn mount_dynamic<P>(mut self, path: P, methods_getter: MethodsGetter<T>, request_handler: RequestHandler<T>) -> Self
    where
        P: Into<String>,
    {
        self.mounts.insert(path.into(), ClientNode::dynamic(methods_getter, request_handler));
        self
    }

    pub fn with_app_state(mut self, app_state: AppState<T>) -> Self {
        self.app_state = Some(app_state);
        self
    }

    pub async fn run(mut self, config: &ClientConfig) -> shvrpc::Result<()> {
        self.run_with_init_opt(
            config,
            Option::<fn(_,_)>::None,
        )
        .await
    }
}

impl<V: ClientVariant, T: Send + Sync + 'static> Client<V, T> {
    pub fn rpc_call_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_call_timeout = timeout;
        self
    }

    async fn run_with_init_opt<H>(
        &mut self,
        config: &ClientConfig,
        init_handler: Option<H>,
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender<T>, ClientEventsReceiver),
    {
        let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
        spawn_connection_task(config, conn_evt_tx);
        self.client_loop(conn_evt_rx, init_handler).await
    }

    pub async fn run_with_init<H>(mut self, config: &ClientConfig, handler: H) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender<T>, ClientEventsReceiver),
    {
        self.run_with_init_opt(config, Some(handler)).await
    }

    async fn client_loop<H>(
        &mut self,
        mut conn_events_rx: Receiver<ConnectionEvent>,
        init_handler: Option<H>,
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender<T>, ClientEventsReceiver),
    {
        let mut rpc_call_timers = FuturesUnordered::new();
        let mut pending_rpc_calls: HashMap<i64, (Sender<RpcFrame>, UnboundedSender<()>)> = HashMap::new();
        let mut subscriptions = Subscriptions::new();
        let mut subscription_requests = HashMap::<RqId, u64>::new();

        let (client_cmd_tx, mut client_cmd_rx) = futures::channel::mpsc::unbounded();
        let client_cmd_tx = ClientCommandSender { sender: client_cmd_tx };
        let (mut client_events_tx, client_events_rx) = async_broadcast::broadcast(10);
        client_events_tx.set_overflow(true);
        let client_events_receiver = ClientEventsReceiver(client_events_rx.clone());
        let mut conn_cmd_sender: Option<Sender<ConnectionCommand>> = None;

        if let Some(init_handler) = init_handler {
            init_handler(client_cmd_tx.clone(), client_events_receiver);
        }

        async fn check_shv_api_version<T>(client_cmd_tx: ClientCommandSender<T>) -> Result<ShvApiVersion, CallRpcMethodError>
        {
            let api_version = RpcCallLsList::new(".broker")
                .exec(&client_cmd_tx)
                .await?
                .iter()
                .find_map(|node| match node.as_str() {
                    "app" => Some(ShvApiVersion::V2),
                    "client" => Some(ShvApiVersion::V3),
                    _ => None,
                })
            .unwrap_or_else(|| {
                warn!("Cannot detect SHV API version. Using version 2 as a fallback.");
                ShvApiVersion::V2
            });
            Ok(api_version)
        }

        let mut next_client_cmd = client_cmd_rx.next().fuse();
        let mut next_conn_event = conn_events_rx.next().fuse();
        let mut shv_api_version = None;
        let (api_version_tx, mut api_version_rx) = futures::channel::mpsc::unbounded();

        loop {
            select! {
                timer_result = rpc_call_timers.select_next_some() => {
                    if let Some((req_id, duration)) = timer_result
                        && let Some((response_sender, _)) = pending_rpc_calls.remove(&req_id)
                            && let Ok(mut response) = RpcMessage::new_request("", "", None).prepare_response()
                                && let Ok(err_frame) = response
                                    .set_error(RpcError::new(RpcErrorCode::MethodCallTimeout, format!("No response received within {duration} secs"))).to_frame() {
                                        response_sender.unbounded_send(err_frame).unwrap_or_default();
                                }
                }
                client_cmd_result = next_client_cmd => match client_cmd_result {
                    Some(client_cmd) => {
                        use ClientCommand::*;
                        match client_cmd {
                            SendMessage { message } => {
                                if let Some(ref conn_cmd_sender) = conn_cmd_sender {
                                    conn_cmd_sender
                                        .unbounded_send(ConnectionCommand::SendMessage(message))
                                        .unwrap_or_else(|e| error!("Cannot send SendMessage command through ConnectionCommand channel: {e}"));
                                } else {
                                    warn!("Client tries to send an RPC message while a connection to the broker is not established. Message: {message:?}");
                                }
                            },
                            RpcCall { request, response_sender, timeout } => {
                                match request.request_id() {
                                    None => {
                                        if let Ok(mut response) = request.prepare_response()
                                            && let Ok(err_frame) = response
                                                .set_error(RpcError::new(RpcErrorCode::InvalidRequest, "Request ID must be set")).to_frame() {
                                                    response_sender.unbounded_send(err_frame).unwrap_or_default();
                                            }
                                    }
                                    Some(req_id) => {
                                        // A message passed to the channel updates the timer, the tx drop cancels the timer.
                                        let (timer_update_tx, mut timer_update_rx) = futures::channel::mpsc::unbounded();
                                        let timeout = timeout.unwrap_or(self.rpc_call_timeout);
                                        rpc_call_timers.push(async move {
                                            loop {
                                                select! {
                                                    _ = sleep(timeout).fuse() => {
                                                        return Some((req_id, timeout.as_secs()))
                                                    }
                                                    msg = timer_update_rx.next() => if msg.is_some() {
                                                        continue
                                                    } else {
                                                        break
                                                    },
                                                }
                                            }
                                            None
                                        });

                                        if let Some((old_response_sender, _)) = pending_rpc_calls.insert(req_id, (response_sender, timer_update_tx)) {
                                            error!("Request ID `{req_id}` for async RpcCall has already been registered");
                                            if let Ok(mut response) = request.prepare_response()
                                                && let Ok(err_frame) = response
                                                    .set_error(RpcError::new(RpcErrorCode::InternalError, "A request with the same request ID cancelled this call")).to_frame() {
                                                        old_response_sender.unbounded_send(err_frame).unwrap_or_default();
                                                }
                                        }
                                        client_cmd_tx
                                            .send_message(request)
                                            .unwrap_or_else(|e| error!("Cannot send RpcCall command through ClientCommand channel: {e}"));
                                    }
                                }
                            },
                            Subscribe { ri, subscription_id, notifications_tx } => {
                                if let Some(api_version) = &shv_api_version {
                                    match subscriptions.add(api_version, ri, subscription_id, notifications_tx.clone()) {
                                        Ok(Some(subscribe_req)) => {
                                            let req_id = subscribe_req.request_id().expect("Request ID of a subscription request is Some");
                                            subscription_requests.insert(req_id, subscription_id);
                                            client_cmd_tx
                                                .send_message(subscribe_req)
                                                .unwrap_or_else(|e|
                                                    error!("Cannot send Subscribe command through ClientCommand channel: {e}")
                                                );
                                        }
                                        Ok(None) => {
                                            // There is already a subscription with the same RI.
                                            // Do not subscribe it twice, but send Ok response to
                                            // the caller.
                                            if let Ok(mut response) = RpcMessage::new_request("", METH_SUBSCRIBE, None).prepare_response()
                                                && let Ok(frame) = response.set_result(()).to_frame() {
                                                    notifications_tx.unbounded_send(frame).unwrap_or_default();
                                                    if let Some(subscr) = subscriptions.0
                                                        .iter_mut()
                                                        .find(|subscr| subscr.subscr_id == subscription_id) {
                                                            subscr.confirmed = true;
                                                    }
                                                }
                                        }
                                        Err(err) => {
                                            // The subscription params are invalid. Send an error frame to the caller.
                                            if let Ok(mut response) = RpcMessage::new_request("", METH_SUBSCRIBE, None).prepare_response()
                                                && let Ok(err_frame) = response
                                                    .set_error(RpcError::new(RpcErrorCode::InvalidParam, err)).to_frame() {
                                                        notifications_tx.unbounded_send(err_frame).unwrap_or_default();
                                                }
                                        }
                                    }
                                } else {
                                    // Subscribe called before the SHV API version has been
                                    // determined. Send an error frame to the caller.
                                    if let Ok(mut response) = RpcMessage::new_request("", METH_SUBSCRIBE, None).prepare_response()
                                        && let Ok(err_frame) = response
                                            .set_error(RpcError::new(RpcErrorCode::InternalError, "Unable to subscribe, because SHV API version is unknown.")).to_frame() {
                                                notifications_tx.unbounded_send(err_frame).unwrap_or_default();
                                        }
                                }
                            },
                            Unsubscribe { subscription_id } => {
                                let api_version = shv_api_version.as_ref().expect("SHV API version is known when dropping a subscription");
                                if let Some(unsubscribe_req) = subscriptions.remove(api_version, subscription_id) {
                                    client_cmd_tx
                                        .send_message(unsubscribe_req)
                                        .unwrap_or_else(|e| error!("Cannot send Unsubscribe command through ClientCommand channel: {e}"));
                                }
                            }
                            MountNode { path, node } => {
                                if self.mounts.contains_key(&path) {
                                    warn!("Another node already mounted on path `{path}`");
                                }
                                self.mounts.insert(path.clone(), node);
                            }
                            UnmountNode { path } => {
                                if self.mounts.remove(&path).is_none() {
                                    warn!("No node to unmount on path `{path}`");
                                }
                            }
                            TerminateClient => {
                                info!("TerminateClient command received, exiting client loop");
                                return Ok(());
                            },
                        }
                        next_client_cmd = client_cmd_rx.next().fuse();
                    },
                    None => {
                        // This should not happen because we keep a copy of
                        // client_cmd_tx in this task, so at least one sender
                        // exists and the close() method of the underlying TX
                        // channel is not accessible to the user.
                        panic!("ClientCommand channel has been unexpectedly closed");
                    },
                },
                conn_event_result = next_conn_event => match conn_event_result {
                    Some(conn_event) => {
                        use ConnectionEvent::*;
                        match conn_event {
                            RpcFrameReceived(frame) => {
                                self
                                    .process_rpc_frame(
                                        frame,
                                        &client_cmd_tx,
                                        &mut pending_rpc_calls,
                                        &mut subscriptions,
                                        &mut subscription_requests,
                                        &shv_api_version,
                                    )
                                    .await
                                    .unwrap_or_else(|e| error!("Cannot process an RPC frame: {e}"));
                                }
                            ConnectionFailed(kind) => {
                                if let Err(err) = client_events_tx.try_broadcast(ClientEvent::ConnectionFailed(kind)) {
                                    error!("Client event `ConnectionFailed` broadcast error: {err}");
                                }
                            }
                            Connected(sender) => {
                                conn_cmd_sender = Some(sender);
                                // Check SHV API version
                                let client_cmd_tx = client_cmd_tx.clone();
                                let api_version_tx = api_version_tx.clone();
                                crate::runtime::spawn_task(async move {
                                    let api_version_res = check_shv_api_version(client_cmd_tx)
                                        .await
                                        .inspect_err(|e| warn!("check_api_version failed: {e}"));
                                    api_version_tx
                                        .unbounded_send(api_version_res)
                                        .unwrap_or_else(|e| warn!("check_api_version send result failed: {e}"));
                                }).detach();
                            }
                            HeartbeatTimeout => {
                                if let Some(api_version) = &shv_api_version {
                                    let broker_app_path = match api_version {
                                        ShvApiVersion::V2 => ".broker/app",
                                        ShvApiVersion::V3 => ".app",
                                    };
                                    let message = RpcMessage::new_request(broker_app_path, METH_PING, None);
                                    client_cmd_tx
                                        .send_message(message)
                                        .unwrap_or_else(|e|
                                            error!("Cannot send ping through ClientCommand channel: {e}")
                                        );
                                } else {
                                    warn!("Unable to send ping, because SHV API version is unknown.");
                                }
                            }
                            Disconnected => {
                                conn_cmd_sender = None;
                                // NOTE: When the client is disconnected, the broker also knows that
                                // (because of heartbeats) and it should remove all the subscriptions
                                // registered by the client, so the client can also safely clear
                                // the subscriptions here.
                                subscriptions.clear();
                                subscription_requests.clear();
                                pending_rpc_calls.clear();
                                rpc_call_timers.clear();
                                if let Err(err) = client_events_tx.try_broadcast(ClientEvent::Disconnected) {
                                    error!("Client event `Disconnected` broadcast error: {err}");
                                }
                            }
                        }
                        next_conn_event = conn_events_rx.next().fuse();
                    }
                    None => {
                        info!("Connection task terminated, exiting client loop");
                        return Ok(());
                    }
                },
                api_version_result = api_version_rx.next() => match api_version_result {
                    Some(Ok(api_version)) => {
                        info!("SHV API version {}", match api_version {
                            ShvApiVersion::V2 => "v2",
                            ShvApiVersion::V3 => "v3",
                        });
                        shv_api_version = Some(api_version.clone());
                        if let Err(err) = client_events_tx.try_broadcast(ClientEvent::Connected(api_version)) {
                            error!("Client event `Connected` broadcast error: {err}");
                        }
                    }
                    _ => {
                        // TODO: the client might send `Connected` with `ShvApiVersion::Unknown` or
                        // similar, so the client can communicate in a restricted mode. It would
                        // have to send heartbeats and could not do subscriptions.
                        if let Err(err) = client_events_tx.try_broadcast(ClientEvent::ConnectionFailed(ConnectionFailedKind::NetworkError)) {
                            error!("Client event `ConnectionFailed` broadcast error: {err}");
                        }
                    }
                }
            }
        }
    }

    async fn process_rpc_frame(
        &self,
        frame: RpcFrame,
        client_cmd_tx: &ClientCommandSender<T>,
        pending_rpc_calls: &mut HashMap<i64, (Sender<RpcFrame>, UnboundedSender<()>)>,
        subscriptions: &mut Subscriptions,
        subscription_requests: &mut HashMap<RqId, u64>,
        api_version: &Option<ShvApiVersion>,
    ) -> shvrpc::Result<()> {

        fn send_subscription_frame(subscr: &SubscriptionEntry, frame: RpcFrame) {
            if subscr.sender.unbounded_send(frame).is_err() {
                warn!(
                    "Notification channel for RI `{}`, id `{}` closed while the subscription is still active",
                    &subscr.glob.as_ri(), subscr.subscr_id
                );
            }
        }

        if frame.is_request() {
            if let Ok(mut request_msg) = frame.to_rpcmesage() {
                if let Ok(mut resp) = request_msg.prepare_response() {
                    let shv_path = frame.shv_path().unwrap_or_default();
                    let local_result = process_local_dir_ls(&self.mounts, &frame);
                    match local_result {
                        None => {
                            if let Some((mount, path)) = find_longest_path_prefix(&self.mounts, shv_path) {
                                request_msg.set_shvpath(path);
                                let node = self.mounts.get(mount).unwrap_or_else(|| panic!("A node on path '{mount}' should exist"));
                                node.process_request(request_msg, mount.to_owned(), client_cmd_tx.clone(), &self.app_state).await;
                            } else {
                                let method = frame.method().unwrap_or_default();
                                resp.set_error(RpcError::new(
                                    RpcErrorCode::MethodNotFound,
                                    format!("Invalid shv path {shv_path}:{method}()"),
                                ));
                                client_cmd_tx.send_message(resp)?;
                            }
                        }
                        Some(result) => {
                            match result {
                                RequestResult::Response(r) => {
                                    resp.set_result(r);
                                    client_cmd_tx.send_message(resp)?;
                                }
                                RequestResult::Error(e) => {
                                    resp.set_error(e);
                                    client_cmd_tx.send_message(resp)?;
                                }
                            }
                        }
                    };
                } else {
                    warn!("Invalid request frame received.");
                }
            } else {
                warn!("Invalid shv request");
            }
        } else if frame.is_response() {
            if let (Some(req_id), Ok(rpcmsg)) = (frame.request_id(), frame.to_rpcmesage()) {
                let frame_sender = if rpcmsg.is_delay() {
                    // Update the RPC call timer
                    pending_rpc_calls
                        .get(&req_id)
                        .map(|(frame_sender, timer_updater)| {
                            timer_updater.unbounded_send(()).unwrap_or_default();
                            frame_sender.clone()
                        })
                } else {
                    pending_rpc_calls
                        .remove(&req_id)
                        .map(|(frame_sender, _)| frame_sender)
                };

                if let Some(frame_sender) = frame_sender {
                    frame_sender
                        .unbounded_send(frame)
                        .unwrap_or_default();
                } else if let Some(subscr_id) = subscription_requests.remove(&req_id)
                    && let Some(subscr) = subscriptions.0.iter_mut().find(|s| s.subscr_id == subscr_id) {
                        send_subscription_frame(subscr, frame);
                        subscr.confirmed = true;
                    }
            }
        } else if frame.is_signal()
            && let (Some(path), source, signal) = (frame.shv_path(), frame.source(), frame.method())
                && let Ok(notification_ri) = ShvRI::from_path_method_signal(path, source.unwrap_or_default(), signal) {
                    if let Some(api_version) = api_version {
                        for subscr in &subscriptions.0 {
                            if subscr.confirmed && subscr.matches(&notification_ri, api_version) {
                                debug!("Send subscription frame: {frame:?}");
                                send_subscription_frame(subscr, frame.clone());
                            }
                        }
                    } else {
                        warn!("Cannot process a notification frame {notification_ri}, because SHV API version is unknown.");
                    }
                }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use futures::Future;
    use generics_alias::*;

    mod drivers {
        use super::*;
        use crate::appnodes::DotAppNode;
        use futures_time::future::FutureExt;
        use futures_time::time::Duration;
        use crate::clientnode::{SIG_CHNG, PROPERTY_METHODS};
        use shvrpc::metamethod::AccessLevel;

        struct ConnectionMock {
            conn_evt_tx: Sender<ConnectionEvent>,
            conn_cmd_rx: Receiver<ConnectionCommand>,
        }

        impl Drop for ConnectionMock {
            fn drop(&mut self) {
                if self.conn_evt_tx.unbounded_send(ConnectionEvent::Disconnected).is_err() {
                    error!("Disconnected event send error");
                }
            }
        }

        impl ConnectionMock {
            fn new(conn_evt_tx: &Sender<ConnectionEvent>) -> Self {
                let (conn_cmd_tx, conn_cmd_rx) = futures::channel::mpsc::unbounded::<ConnectionCommand>();
                conn_evt_tx.unbounded_send(ConnectionEvent::Connected(conn_cmd_tx)).expect("Connected event send error");
                Self {
                    conn_evt_tx: conn_evt_tx.clone(),
                    conn_cmd_rx,
                }
            }

            fn emulate_receive_request(&self, request: RpcMessage) {
                self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(request.to_frame().unwrap())).unwrap();
            }

            fn emulate_receive_response(&self, from_request: &RpcMessage, result: impl Into<RpcValue>) {
                let mut resp = from_request.prepare_response().unwrap();
                resp.set_result(result);
                self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(resp.to_frame().unwrap())).unwrap();
            }

            fn emulate_receive_delay(&self, from_request: &RpcMessage, progress: f64) {
                let mut resp = from_request.prepare_response().unwrap();
                resp.set_delay(progress);
                self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(resp.to_frame().unwrap())).unwrap();
            }

            fn emulate_receive_signal(&self, path: &str, sig_name: &str, param: Option<RpcValue>) {
                let sig = RpcMessage::new_signal(path, sig_name, param);
                self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(sig.to_frame().unwrap())).unwrap();
            }

            async fn expect_send_message(&mut self) -> RpcMessage {
                let Some(ConnectionCommand::SendMessage(msg)) = self.conn_cmd_rx.next().await else {
                    panic!("ConnectionCommand receive error");
                };
                msg
            }
        }

        async fn expect_client_connected(client_events_rx: &mut ClientEventsReceiver) {
            let ClientEvent::Connected(_) = client_events_rx.wait_for_event().await.expect("Client event receive") else {
                panic!("Expected Connected client event");
            };
        }

        async fn expect_client_disconnected(client_events_rx: &mut ClientEventsReceiver) {
            let ClientEvent::Disconnected = client_events_rx.wait_for_event().await.expect("Client event receive") else {
                panic!("Expected Disconnected client event");
            };
        }

        async fn init_connection(
            conn_evt_tx: &Sender<ConnectionEvent>,
            cli_evt_rx: &mut ClientEventsReceiver,
            api_version: ShvApiVersion,
        ) -> ConnectionMock {
            let mut conn_mock = ConnectionMock::new(conn_evt_tx);
            // Check SHV API version
            let req = conn_mock.expect_send_message().await;
            assert_eq!(req.shv_path(), Some(".broker"));
            assert_eq!(req.method(), Some("ls"));
            let resp = match api_version {
                ShvApiVersion::V2 => vec![RpcValue::from("app")],
                ShvApiVersion::V3 => vec![RpcValue::from("client")],
            };
            conn_mock.emulate_receive_response(&req, resp);

            expect_client_connected(cli_evt_rx).await;
            conn_mock
        }

        const SHV_API_VERSION_DEFAULT: ShvApiVersion = ShvApiVersion::V3;

        pub(super) async fn receive_connected_and_disconnected_events<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            _cli_cmd_tx: ClientCommandSender<T>,
            mut client_events_rx: ClientEventsReceiver,
        ) {
            {
                let _conn_mock = init_connection(&conn_evt_tx, &mut client_events_rx, SHV_API_VERSION_DEFAULT).await;
            }
            expect_client_disconnected(&mut client_events_rx).await;

            let _conn_mock = init_connection(&conn_evt_tx, &mut client_events_rx, SHV_API_VERSION_DEFAULT).await;
        }

        pub(super) async fn send_message<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;

            cli_cmd_tx.send_message(RpcMessage::new_request(
                    "path/test",
                    "test_method",
                    Some(42.into())))
                .expect("Client command send");

            let msg = conn_mock.expect_send_message().await;

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&42.into()));
        }

        pub(super) async fn send_message_fails<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;

            cli_cmd_tx.send_message(RpcMessage::new_request(
                    "path/test",
                    "test_method",
                    Some(42.into())))
                .expect("Client command send");

            let msg = conn_mock.expect_send_message().await;

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&RpcValue::from(41)));
        }

        async fn receive_rpc_msg(rx: &mut Receiver<RpcFrame>) -> RpcMessage {
            rx.next().await.unwrap().to_rpcmesage().unwrap()
        }

        async fn receive_notification<T>(rx: &mut Subscriber<T>) -> RpcMessage {
            rx.next().await.unwrap().to_rpcmesage().unwrap()
        }

        pub(super) async fn call_method_and_receive_response<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;
            let mut resp_rx = cli_cmd_tx
                .do_rpc_call("path/to/resource", "get", None, None)
                .expect("RpcCall command send");

            let req = conn_mock.expect_send_message().await;
            conn_mock.emulate_receive_response(&req, 42);

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_response());
            assert_eq!(resp.response().unwrap().success().unwrap(), &RpcValue::from(42));
        }

        pub(super) async fn call_method_and_receive_error_timeout_response<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;
            let mut resp_rx = cli_cmd_tx
                .do_rpc_call("path/to/resource", "get", None, Some(Duration::from_millis(100)))
                .expect("RpcCall command send");

            let _req = conn_mock.expect_send_message().await;
            sleep(Duration::from_millis(200)).await;

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_error());
            assert_eq!(resp.error().unwrap().code, RpcErrorCode::MethodCallTimeout.into());
        }

        pub(super) async fn call_method_and_receive_delay<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;
            let mut resp_rx = cli_cmd_tx
                .do_rpc_call("path/to/resource", "get", None, Some(Duration::from_millis(100)))
                .expect("RpcCall command send");

            let req = conn_mock.expect_send_message().await;
            sleep(Duration::from_millis(50)).await;
            conn_mock.emulate_receive_delay(&req, 0.2);
            sleep(Duration::from_millis(50)).await;
            conn_mock.emulate_receive_delay(&req, 0.4);
            sleep(Duration::from_millis(50)).await;
            conn_mock.emulate_receive_delay(&req, 0.6);
            sleep(Duration::from_millis(50)).await;
            conn_mock.emulate_receive_delay(&req, 0.8);
            sleep(Duration::from_millis(50)).await;
            conn_mock.emulate_receive_response(&req, 42);

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_delay());
            assert_eq!(resp.response().unwrap().delay(), Some(0.2));

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_delay());
            assert_eq!(resp.response().unwrap().delay(), Some(0.4));

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_delay());
            assert_eq!(resp.response().unwrap().delay(), Some(0.6));

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_delay());
            assert_eq!(resp.response().unwrap().delay(), Some(0.8));

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_success());
            assert_eq!(resp.response().unwrap().success(), Some(&42.into()));
        }

        pub(super) async fn call_method_timeouts_when_disconnected<T>(
            _conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut _cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut resp_rx = cli_cmd_tx
                .do_rpc_call("path/to/resource", "get", None, None)
                .expect("RpcCall command send");
            receive_rpc_msg(&mut resp_rx).timeout(Duration::from_millis(1000)).await.expect_err("Unexpected method call response");
        }

        async fn check_notification_received<T>(
            notify_rx: &mut Subscriber<T>,
            path: Option<&str>,
            method: Option<&str>,
            param: Option<&RpcValue>,
        ) {
            let received_msg = receive_notification(notify_rx)
                .timeout(Duration::from_millis(3000)).await
                .unwrap_or_else(|_| panic!("Notification for path `{:?}`, signal `{:?}`, param `{:?}` not received", &path, &method, &param));
            assert!(received_msg.is_signal());
            assert_eq!(received_msg.shv_path(), path);
            assert_eq!(received_msg.method(), method);
            assert_eq!(received_msg.param(), param);
        }

        // Notifications in SHV API v2
        pub(super) async fn receive_subscribed_notification_v2<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, ShvApiVersion::V2).await;
            crate::runtime::spawn_task(async move {
                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");
                //
                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(43.into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("baz".into()));
                }
            ).detach();

            let mut notify_rx = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut notify_rx_dup = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut notify_rx_wildcard = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some("*")).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut notify_rx_prefix = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut notify_rx_wildcard, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_wildcard, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx_wildcard, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx_wildcard, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
        }

        pub(super) async fn do_not_receive_unsubscribed_notification_v2<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, ShvApiVersion::V2).await;

            let (tx, _rx) = futures::channel::oneshot::channel();
            crate::runtime::spawn_task(async move {
                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                // Path mismatch
                conn_mock.emulate_receive_signal("path/to/resource2", SIG_CHNG, Some(42.into()));
                conn_mock.emulate_receive_signal("path/to/res", SIG_CHNG, Some(42.into()));
                // Signal mismatch
                conn_mock.emulate_receive_signal("path/to/resource", "mntchng", Some(42.into()));

                // Keep the channels in conn_mock alive until the recieve_notification in the
                // parent task times out.
                let _ = tx.send(conn_mock);
            }).detach();

            let mut notify_rx = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            receive_notification(&mut notify_rx)
                .timeout(Duration::from_millis(1000))
                .await
                .expect_err("Unexpected notification received");
        }

        pub(super) async fn subscribe_and_unsubscribe_v2<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, ShvApiVersion::V2).await;

            let (tx, rx) = futures::channel::oneshot::channel();
            crate::runtime::spawn_task(async move {
                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                let _ = tx.send(conn_mock);
            }).detach();

            let mut notify_rx_1 = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");


            let mut notify_rx_2 = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut conn_mock = rx.await.unwrap();

            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
            check_notification_received(&mut notify_rx_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;

            drop(notify_rx_1);
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;

            drop(notify_rx_2);
            let unsubscribe_req = conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Unsubscribe request timeout");
            assert_eq!(unsubscribe_req.shv_path(), Some(BROKER_APP_NODE));
            assert_eq!(unsubscribe_req.method(), Some("unsubscribe"));

            let param = unsubscribe_req.param().expect("Unsubscribe request has param");
            let param = SubscriptionParam::from_rpcvalue(param).unwrap();
            assert_eq!(param.ri.signal(), Some(SIG_CHNG));
            assert_eq!(param.ri.path(), "path/to/resource");
            assert!(param.ttl.is_none());
        }

        // Notifications in SHV API v3
        pub(super) async fn receive_subscribed_notification_v3<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, ShvApiVersion::V3).await;
            crate::runtime::spawn_task(async move {
                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");
                //
                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(43.into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("baz".into()));
            }).detach();

            let mut notify_rx = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut notify_rx_dup = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut notify_rx_prefix = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/*", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
        }

        pub(super) async fn do_not_receive_unsubscribed_notification_v3<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, ShvApiVersion::V3).await;

            let (tx, _rx) = futures::channel::oneshot::channel();
            crate::runtime::spawn_task(async move {
                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                // Path mismatch
                conn_mock.emulate_receive_signal("path/to/resource2", SIG_CHNG, Some(42.into()));
                conn_mock.emulate_receive_signal("path/to/res", SIG_CHNG, Some(42.into()));
                // Signal mismatch
                conn_mock.emulate_receive_signal("path/to/resource", "mntchng", Some(42.into()));

                // Keep the channels in conn_mock alive until the recieve_notification in the
                // parent task times out.
                let _ = tx.send(conn_mock);
            }).detach();

            let mut notify_rx = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            receive_notification(&mut notify_rx)
                .timeout(Duration::from_millis(1000))
                .await
                .expect_err("Unexpected notification received");
        }

        pub(super) async fn subscribe_and_unsubscribe_v3<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, ShvApiVersion::V3).await;

            let (tx, rx) = futures::channel::oneshot::channel();
            crate::runtime::spawn_task(async move {
                let subscription_req = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // The subscription response
                conn_mock.emulate_receive_response(&subscription_req, ());

                let _ = tx.send(conn_mock);
            }).detach();

            let mut notify_rx_1 = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");


            let mut notify_rx_2 = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut conn_mock = rx.await.unwrap();

            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
            check_notification_received(&mut notify_rx_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;

            drop(notify_rx_1);
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;

            drop(notify_rx_2);
            let unsubscribe_req = conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Unsubscribe request timeout");
            assert_eq!(unsubscribe_req.shv_path(), Some(BROKER_CURRENT_CLIENT_NODE));
            assert_eq!(unsubscribe_req.method(), Some("unsubscribe"));
            let param = unsubscribe_req.param().expect("Unsubscribe request has param");
            let param = SubscriptionParam::from_rpcvalue(param).unwrap();
            assert_eq!(param.ri.signal(), Some(SIG_CHNG));
            assert_eq!(param.ri.path(), "path/to/resource");
            assert!(param.ttl.is_none());
        }

        // Tests that notifications are forwarded to subscribers only for confirmed
        // subscriptions — those that have received a response to their subscription request.
        pub(super) async fn send_notifications_only_for_confirmed_subscriptions<T>(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender<T>,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, ShvApiVersion::V3).await;
            crate::runtime::spawn_task(async move {
                // 1st subscription
                let subscr_req_1 = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // 1st subscription response
                conn_mock.emulate_receive_response(&subscr_req_1, ());

                // 2nd subscription
                let subscr_req_2 = conn_mock.expect_send_message()
                    .timeout(Duration::from_millis(1000))
                    .await
                    .expect("Subscribe request timeout");

                // These signals should be passed only to the first subscriber; the second is
                // still waiting for the subscription response.
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));

                // 2nd subscription response
                conn_mock.emulate_receive_response(&subscr_req_2, ());

                // These signals should be received by both subscribers.
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(43.into()));
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("baz".into()));
            }).detach();

            let mut subscriber_1 = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/resource", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            let mut subscriber_2 = cli_cmd_tx
                .subscribe(ShvRI::from_path_method_signal("path/to/*", "*", Some(SIG_CHNG)).unwrap())
                .await
                .expect("ClientCommand subscribe send");

            check_notification_received(&mut subscriber_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut subscriber_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;

            check_notification_received(&mut subscriber_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut subscriber_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut subscriber_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut subscriber_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
        }

        // Request handling tests
        //
        pub(super) fn make_client_with_handlers() -> Client<Full,()> {
            async fn methods_getter(path: String, _: Option<AppState<()>>) -> Option<MetaMethods> {
                if path.is_empty() {
                    Some(MetaMethods::from(&PROPERTY_METHODS))
                } else {
                    None
                }
            }

            async fn request_handler<T>(rq: RpcMessage, client_cmd_tx: ClientCommandSender<T>) {
                let mut resp = rq.prepare_response().unwrap();
                match rq.method() {
                    Some(crate::clientnode::METH_LS) => {
                        resp.set_result("ls");
                    },
                    Some(crate::clientnode::METH_GET) => {
                        resp.set_result("get");
                    },
                    Some(crate::clientnode::METH_SET) => {
                        resp.set_result("set");
                    },
                    _ => {
                        resp.set_error(RpcError::new(
                                RpcErrorCode::MethodNotFound,
                                format!("Unknown method '{:?}'", rq.method())));
                    }
                }
                client_cmd_tx.send_message(resp).unwrap();
            }

            Client::new()
                .app(DotAppNode::new("test"))
                .mount_dynamic("dynamic/sync",
                    MethodsGetter::new(methods_getter),
                    RequestHandler::stateless(request_handler))
                .mount_dynamic("dynamic/async",
                    MethodsGetter::new(methods_getter),
                    RequestHandler::stateless(request_handler))
                .mount_fixed("static",
                    PROPERTY_METHODS,
                    [Route::new([crate::clientnode::METH_GET, crate::clientnode::METH_SET],
                        RequestHandler::stateless(request_handler))])
        }

        async fn recv_request_get_response(conn_mock: &mut ConnectionMock, request: RpcMessage) -> RpcMessage {
            conn_mock.emulate_receive_request(request);
            conn_mock.expect_send_message().await
        }

        pub(super) async fn handle_method_calls<T>(conn_evt_tx: Sender<ConnectionEvent>,
                                         _cli_cmd_tx: ClientCommandSender<T>,
                                         mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;

            {
                // Nonexisting method or path
                let request = RpcMessage::new_request("dynamic/a", "dir", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());

                let request = RpcMessage::new_request("dynamic/sync", "bar", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());

                let request = RpcMessage::new_request("static/none", "dir", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());

                let request = RpcMessage::new_request("static", "foo", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());
            }

            {
                // Access level is missing
                let request = RpcMessage::new_request("dynamic/async", "dir", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::InvalidRequest.into());
            }

            {
                // Requests to a valid method with sufficient permissions
                let mut request = RpcMessage::new_request("static", "get", None);
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_str(), "get");

                let mut request = RpcMessage::new_request("dynamic/sync", "set", None);
                request.set_access_level(AccessLevel::Service);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_str(), "set");

                let mut request = RpcMessage::new_request("dynamic/async", "get", None);
                request.set_access_level(AccessLevel::Superuser);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_str(), "get");

                let mut request = RpcMessage::new_request("dynamic/async", "dir", None);
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_list().len(), 5);
            }

            {
                // Insufficient permissions
                let mut request = RpcMessage::new_request("static", "set", None);
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.response().expect_err("Response should be Err").code, RpcErrorCode::PermissionDenied.into());

                let mut request = RpcMessage::new_request("dynamic/sync", "set", None);
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.response().expect_err("Response should be Err").code, RpcErrorCode::PermissionDenied.into());

                let mut request = RpcMessage::new_request("dynamic/async", "get", None);
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.response().expect_err("Response should be Err").code, RpcErrorCode::PermissionDenied.into());
            }
        }
    }

    macro_rules! def_test{
        ($name:ident $(#[$attr:meta])* $(,$client:expr)?) => {
            mk_test_fn_args!($name $(#[$attr])* $(,$client)?);
        };
    }

    macro_rules! mk_test_fn_args {
        ($name:ident $(#[$attr:meta])* , $client:expr) => {
            mk_test_fn!($name ($(#[$attr])*) Some($client));
        };
        ($name:ident $(#[$attr:meta])*) => {
            mk_test_fn!($name ($(#[$attr])*) None::<$crate::Client<Full,()>>);
        };
    }

    macro_rules! mk_test_fn {
        ($name:ident ($(#[$attr:meta])*) $client_opt:expr) => {

            #[test]
            $(#[$attr])*
            fn $name() {
                run_test($crate::client::tests::drivers::$name, $client_opt);
            }
        };
    }

    generics_def!(TestDriverBounds <C, F, S> where
                  C: FnOnce(Sender<ConnectionEvent>, ClientCommandSender<S>, ClientEventsReceiver) -> F,
                  F: Future + Send + 'static,
                  F::Output: Send + 'static,
                  S: Send + Sync + 'static,
                  );


    macro_rules! def_tests {
        ($($name:ident $(#[$attr:meta])* $(($client:expr))?),+$(,)?) => {

            use crate::appnodes::DotAppNode;

            $(def_test!($name $(#[$attr])* $(,$client)?);)+

            #[generics(TestDriverBounds)]
            async fn init_client(test_drv: C, custom_client: Option<Client<Full,S>>) {
                let mut client = custom_client.unwrap_or_else(|| Client::new().app(DotAppNode::new("test")));
                let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
                let (join_handle_tx, mut join_handle_rx) = futures::channel::mpsc::unbounded();
                let init_handler = move |cli_cmd_tx, cli_evt_rx| {
                    let join_test_handle = crate::runtime::spawn_task(test_drv(conn_evt_tx, cli_cmd_tx, cli_evt_rx));
                    join_handle_tx.unbounded_send(join_test_handle).unwrap();
                };
                client.client_loop(conn_evt_rx, Some(init_handler)).await.expect("Client loop terminated with an error");
                let join_handle = join_handle_rx.next().await.expect("fetch test join handle");
                let _res = join_handle.0.await;

                #[cfg(feature = "tokio")]
                _res.expect("Test finished with error");
            }

            #[generics(TestDriverBounds)]
            pub fn run_test(test_drv: C, custom_client: Option<Client<Full,S>>) {
                let _ = simple_logger::init_with_level(Level::Debug);

                #[cfg(feature = "tokio")]
                ::tokio::runtime::Builder::new_multi_thread()
                    .build()
                    .unwrap()
                    .block_on(init_client(test_drv, custom_client));

                #[cfg(feature = "async_std")]
                ::async_std::task::block_on(init_client(test_drv, custom_client));

                #[cfg(feature = "smol")]
                ::smol::block_on(init_client(test_drv, custom_client));
            }
        };
    }

    use drivers::make_client_with_handlers;

    def_tests! {
        receive_connected_and_disconnected_events,
        send_message,
        send_message_fails #[should_panic],
        call_method_timeouts_when_disconnected,
        call_method_and_receive_response,
        call_method_and_receive_error_timeout_response,
        call_method_and_receive_delay,
        receive_subscribed_notification_v2,
        do_not_receive_unsubscribed_notification_v2,
        subscribe_and_unsubscribe_v2,
        receive_subscribed_notification_v3,
        do_not_receive_unsubscribed_notification_v3,
        subscribe_and_unsubscribe_v3,
        send_notifications_only_for_confirmed_subscriptions,
        handle_method_calls (make_client_with_handlers()),
    }

}

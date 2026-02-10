use std::pin::Pin;

use futures::{Stream, StreamExt};
use futures_time::time::Duration;
use log::{error, warn};
use shvproto::RpcValue;
use shvrpc::rpc::ShvRI;
use shvrpc::rpcdiscovery::{DirParam, DirResult, LsParam, LsResult, MethodInfo};
use shvrpc::rpcmessage::RpcError;
use shvrpc::{RpcFrame, RpcMessage};

use private::next_subscription_id;

use crate::clientnode::{METH_DIR, METH_LS};
use crate::ConnectionFailedKind;

pub(crate) const METH_SUBSCRIBE: &str = "subscribe";
pub(crate) const METH_UNSUBSCRIBE: &str = "unsubscribe";

#[derive(Debug, Clone)]
pub enum ShvApiVersion {
    V2,
    V3,
}

pub type Sender<K> = futures::channel::mpsc::UnboundedSender<K>;
pub type Receiver<K> = futures::channel::mpsc::UnboundedReceiver<K>;

type BroadcastReceiver<K> = async_broadcast::Receiver<K>;

mod private {
    static SUBSCRIPTION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    pub(super) fn next_subscription_id() -> u64 {
        SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

pub struct Subscriber {
    notifications_rx: Receiver<RpcFrame>,
    // For unsubscribe on drop
    client_cmd_tx: Sender<ClientCommand>,
    ri: ShvRI,
    subscription_id: u64,
}

impl Subscriber {
    pub fn path_signal(&self) -> (&str, &str) {
        (self.ri.path(), self.ri.signal().unwrap_or("*"))
    }
    pub fn ri(&self) -> &ShvRI {
        &self.ri
    }
}

impl futures::Stream for Subscriber {
    type Item = RpcFrame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().notifications_rx.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.notifications_rx.size_hint()
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        if self.client_cmd_tx.is_closed() {
            return;
        }

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

pub struct ClientCommandSender {
    pub(crate) sender: Sender<ClientCommand>,
}

impl Clone for ClientCommandSender {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

impl ClientCommandSender {
    #[cfg(any(feature = "mocking", test))]
    pub fn from_raw(sender: Sender<ClientCommand>) -> Self {
        Self { sender }
    }

    pub fn terminate_client(&self) {
        self.sender
            .unbounded_send(ClientCommand::TerminateClient)
            .unwrap_or_else(|e| error!("Failed to send TerminateClient command: {e}"));
    }

    pub fn do_rpc_call(
        &self,
        shvpath: impl AsRef<str>,
        method: impl AsRef<str>,
        param: Option<RpcValue>,
        timeout: Option<Duration>,
    ) -> Result<Receiver<RpcFrame>, futures::channel::mpsc::TrySendError<ClientCommand>>
    {
        let (response_sender, response_receiver) = futures::channel::mpsc::unbounded();
        self.sender.unbounded_send(ClientCommand::RpcCall {
            request: RpcMessage::new_request(shvpath, method).with_param(param),
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
        R: for<'a> TryFrom<&'a RpcValue, Error = E> + Send + 'static,
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
        if self.sender.is_closed() {
            return Box::pin(futures::stream::empty());
        }
        let call = self.do_rpc_call(path, method, param, timeout)
            .map_err(|err| {
                warn!("Cannot send RPC request to the client core. \
                    Path: `{path}`, method: `{method}`, error: {err}");
                make_error(ConnectionClosed)
            });
        match call {
            Err(err) => Box::pin(futures::stream::once(async { Err(err) })),
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
        R: for<'a> TryFrom<&'a RpcValue, Error = E> + Send + 'static,
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

    pub fn send_message(&self, message: RpcMessage) -> Result<(), futures::channel::mpsc::TrySendError<ClientCommand>> {
        if self.sender.is_closed() {
            return Ok(());
        }
        self.sender.unbounded_send(ClientCommand::SendMessage { message })
    }

    pub async fn subscribe(&self, ri: ShvRI) -> Result<Subscriber, CallRpcMethodError> {
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


pub enum ClientCommand {
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

    pub async fn exec<R, E>(self, client_cmd_sender: &ClientCommandSender) -> Result<R, CallRpcMethodError>
    where
        R: for<'r> TryFrom<&'r RpcValue, Error = E> + Send + 'static,
        E: std::fmt::Display,
    {
        client_cmd_sender.call_rpc_method(self.path, self.method, self.param, self.timeout, None::<fn(_)>).await
    }

    pub async fn exec_with_progress<R, E>(self, client_cmd_sender: &ClientCommandSender, progress_notifier: impl Fn(f64) + Send + 'static) -> Result<R, CallRpcMethodError>
    where
        R: for<'r> TryFrom<&'r RpcValue, Error = E> + Send + 'static,
        E: std::fmt::Display,
    {
        client_cmd_sender.call_rpc_method(self.path, self.method, self.param, self.timeout, Some(progress_notifier)).await
    }

    pub fn stream<T, R, E>(self, client_cmd_sender: &ClientCommandSender) -> Pin<Box<dyn Stream<Item = Result<RpcCallResponse<R>, CallRpcMethodError>> + Send>>
    where
        R: for<'r> TryFrom<&'r RpcValue, Error = E> + Send + 'static,
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

    pub async fn exec(self, client_cmd_sender: &ClientCommandSender) -> Result<Vec<String>, CallRpcMethodError> {
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

    pub async fn exec(self, client_cmd_sender: &ClientCommandSender) -> Result<bool, CallRpcMethodError> {
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

    pub async fn exec_brief(self, client_cmd_sender: &ClientCommandSender) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
        client_cmd_sender.call_dir_brief(self.path, self.timeout).await
    }

    pub async fn exec_full(self, client_cmd_sender: &ClientCommandSender) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
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

    pub async fn exec(self, client_cmd_sender: &ClientCommandSender) -> Result<bool, CallRpcMethodError> {
        client_cmd_sender.call_dir_exists(self.path, self.method, self.timeout).await
    }
}

#[derive(Clone)]
pub enum ClientEvent {
    ConnectionFailed(ConnectionFailedKind),
    Connected(ShvApiVersion),
    Disconnected,
}

#[derive(Clone)]
pub struct ClientEventsReceiver(pub(crate) BroadcastReceiver<ClientEvent>);

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
    pub async fn wait_for_event(&mut self) -> Result<ClientEvent, async_broadcast::RecvError> {
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


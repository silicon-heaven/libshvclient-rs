use crate::clientapi::{ShvApiVersion, Sender, Receiver, ClientEventsReceiver, CallRpcMethodError, RpcCallLsList, METH_SUBSCRIBE, METH_UNSUBSCRIBE};
use crate::connection::{spawn_connection_task, ConnectionCommand, ConnectionEvent, ConnectionFailedKind};
use crate::clientnode::{process_local_dir_ls, ClientNode, RequestHandlerResult, StaticNode, METH_PING};
use crate::{ClientCommandSender, ClientEvent};
use futures::stream::FuturesUnordered;
use futures::{select, FutureExt, StreamExt};
use futures::channel::mpsc::UnboundedSender;
use futures_time::task::sleep;
use futures_time::time::Duration;
use log::{debug, warn, error, info};
use shvrpc::client::ClientConfig;
use shvrpc::rpc::{Glob, ShvRI, SubscriptionParam};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode, RqId};
use shvrpc::util::find_longest_path_prefix;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;

pub const BROKER_APP_NODE: &str = ".broker/app";
pub const BROKER_CLIENT_NODE: &str = ".broker/client";
pub const BROKER_CURRENT_CLIENT_NODE: &str = ".broker/currentClient";

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

#[derive(Copy, Clone)]
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
                method
            )
            .with_param({
                let mut map = shvproto::Map::new();
                map.insert("signal".to_string(), ri.signal().map(|s| if s == "*" { "" } else { s }).into());
                map.insert("paths".to_string(),ri.path().into());
                map
            }),
        ShvApiVersion::V3 =>
            RpcMessage::new_request(
                BROKER_CURRENT_CLIENT_NODE,
                method
            )
            .with_param(SubscriptionParam { ri: ri.clone(), ttl: None }.to_rpcvalue()),
    }
}

impl Subscriptions {
    fn new() -> Self {
        Self::default()
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
        assert!(!subscriptions.iter().any(|subscr| subscr.subscr_id == subscr_id), "Tried to add a subscription with already existing ID: {subscr_id}. RI: {ri}. Dump: {self:?}");
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

mod private {
    pub trait Sealed { }
}

pub trait ClientVariant: private::Sealed { }

pub enum Plain { }
impl ClientVariant for Plain { }
impl private::Sealed for Plain { }

pub enum Full { }
impl ClientVariant for Full { }
impl private::Sealed for Full { }

pub struct Client<V: ClientVariant> {
    mounts: BTreeMap<String, ClientNode>,
    rpc_call_timeout: Duration,
    variant_marker: PhantomData<V>,
}

const RPC_CALL_DEFAULT_TIMEOUT_SECS: u64 = 10;

impl Client<Plain> {
    pub fn new_plain() -> Self {
        Self {
            mounts: BTreeMap::default(),
            rpc_call_timeout: Duration::from_secs(RPC_CALL_DEFAULT_TIMEOUT_SECS),
            variant_marker: PhantomData,
        }
    }
}

impl Default for Client<Full> {
    fn default() -> Self {
        Self::new()
    }
}

impl Client<Full> {
    pub fn new() -> Self {
        Self {
            mounts: BTreeMap::default(),
            rpc_call_timeout: Duration::from_secs(RPC_CALL_DEFAULT_TIMEOUT_SECS),
            variant_marker: PhantomData,
        }
    }

    #[must_use]
    pub fn app(self, app_node: crate::appnodes::DotAppNode) -> Self {
        self.mount(".app", ClientNode::new_static(app_node))
    }

    #[must_use]
    pub fn device(self, device_node: crate::appnodes::DotDeviceNode) -> Self {
        self.mount(".device", ClientNode::new_static(device_node))
    }

    #[must_use]
    pub fn mount(mut self, path: impl Into<String>, node: ClientNode) -> Self {
        self.mounts.insert(path.into(), node);
        self
    }

    #[must_use]
    pub fn mount_static(mut self, path: impl Into<String>, node: impl StaticNode) -> Self {
        self.mounts.insert(path.into(), ClientNode::new_static(node));
        self
    }

    #[must_use]
    pub fn mount_dynamic<F, Fut>(mut self, path: impl Into<String>, handler: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = RequestHandlerResult> + Send + 'static
    {
        self.mounts.insert(path.into(), ClientNode::new_dynamic(handler));
        self
    }

    pub async fn run(self, config: &ClientConfig) -> shvrpc::Result<()> {
        self.run_with_init_opt(
            config,
            Option::<fn(_,_)>::None,
        )
        .await
    }
}

impl<V: ClientVariant> Client<V> {
    #[must_use]
    pub fn rpc_call_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_call_timeout = timeout;
        self
    }

    async fn run_with_init_opt<H>(
        &self,
        config: &ClientConfig,
        init_handler: Option<H>,
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
    {
        let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
        spawn_connection_task(config, conn_evt_tx);
        self.client_loop(conn_evt_rx, init_handler).await
    }

    pub async fn run_with_init<H>(self, config: &ClientConfig, handler: H) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
    {
        self.run_with_init_opt(config, Some(handler)).await
    }

    #[cfg(feature = "mocking")]
    async fn mock_run_with_init_opt<H>(
        &mut self,
        init_handler: Option<H>,
        conn_evt_rx: futures::channel::mpsc::UnboundedReceiver::<ConnectionEvent>,
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
    {
        self.client_loop(conn_evt_rx, init_handler).await
    }

    #[cfg(feature = "mocking")]
    pub async fn mock_run_with_init<H>(
        mut self,
        handler: H,
        channel: futures::channel::mpsc::UnboundedReceiver::<ConnectionEvent>
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
    {
        self.mock_run_with_init_opt(Some(handler), channel).await
    }

    async fn client_loop<H>(
        &self,
        mut conn_events_rx: Receiver<ConnectionEvent>,
        init_handler: Option<H>,
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
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

        async fn check_shv_api_version(client_cmd_tx: ClientCommandSender) -> Result<ShvApiVersion, CallRpcMethodError>
        {
            let api_version = RpcCallLsList::new(".broker")
                .exec(&client_cmd_tx)
                .await?
                .iter()
                .find_map(|node| match node.as_str() {
                        "client" => Some(ShvApiVersion::V3),
                        "clients" => Some(ShvApiVersion::V2),
                        _ => None,
                    }
                )
            .unwrap_or_else(|| {
                warn!("Cannot detect SHV API version. Using version 3 as a fallback.");
                ShvApiVersion::V3
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
                            && let Ok(mut response) = RpcMessage::new_request("", "").prepare_response()
                                && let Ok(err_frame) = response
                                    .set_error(RpcError::new(RpcErrorCode::MethodCallTimeout, format!("No response received within {duration} secs"))).to_frame() {
                                        response_sender.unbounded_send(err_frame).unwrap_or_default();
                                }
                }
                client_cmd_result = next_client_cmd => match client_cmd_result {
                    Some(client_cmd) => {
                        use crate::clientapi::ClientCommand::*;
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
                                                    msg = timer_update_rx.next() => if msg.is_none() {
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
                                            if let Ok(mut response) = RpcMessage::new_request("", METH_SUBSCRIBE).prepare_response()
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
                                            if let Ok(mut response) = RpcMessage::new_request("", METH_SUBSCRIBE).prepare_response()
                                                && let Ok(err_frame) = response
                                                    .set_error(RpcError::new(RpcErrorCode::InvalidParam, err)).to_frame() {
                                                        notifications_tx.unbounded_send(err_frame).unwrap_or_default();
                                                }
                                        }
                                    }
                                } else {
                                    // Subscribe called before the SHV API version has been
                                    // determined. Send an error frame to the caller.
                                    if let Ok(mut response) = RpcMessage::new_request("", METH_SUBSCRIBE).prepare_response()
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
                                    let message = RpcMessage::new_request(broker_app_path, METH_PING);
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
        client_cmd_tx: &ClientCommandSender,
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
                                node.process_request(request_msg, mount.to_owned(), client_cmd_tx.clone()).await;
                            } else {
                                let method = frame.method().unwrap_or_default();
                                resp.set_error(RpcError::new(
                                    RpcErrorCode::MethodNotFound,
                                    format!("Invalid shv path {shv_path}:{method}()"),
                                ));
                                client_cmd_tx.send_message(resp)?;
                            }
                        }
                        Some(res) => {
                            match res {
                                Ok(val) => resp.set_result(val),
                                Err(err) => resp.set_error(err),
                            };
                            client_cmd_tx.send_message(resp)?;
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
    pub use crate::clientapi::*;
    use futures::Future;
    use generics_alias::*;

    mod drivers {
        use crate::clientnode::{err_unresolved_request, rpc_error_unknown_method_on_path, Method, RequestResult, METH_GET, METH_LS, METH_SET};

        use super::*;
        use crate::appnodes::DotAppNode;
        use async_trait::async_trait;
        use futures_time::future::FutureExt;
        use futures_time::time::Duration;
        use shvproto::RpcValue;
        use crate::clientnode::{RequestHandlerResult, PROPERTY_METHODS, SIG_CHNG};
        use shvrpc::metamethod::{AccessLevel, MetaMethod};

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

            fn emulate_receive_request(&self, request: &RpcMessage) {
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

            fn emulate_receive_signal(&self, path: &str, sig_name: &str, param: impl Into<RpcValue>) {
                let sig = RpcMessage::new_signal(path, sig_name).with_param(param);
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
                ShvApiVersion::V2 => vec![RpcValue::from("clients")],
                ShvApiVersion::V3 => vec![RpcValue::from("client")],
            };
            conn_mock.emulate_receive_response(&req, resp);

            expect_client_connected(cli_evt_rx).await;
            conn_mock
        }

        const SHV_API_VERSION_DEFAULT: ShvApiVersion = ShvApiVersion::V3;

        pub(super) async fn receive_connected_and_disconnected_events(
            conn_evt_tx: Sender<ConnectionEvent>,
            _cli_cmd_tx: ClientCommandSender,
            mut client_events_rx: ClientEventsReceiver,
        ) {
            {
                let _conn_mock = init_connection(&conn_evt_tx, &mut client_events_rx, SHV_API_VERSION_DEFAULT).await;
            }
            expect_client_disconnected(&mut client_events_rx).await;

            let _conn_mock = init_connection(&conn_evt_tx, &mut client_events_rx, SHV_API_VERSION_DEFAULT).await;
        }

        pub(super) async fn send_message(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;

            cli_cmd_tx.send_message(RpcMessage::new_request("path/test", "test_method").with_param(42))
                .expect("Client command send");

            let msg = conn_mock.expect_send_message().await;

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&42.into()));
        }

        pub(super) async fn send_message_fails(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;

            cli_cmd_tx.send_message(RpcMessage::new_request("path/test", "test_method").with_param(42))
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

        async fn receive_notification(rx: &mut Subscriber) -> RpcMessage {
            rx.next().await.unwrap().to_rpcmesage().unwrap()
        }

        pub(super) async fn call_method_and_receive_response(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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

        pub(super) async fn call_method_and_receive_error_timeout_response(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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

        pub(super) async fn call_method_and_receive_delay(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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

        pub(super) async fn call_method_timeouts_when_disconnected(
            _conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut _cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut resp_rx = cli_cmd_tx
                .do_rpc_call("path/to/resource", "get", None, None)
                .expect("RpcCall command send");
            receive_rpc_msg(&mut resp_rx).timeout(Duration::from_millis(1000)).await.expect_err("Unexpected method call response");
        }

        async fn check_notification_received(
            notify_rx: &mut Subscriber,
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
        pub(super) async fn receive_subscribed_notification_v2(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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

                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 42);
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 43);
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "bar");
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "baz");
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

        pub(super) async fn do_not_receive_unsubscribed_notification_v2(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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
                conn_mock.emulate_receive_signal("path/to/resource2", SIG_CHNG, 42);
                conn_mock.emulate_receive_signal("path/to/res", SIG_CHNG, 42);
                // Signal mismatch
                conn_mock.emulate_receive_signal("path/to/resource", "mntchng", 42);

                // Keep the channels in conn_mock alive until the recieve_notification in the
                // parent task times out.
                tx.send(conn_mock).ok();
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

        pub(super) async fn subscribe_and_unsubscribe_v2(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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

                tx.send(conn_mock).ok();
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

            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 42);
            check_notification_received(&mut notify_rx_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;

            drop(notify_rx_1);
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "bar");
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
        pub(super) async fn receive_subscribed_notification_v3(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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

                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 42);
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 43);
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "bar");
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "baz");
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

        pub(super) async fn do_not_receive_unsubscribed_notification_v3(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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
                conn_mock.emulate_receive_signal("path/to/resource2", SIG_CHNG, 42);
                conn_mock.emulate_receive_signal("path/to/res", SIG_CHNG, 42);
                // Signal mismatch
                conn_mock.emulate_receive_signal("path/to/resource", "mntchng", 42);

                // Keep the channels in conn_mock alive until the recieve_notification in the
                // parent task times out.
                tx.send(conn_mock).ok();
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

        pub(super) async fn subscribe_and_unsubscribe_v3(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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

                tx.send(conn_mock).ok();
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

            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 42);
            check_notification_received(&mut notify_rx_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;

            drop(notify_rx_1);
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "bar");
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
        pub(super) async fn send_notifications_only_for_confirmed_subscriptions(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
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
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 42);
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "bar");

                // 2nd subscription response
                conn_mock.emulate_receive_response(&subscr_req_2, ());

                // These signals should be received by both subscribers.
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, 43);
                conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, "baz");
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
        pub(super) fn make_client_with_handlers() -> Client<Full> {
            async fn request_handler(rq: RpcMessage, _client_cmd_tx: ClientCommandSender) -> RequestHandlerResult {
                let path = rq.shv_path().unwrap_or_default();
                if !path.is_empty() {
                    return err_unresolved_request();
                }

                let method = Method::from_request(&rq);

                match method {
                    Method::Dir(dir) => {
                        dir.resolve(PROPERTY_METHODS
                            .iter()
                            .map(|mm| MetaMethod { access: AccessLevel::Command, ..mm.clone() })
                            .collect::<Vec<_>>()
                        )
                    }
                    Method::Ls(ls) => {
                        ls.resolve_opt(PROPERTY_METHODS, async || {
                            Some(Ok(vec!["ls".into()]))
                        })
                    },
                    Method::Other(m) if m.method() == METH_GET => {
                        m.resolve(PROPERTY_METHODS, async || {
                            Ok("get")
                        })
                    },
                    Method::Other(m) if m.method() == METH_SET => {
                        m.resolve_opt(PROPERTY_METHODS, async || {
                            Some(Ok("set"))
                        })
                    },
                    Method::Other(_) => {
                        err_unresolved_request()
                    },
                }
            }

            struct PropertyNode;

            #[async_trait]
            impl StaticNode for PropertyNode {
                fn methods(&self) -> &'static [MetaMethod] {
                    PROPERTY_METHODS
                }

                async fn process_request(&self, request: RpcMessage, _client_cmd_tx: ClientCommandSender) -> Option<RequestResult> {
                    match request.method().unwrap_or_default() {
                        METH_LS => Some(Ok(vec!["ls"].into())),
                        METH_GET => Some(Ok("get".into())),
                        METH_SET => Some(Ok("set".into())),
                        method => Some(Err(rpc_error_unknown_method_on_path(request.shv_path().unwrap_or_default(), method))),
                    }
                }
            }

            Client::new()
                .app(DotAppNode::new("test"))
                .mount_dynamic("dynamic/sync", request_handler)
                .mount_dynamic("dynamic/async", request_handler)
                .mount_static("static", PropertyNode)
                .rpc_call_timeout(Duration::from_secs(10))

        }

        async fn recv_request_get_response(conn_mock: &mut ConnectionMock, request: &RpcMessage) -> RpcMessage {
            conn_mock.emulate_receive_request(request);
            conn_mock.expect_send_message().await
        }

        pub(super) async fn handle_method_calls(conn_evt_tx: Sender<ConnectionEvent>,
                                         _cli_cmd_tx: ClientCommandSender,
                                         mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx, SHV_API_VERSION_DEFAULT).await;

            {
                // Nonexisting method or path
                let mut request = RpcMessage::new_request("dynamic/a", "dir");
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, &request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());

                let mut request = RpcMessage::new_request("dynamic/sync", "bar");
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, &request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());

                let mut request = RpcMessage::new_request("static/none", "dir");
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, &request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());

                let mut request = RpcMessage::new_request("static", "foo");
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, &request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound.into());
            }

            {
                // Access level is missing
                let request = RpcMessage::new_request("dynamic/async", "dir");
                let response = recv_request_get_response(&mut conn_mock, &request).await
                    .response().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::InvalidRequest.into());
            }

            {
                // Requests to a valid method with sufficient permissions
                let mut request = RpcMessage::new_request("static", "get");
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, &request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_str(), "get");

                let mut request = RpcMessage::new_request("dynamic/sync", "set");
                request.set_access_level(AccessLevel::Service);
                let response = recv_request_get_response(&mut conn_mock, &request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_str(), "set");

                let mut request = RpcMessage::new_request("dynamic/async", "get");
                request.set_access_level(AccessLevel::Superuser);
                let response = recv_request_get_response(&mut conn_mock, &request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_str(), "get");

                let mut request = RpcMessage::new_request("dynamic/async", "dir");
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, &request).await;
                assert_eq!(response.response().expect("Response should be Ok").success().unwrap().as_list().len(), 4);
            }

            {
                // Insufficient permissions
                let mut request = RpcMessage::new_request("static", "set");
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, &request).await;
                assert_eq!(response.response().expect_err("Response should be Err").code, RpcErrorCode::MethodNotFound.into());

                let mut request = RpcMessage::new_request("dynamic/sync", "set");
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, &request).await;
                assert_eq!(response.response().expect_err("Response should be Err").code, RpcErrorCode::MethodNotFound.into());

                let mut request = RpcMessage::new_request("dynamic/async", "get");
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, &request).await;
                assert_eq!(response.response().expect_err("Response should be Err").code, RpcErrorCode::MethodNotFound.into());
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
            mk_test_fn!($name ($(#[$attr])*) None::<$crate::Client<Full>>);
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

    generics_def!(TestDriverBounds <C, F> where
                  C: FnOnce(Sender<ConnectionEvent>, ClientCommandSender, ClientEventsReceiver) -> F,
                  F: Future + Send + 'static,
                  F::Output: Send + 'static,
                  );


    macro_rules! def_tests {
        ($($name:ident $(#[$attr:meta])* $(($client:expr))?),+$(,)?) => {

            use crate::appnodes::DotAppNode;

            $(def_test!($name $(#[$attr])* $(,$client)?);)+

            #[generics(TestDriverBounds)]
            async fn init_client(test_drv: C, custom_client: Option<Client<Full>>) {
                let client = custom_client.unwrap_or_else(|| Client::new().app(DotAppNode::new("test")));
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
            pub fn run_test(test_drv: C, custom_client: Option<Client<Full>>) {
                simple_logger::init_with_level(log::Level::Debug).ok();

                #[cfg(feature = "tokio")]
                ::tokio::runtime::Builder::new_multi_thread()
                    .build()
                    .unwrap()
                    .block_on(init_client(test_drv, custom_client));

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

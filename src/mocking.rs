use std::{error::Error, sync::Arc, time::Duration};
use futures::{StreamExt, channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded}};
use log::debug;
use shvproto::RpcValue;
use shvrpc::{RpcMessage, RpcMessageMetaTags, rpc::ShvRI, rpcmessage::{RpcError, RpcErrorCode}};
use tokio::{sync::{Notify, RwLock}, task::JoinHandle};
use crate::{ConnectionCommand, ConnectionEvent};

const TIMEOUT_DURATION: Duration = Duration::from_secs(5);

#[must_use]
pub struct PendingResponse<'a>(i64, &'a TestApp);

impl PendingResponse<'_> {
    pub async fn await_response(self, expected_result: Result<&str, &str>) {
        let rpc_message = self.1.await_and_remove(|msg| msg.is_response() && msg.request_id() == Some(self.0)).await;
        let response = rpc_message.response().map(|resp| resp.success().expect("Expected a success response"));
        match expected_result {
            Ok(expected_result) => {
                let result = response.expect("Expected a success response");
                assert_eq!(*result, RpcValue::from_cpon(expected_result).unwrap_or_else(|err| panic!("Invalid CPON '{expected_result}': {err}")));
            },
            Err(err) => {
                let result = response.expect_err("Expected an Err response");
                assert_eq!(result.to_string(), err);
            },
        }
    }
}

#[must_use]
pub struct PendingRequest(RpcMessage, UnboundedSender<ConnectionEvent>);
impl PendingRequest {
    pub fn respond(mut self, result: Result<&str, &str>) {
        let rq_id = self.0.request_id().expect("RpcMessage must be request");
        debug!(target: "test-driver", "==> rq_id:{rq_id} {result:?}");
        match result {
            Ok(result) => {
                let result = RpcValue::from_cpon(result).unwrap_or_else(|err| panic!("Invalid CPON '{result}': {err}"));
                self.0.set_result(result);
            },
            Err(err) => {
                self.0.set_error(RpcError::new(RpcErrorCode::MethodCallException, err));
            },
        }
        self.1.unbounded_send(ConnectionEvent::RpcFrameReceived(self.0.to_frame().expect("to_frame() must work"))).expect("sending ConnectionEvent must work");
    }
}

pub struct TestApp {
    app: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    msg_task: JoinHandle<()>,
    conn_evt_tx: UnboundedSender<ConnectionEvent>,
    pending_msgs: Arc<RwLock<Vec<RpcMessage>>>,
    notifier: Arc<Notify>,
}

impl TestApp {
    pub fn request(&self, ri: impl TryInto<ShvRI, Error = impl std::fmt::Display>, param: &str) -> PendingResponse<'_> {
        let ri = ri.try_into().unwrap_or_else(|err| panic!("Invalid RI: {err}"));
        let path = ri.path();
        let method = ri.method();
        let mut rpc_message = RpcMessage::new_request(path, method).with_param(RpcValue::from_cpon(param).unwrap_or_else(|err| panic!("Invalid CPON '{param}': {err}")));
        rpc_message.set_user_id("test-driver");
        rpc_message.set_access_level(shvrpc::metamethod::AccessLevel::Superuser);
        let rq_id = rpc_message.request_id().expect("This must be request");
        debug!(target: "test-driver", "==> rq_id:{rq_id} {path}:{method}, param: {param}");
        self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(rpc_message.to_frame().expect("to_frame must work"))).expect("events must work");
        PendingResponse(rq_id, self)
    }

    async fn await_and_remove<F>(&self, matcher: F) -> RpcMessage
    where
        F: Fn(&RpcMessage) -> bool
    {
        loop {
            let waiter = self.notifier.notified();

            let mut pending = self.pending_msgs.write().await;
            let matched_msg = pending.iter().position(&matcher).map(|pos| pending.remove(pos));

            if let Some(matched_msg) = matched_msg {
                return matched_msg;
            }

            drop(pending);
            tokio::time::timeout(TIMEOUT_DURATION, waiter).await.unwrap_or_else(|_err| panic!("Timed out while waiting for message"));
        }
    }

    pub fn signal(&self, ri: impl TryInto<ShvRI, Error = impl std::fmt::Display>, param: &str) {
        let ri = ri.try_into().unwrap_or_else(|err| panic!("Invalid RI: {err}"));
        let path = ri.path();
        let method = ri.method();
        let signal = ri.signal().unwrap_or_else(|| panic!("Signal RI must have a signal: {ri}"));
        debug!(target: "test-driver", "==> {path}:{method}:{signal}, param: {param}");
        let param = RpcValue::from_cpon(param).unwrap_or_else(|err| panic!("Invalid CPON '{param}': {err}"));
        let rpc_message = RpcMessage::new_signal_with_source(path, signal, method).with_param(param);
        self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(rpc_message.to_frame().expect("to_frame must work"))).expect("events must work");
    }

    pub async fn await_signal(&self, expected_ri: impl TryInto<ShvRI, Error = impl std::fmt::Display>, expected_param: &str) {
        let expected_ri = expected_ri.try_into().unwrap_or_else(|err| panic!("Invalid RI: {err}"));
        let expected_signal = expected_ri.signal().unwrap_or_else(|| panic!("Signal RI must have a signal: {expected_ri}"));
        let expected_param = RpcValue::from_cpon(expected_param).unwrap_or_else(|err| panic!("Invalid CPON '{expected_param}': {err}"));

        self.await_and_remove(|rpc_message| {
            if !rpc_message.is_signal() {
                return false;
            }
            let shv_path = rpc_message.shv_path().expect("msg must have a path");
            let method = rpc_message.method().expect("msg must have a method");
            let param = rpc_message.param().cloned().unwrap_or_else(RpcValue::null);
            shv_path == expected_ri.path() && method == expected_signal && param == expected_param
        }).await;
    }

    pub async fn await_request(&self, expected_ri: impl TryInto<ShvRI, Error = impl std::fmt::Display>, expected_param: &str) -> PendingRequest {
        let expected_ri = expected_ri.try_into().unwrap_or_else(|err| panic!("Invalid RI: {err}"));
        let expected_param = RpcValue::from_cpon(expected_param).unwrap_or_else(|err| panic!("Invalid CPON '{expected_param}': {err}"));

        let rpc_message = self.await_and_remove(|rpc_message| {
            if !rpc_message.is_request() {
                return false;
            }
            let shv_path = rpc_message.shv_path().expect("msg must have a path");
            let method = rpc_message.method().expect("msg must have a method");
            let param = rpc_message.param().cloned().unwrap_or_else(RpcValue::null);
            shv_path == expected_ri.path() && method == expected_ri.method() && param == expected_param
        }).await;

        PendingRequest(rpc_message.prepare_response().expect("prepare_response must work"), self.conn_evt_tx.clone())
    }

    pub async fn await_subscription(&self, expected_ri: impl TryInto<ShvRI, Error = impl std::fmt::Display>) {
        let expected_ri = expected_ri.try_into().unwrap_or_else(|err| panic!("Invalid RI: {err}"));
        let expected_ri = expected_ri.as_str();
        self.await_request(".broker/currentClient:subscribe", &format!(r#"["{expected_ri}",null]""#)).await
            .respond(Ok("true"));
    }

    pub fn new(app_maker: impl FnOnce(UnboundedReceiver<ConnectionEvent>) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>) -> Self {
        let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
        let app = app_maker(conn_evt_rx);

        let (conn_cmd_sender_in, mut conn_cmd_receiver_in) = unbounded();
        let pending_msgs = Arc::new(RwLock::new(Vec::new()));
        let notifier = Arc::new(Notify::new());

        let msg_task = {
            let pending_msgs = pending_msgs.clone();
            let notifier = notifier.clone();
            tokio::spawn(async move {
                while let Some(ConnectionCommand::SendMessage(rpc_message)) = conn_cmd_receiver_in.next().await {
                    let shv_path = rpc_message.shv_path().unwrap_or_default();
                    let method = rpc_message.method().unwrap_or_default();
                    let param = rpc_message.param().cloned().unwrap_or_else(RpcValue::null);
                    let msg_prefix = rpc_message.request_id().map_or_else(String::new, |rq_id| format!("rq_id:{rq_id} "));
                    let msg_suffix = rpc_message.response().map(|resp| resp.success().cloned().unwrap_or_else(RpcValue::null));
                    if method.is_empty() && shv_path.is_empty() {
                        debug!(target: "test-driver", "<== {msg_prefix}-> {msg_suffix:?}");
                    } else {
                        debug!(target: "test-driver", "<== {msg_prefix}{shv_path}:{method}, param: {param}");
                    }

                    {
                        pending_msgs.write().await.push(rpc_message);
                    }
                    notifier.notify_waiters();
                }
            })
        };
        conn_evt_tx.unbounded_send(ConnectionEvent::Connected(conn_cmd_sender_in)).expect("Events must work");
        Self {
            app,
            msg_task,
            conn_evt_tx,
            pending_msgs,
            notifier,
        }
    }

    pub async fn wait_until_finished(self) -> shvrpc::Result<()> {
        // Wait for a bit to ensure silence from the app.
        tokio::time::sleep(Duration::from_millis(500)).await;
        self.conn_evt_tx.close_channel();
        let mut pending_msgs = self.pending_msgs.write().await;
        // We'll let unsubscribe calls slide.
        pending_msgs.retain(|msg|
            !msg.is_request() || msg.shv_path() != Some(".broker/currentClient") || msg.method() != Some("unsubscribe")
        );
        for rpc_message in pending_msgs.iter() {
            let shv_path = rpc_message.shv_path().unwrap_or_default();
            let method = rpc_message.method().unwrap_or_default();
            let param = rpc_message.param().cloned().unwrap_or_else(RpcValue::null);
            let msg_prefix = rpc_message.request_id().map_or_else(String::new, |rq_id| format!("rq_id:{rq_id} "));
            if rpc_message.is_response() {
                let result = rpc_message.response().map(|resp| resp.success().expect("Only success responses are supported"));
                debug!(target: "test-driver", "UNEXPECTED <== {msg_prefix} -> {result:?}");
            } else if method.is_empty() && shv_path.is_empty() {
                debug!(target: "test-driver", "UNEXPECTED <== {msg_prefix}{param}");
            } else {
                debug!(target: "test-driver", "UNEXPECTED <== {msg_prefix}{shv_path}:{method}, param: {param}");
            }
        };

        if !pending_msgs.is_empty() {
             return Err("There were unexpected messages received from the app.".into());
        }

        let end = async move {
            self.msg_task.await?;
            self.app.await??;

            Ok(())
        };

        tokio::time::timeout(TIMEOUT_DURATION, end).await?
    }
}


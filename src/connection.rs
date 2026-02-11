use std::sync::Arc;

pub use crate::clientapi::Sender;
use duration_str::HumanFormat;
use futures::{select, AsyncRead, AsyncWrite, FutureExt, StreamExt};
use futures_rustls::pki_types::ServerName;
use futures_rustls::TlsConnector;
use log::{info, warn, debug};
use rustls_platform_verifier::BuilderVerifierExt;
pub use shvrpc::client::ClientConfig;
use shvrpc::client::LoginParams;
use shvrpc::framerw::{FrameReader, FrameWriter, ReceiveFrameError};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::util::login_from_url;
use shvrpc::{client, RpcMessage, RpcMessageMetaTags};
use futures::AsyncReadExt;
use futures_rustls::rustls::ClientConfig as TlsClientConfig;

fn build_tls_connector(url: &url::Url) -> shvrpc::Result<futures_rustls::TlsConnector> {
    let crypto_provider = Arc::new(futures_rustls::rustls::crypto::aws_lc_rs::default_provider());
    if let Some((_, ca_path)) = url.query_pairs().find(|(k, _)| k == "ca") {
        let ca_certs = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(ca_path.as_ref())?))
            .collect::<Result<Vec<_>,_>>()?;
        let mut root_store = futures_rustls::rustls::RootCertStore::empty();
        root_store.add_parsable_certificates(ca_certs);
        let client_config = TlsClientConfig::builder_with_provider(crypto_provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(futures_rustls::TlsConnector::from(Arc::new(client_config)))
    } else {
        let client_config = TlsClientConfig::builder_with_provider(crypto_provider)
            .with_safe_default_protocol_versions()?
            .with_platform_verifier()?
            .with_no_client_auth();
        Ok(futures_rustls::TlsConnector::from(Arc::new(client_config)))
    }
}

pub fn spawn_connection_task(config: &ClientConfig, conn_evt_tx: Sender<ConnectionEvent>) {
    crate::runtime::spawn_task(connection_task(config.clone(), conn_evt_tx)).detach();
}

pub(crate) trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncReadWrite for T {}

async fn connect(address: &str, tls: &Option<(Arc<TlsConnector>, ServerName<'static>)>)
-> shvrpc::Result<Box<dyn AsyncReadWrite + Send + Unpin>>
{
    #[cfg(feature = "tokio")]
    let stream = tokio_util::compat::TokioAsyncReadCompatExt::compat(
        tokio::net::TcpStream::connect(address).await?
    );

    #[cfg(feature = "smol")]
    let stream = smol::net::TcpStream::connect(address).await?;

    Ok(if let Some((tls_connector, server_name)) = tls {
        Box::new(tls_connector
            .connect(server_name.clone(), stream)
            .await?)
    } else {
        Box::new(stream)
    })
}

#[derive(Debug,Clone)]
pub enum ConnectionFailedKind {
    NetworkError,
    LoginFailed,
}

pub enum ConnectionEvent {
    ConnectionFailed(ConnectionFailedKind),
    Connected(Sender<ConnectionCommand>),
    RpcFrameReceived(RpcFrame),
    HeartbeatTimeout,
    Disconnected,
}

pub enum ConnectionCommand {
    SendMessage(RpcMessage),
}

enum ConnectionLoopResult {
    ConnectionClosed,
    ClientTerminated,
}

async fn connection_task(config: ClientConfig, conn_event_sender: Sender<ConnectionEvent>) {
    let tls = if config.url.scheme() == "ssl" {
        let tls_connector = Arc::new(build_tls_connector(&config.url)
            .unwrap_or_else(|err| panic!("Cannot initialize TLS: {err}"))
        );
        let server_name = futures_rustls::pki_types::ServerName::try_from(config.url.host_str().unwrap_or_default())
            .unwrap_or_else(|err| panic!("Invalid TLS server name `{host:?}`: {err}", host = config.url.host_str()))
            .to_owned();
        Some((tls_connector, server_name))
    } else {
        None
    };

    if let Some(reconnect_interval) = &config.reconnect_interval {
        info!("Reconnect interval set to: {reconnect_interval:?}");
        loop {
            // Check if the client loop has been terminated before trying to connect.
            // The client loop termination is then detected in the connection_loop based on
            // conn_event_receiver, but it happens only after a successful connection.
            if conn_event_sender.is_closed() {
                warn!("conn_event_sender is closed");
                break;
            }
            match Box::pin(connection_loop(&config, &tls, &conn_event_sender)).await {
                ConnectionLoopResult::ClientTerminated => break,
                ConnectionLoopResult::ConnectionClosed => {
                    info!("Connection closed, reconnecting after {}", reconnect_interval.human_format());
                    futures_time::task::sleep((*reconnect_interval).into()).await;
                }
            }
        }
    } else {
        Box::pin(connection_loop(&config, &tls, &conn_event_sender)).await;
    }
    // NOTE: The connection_task termination is detected in the client_task
    // by conn_event_sender drop that occurs here.
}

async fn connection_loop(
    config: &ClientConfig,
    tls: &Option<(Arc<TlsConnector>, ServerName<'static>)>,
    conn_event_sender: &Sender<ConnectionEvent>,
) -> ConnectionLoopResult {
    let (host, port) = (
        config.url.host_str().unwrap_or_default(),
        config.url.port().unwrap_or(3755),
    );
    let address = format!("{host}:{port}");

    // Establish a connection
    info!("Connecting to: {address}");
    let (mut frame_reader, mut frame_writer) = match connect(&address, tls).await {
        Ok(stream) =>{
            let (rd, wr) = stream.split();
            (shvrpc::streamrw::StreamFrameReader::new(futures::io::BufReader::new(rd)), shvrpc::streamrw::StreamFrameWriter::new(wr))
        }
        Err(err) => {
            warn!("Cannot connect to {address}: {err}");
            conn_event_sender
                .unbounded_send(ConnectionEvent::ConnectionFailed(ConnectionFailedKind::NetworkError))
                .unwrap_or_else(|e| debug!("ConnectionEvent::ConnectionFailed(NetworkError) send failed: {e}"));
            return ConnectionLoopResult::ConnectionClosed;
        }
    };
    info!("Connected OK");

    // login
    let (user, password) = login_from_url(&config.url);
    let heartbeat_interval = config.heartbeat_interval;
    // The read timeout can be related to the heartbeat interval given that the interval is
    // significantly larger than roundtrip time. The client has to receive at least a response
    // to the heartbeat within this interval.
    let read_timeout = heartbeat_interval * 2;
    info!("Heartbeat interval set to: {heartbeat_interval:?}");

    let login_params = LoginParams {
        user,
        password,
        mount_point: config.mount.clone().unwrap_or_default(),
        device_id: config.device_id.clone().unwrap_or_default(),
        heartbeat_interval,
        ..Default::default()
    };

    let client_id = match client::login(&mut frame_reader, &mut frame_writer, &login_params, false).await {
        Ok(id) => id,
        Err(err) => {
            warn!("Login failed: {err}");
            conn_event_sender
                .unbounded_send(ConnectionEvent::ConnectionFailed(ConnectionFailedKind::LoginFailed))
                .unwrap_or_else(|e| debug!("ConnectionEvent::ConnectionFailed(LoginFailed) send failed: {e}"));
            return ConnectionLoopResult::ConnectionClosed;
        }
    };
    info!("Login OK, client ID: {client_id}");

    let (writer_tx, mut writer_rx) = futures::channel::mpsc::unbounded();
    crate::runtime::spawn_task(async move {
        debug!("Writer task start");
        let res: shvrpc::Result<()> = {
            while let Some(frame) = writer_rx.next().await {
                frame_writer.send_message(frame)
                    .await
                    .inspect_err(|err| warn!("Send frame error: {err}"))?;
            }
            Ok(())
        };
        debug!("Writer task finish");
        res
    }).detach();

    let (conn_cmd_sender, conn_cmd_receiver) = futures::channel::mpsc::unbounded();

    conn_event_sender
        .unbounded_send(ConnectionEvent::Connected(conn_cmd_sender))
        .unwrap_or_else(|e| debug!("ConnectionEvent::Connected send failed: {e}"));

    async {
        let mut fut_heartbeat_timeout = futures_time::task::sleep(heartbeat_interval.into()).fuse();
        let mut conn_cmd_receiver = conn_cmd_receiver.fuse();
        let mut frame_stream = std::pin::pin!(futures::stream::unfold(frame_reader, async |mut reader| {
            use futures_time::future::FutureExt;
            let frame_res = reader
                .receive_frame()
                .timeout(futures_time::time::Duration::from(read_timeout))
                .await
                .map_err(|_| shvrpc::framerw::ReceiveFrameError::Timeout(None))
                .flatten();
            Some((frame_res, reader))
        }));

        loop {
            select! {
                _ = fut_heartbeat_timeout => {
                    // send heartbeat event
                    conn_event_sender.unbounded_send(ConnectionEvent::HeartbeatTimeout)
                        .unwrap_or_else(|e| debug!("ConnectionEvent::HeartbeatTimeout send failed: {e}"));
                }
                conn_cmd_result = conn_cmd_receiver.next() => {
                    match conn_cmd_result {
                        Some(connection_command) => {
                            match connection_command {
                                ConnectionCommand::SendMessage(message) => {
                                    // reset heartbeat timer
                                    fut_heartbeat_timeout = futures_time::task::sleep(heartbeat_interval.into()).fuse();
                                    if let Err(err) = writer_tx.unbounded_send(message) {
                                        warn!("Cannot send message to the writer task: {err}");
                                        conn_event_sender
                                            .unbounded_send(ConnectionEvent::Disconnected)
                                            .unwrap_or_else(|e| debug!("ConnectionEvent::Disconnected send failed: {e}"));
                                        return ConnectionLoopResult::ConnectionClosed;
                                    }
                                },
                            }
                        },
                        None => {
                            // The only instance of TX is gone, the client loop has terminated
                            warn!("Connection command channel closed, client loop has terminated");
                            return ConnectionLoopResult::ClientTerminated;
                        },
                    }
                }
                receive_frame_result = frame_stream.select_next_some() => {
                    match receive_frame_result {
                        Ok(frame) => {
                            conn_event_sender
                                .unbounded_send(ConnectionEvent::RpcFrameReceived(frame))
                                .unwrap_or_else(|e| debug!("ConnectionEvent::RpcFrameReceived send failed: {e}"));
                        }
                        Err(err) => {
                            warn!("Receive frame error: {err}");
                            let (meta, rpc_err) = match &err {
                                ReceiveFrameError::Timeout(Some(meta)) if meta.is_request() => {
                                    (meta, RpcError::new(RpcErrorCode::MethodCallTimeout, "Could not receive complete request within the time limit"))
                                }
                                ReceiveFrameError::Timeout(Some(meta)) if meta.is_response() => {
                                    (meta, RpcError::new(RpcErrorCode::MethodCallTimeout, "Could not receive complete response within the time limit"))
                                }
                                ReceiveFrameError::FrameTooLarge(reason, Some(meta)) => {
                                    (meta, RpcError::new(RpcErrorCode::MethodCallException, reason))
                                }
                                _ => {
                                    if matches!(err, ReceiveFrameError::Timeout(None)) {
                                        warn!("Connection timed out, no data received for {}",read_timeout.human_format());
                                    }
                                    conn_event_sender
                                        .unbounded_send(ConnectionEvent::Disconnected)
                                        .unwrap_or_else(|e| debug!("ConnectionEvent::Disconnected send failed: {e}"));
                                    return ConnectionLoopResult::ConnectionClosed;
                                }
                            };
                            if meta.is_response() {
                                // Forward the response as an error to the client
                                let mut msg = RpcMessage::from_meta(meta.clone());
                                msg.set_error(rpc_err);
                                if let Ok(frame) = msg.to_frame() {
                                    conn_event_sender
                                        .unbounded_send(ConnectionEvent::RpcFrameReceived(frame))
                                        .unwrap_or_else(|e| debug!("ConnectionEvent::RpcFrameReceived send failed: {e}"));
                                }
                            } else if meta.is_request() && let Ok(mut msg) = RpcMessage::prepare_response_from_meta(meta) {
                                // Send the error response to the caller
                                msg.set_error(rpc_err);
                                // reset heartbeat timer
                                fut_heartbeat_timeout = futures_time::task::sleep(heartbeat_interval.into()).fuse();
                                if let Err(err) = writer_tx.unbounded_send(msg) {
                                    warn!("Cannot send message to the writer task: {err}");
                                    conn_event_sender
                                        .unbounded_send(ConnectionEvent::Disconnected)
                                        .unwrap_or_else(|e| debug!("ConnectionEvent::Disconnected send failed: {e}"));
                                    return ConnectionLoopResult::ConnectionClosed;
                                }
                            }
                        }
                    }
                }
            }
        }
    }.await
}

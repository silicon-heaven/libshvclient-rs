pub use crate::client::Sender;
use duration_str::HumanFormat;
use futures::{select, FutureExt, StreamExt};
use log::*;
pub use shvrpc::client::ClientConfig;
use shvrpc::client::LoginParams;
use shvrpc::framerw::{FrameReader, FrameWriter};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::util::login_from_url;
use shvrpc::{client, RpcMessage};
#[cfg(any(feature = "async_std" ,feature = "smol"))]
use futures::AsyncReadExt;

pub fn spawn_connection_task(config: &ClientConfig, conn_evt_tx: Sender<ConnectionEvent>) {
    let task = connection_task(config.clone(), conn_evt_tx);
    #[cfg(feature = "tokio")]
    tokio::spawn(task);
    #[cfg(feature = "async_std")]
    async_std::task::spawn(task);
    #[cfg(feature = "smol")]
    smol::spawn(task).detach();
}

async fn connect(address: &str)
    -> shvrpc::Result<(
        Box<dyn futures::AsyncRead + Send + Unpin>,
        Box<dyn futures::AsyncWrite + Send + Unpin>
    )>
{
    #[cfg(feature = "tokio")]
    {
        use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
        let stream = tokio::net::TcpStream::connect(address).await?;
        let (reader, writer) = stream.into_split();
        let writer = writer.compat_write();
        let reader = tokio::io::BufReader::new(reader).compat();
        Ok((Box::new(reader), Box::new(writer)))
    }
    #[cfg(feature = "async_std")]
    {
        let stream = async_std::net::TcpStream::connect(address).await?;
        let (reader, writer) = stream.split();
        let reader = futures::io::BufReader::new(reader);
        Ok((Box::new(reader), Box::new(writer)))
    }
    #[cfg(feature = "smol")]
    {
        let stream = smol::net::TcpStream::connect(address).await?;
        let (reader, writer) = stream.split();
        let reader = futures::io::BufReader::new(reader);
        Ok((Box::new(reader), Box::new(writer)))
    }
}

#[derive(Debug,Clone)]
pub enum ConnectionFailedKind {
    NetworkError,
    LoginFailed,
}

pub(crate) enum ConnectionEvent {
    ConnectionFailed(ConnectionFailedKind),
    Connected(Sender<ConnectionCommand>),
    RpcFrameReceived(RpcFrame),
    HeartbeatTimeout,
    Disconnected,
}

pub(crate) enum ConnectionCommand {
    SendMessage(RpcMessage),
}

enum ConnectionLoopResult {
    ConnectionClosed,
    ClientTerminated,
}

async fn connection_task(config: ClientConfig, conn_event_sender: Sender<ConnectionEvent>) {
    async {
        if let Some(reconnect_interval) = &config.reconnect_interval {
            info!("Reconnect interval set to: {:?}", reconnect_interval);
            loop {
                // Check if the client loop has been terminated before trying to connect.
                // The client loop termination is then detected in the connection_loop based on
                // conn_event_receiver, but it happens only after a successful connection.
                if conn_event_sender.is_closed() {
                    warn!("conn_event_sender is closed");
                    break;
                }
                match connection_loop(&config, &conn_event_sender).await {
                    ConnectionLoopResult::ClientTerminated => break,
                    ConnectionLoopResult::ConnectionClosed => {
                        info!("Connection closed, reconnecting after {}", reconnect_interval.human_format());
                        futures_time::task::sleep((*reconnect_interval).into()).await;
                    }
                }
            }
        } else {
            connection_loop(&config, &conn_event_sender).await;
        }
    }
    .await;
    // NOTE: The connection_task termination is detected in the client_task
    // by conn_event_sender drop that occurs here.
}

async fn connection_loop(
    config: &ClientConfig,
    conn_event_sender: &Sender<ConnectionEvent>,
) -> ConnectionLoopResult {
    let (host, port) = (
        config.url.host_str().unwrap_or_default(),
        config.url.port().unwrap_or(3755),
    );
    let address = format!("{host}:{port}");

    // Establish a connection
    info!("Connecting to: {address}");
    let (mut frame_reader, mut frame_writer) = match connect(&address).await {
        Ok((rd, wr)) => (shvrpc::streamrw::StreamFrameReader::new(rd), shvrpc::streamrw::StreamFrameWriter::new(wr)),
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
    info!("Heartbeat interval set to: {:?}", heartbeat_interval);

    let login_params = LoginParams {
        user,
        password,
        mount_point: config.mount.clone().unwrap_or_default().to_owned(),
        device_id: config.device_id.clone().unwrap_or_default().to_owned(),
        heartbeat_interval,
        ..Default::default()
    };

    let client_id = match client::login(&mut frame_reader, &mut frame_writer, &login_params).await {
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
        let mut fut_read_timeout = futures_time::task::sleep(read_timeout.into()).fuse();
        let mut conn_cmd_receiver = conn_cmd_receiver.fuse();
        let mut fut_receive_frame = frame_reader.receive_frame().fuse();

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
                _ = fut_read_timeout => {
                    warn!("Connection timed out, no data received for {}", read_timeout.human_format());
                    conn_event_sender
                        .unbounded_send(ConnectionEvent::Disconnected)
                        .unwrap_or_else(|e| debug!("ConnectionEvent::Disconnected send failed: {e}"));
                    return ConnectionLoopResult::ConnectionClosed;
                }
                receive_frame_result = fut_receive_frame => {
                    match receive_frame_result {
                        Ok(frame) => {
                            fut_read_timeout = futures_time::task::sleep(read_timeout.into()).fuse();
                            conn_event_sender
                                .unbounded_send(ConnectionEvent::RpcFrameReceived(frame))
                                .unwrap_or_else(|e| debug!("ConnectionEvent::RpcFrameReceived send failed: {e}"));
                        }
                        Err(e) => {
                            warn!("Receive frame error: {e}");
                            conn_event_sender
                                .unbounded_send(ConnectionEvent::Disconnected)
                                .unwrap_or_else(|e| debug!("ConnectionEvent::Disconnected send failed: {e}"));
                            return ConnectionLoopResult::ConnectionClosed;
                        }
                    }
                    // The drop before the reassignment is needed because the future is holding
                    // &mut frame_reader until it is dropped, therefore it cannot be borrowed
                    // again on the rhs of the assignment.
                    drop(fut_receive_frame);
                    fut_receive_frame = frame_reader.receive_frame().fuse();
                }
            }
        }
    }.await
}

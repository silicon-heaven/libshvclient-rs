use std::sync::Arc;
use clap::Parser;
use futures::{select, FutureExt};
use futures_time::time::Duration;
use log::*;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{client::ClientConfig, util::parse_log_verbosity};
use shvrpc::{RpcMessage, RpcMessageMetaTags as _};
use shvclient::appnodes::{DotAppNode, DotDeviceNode};
use shvclient::clientnode::{err_unresolved_request, Method, METH_GET, METH_SET, PROPERTY_METHODS, SIG_CHNG};
use shvclient::{ClientCommandSender, ClientEvent, ClientEventsReceiver};
use simple_logger::SimpleLogger;
use smol::lock::RwLock;
use url::Url;

#[derive(Parser, Debug)]
//#[structopt(name = "device", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "SHV call")]
struct Opts {
    /// Config file path
    #[arg(long)]
    config: Option<String>,
    /// Create default config file if one specified by --config is not found
    #[arg(short, long)]
    create_default_config: bool,
    ///Url to connect to, example tcp://admin@localhost:3755?password=dj4j5HHb, localsocket:path/to/socket
    #[arg(short = 's', long)]
    url: Option<String>,
    #[arg(short = 'i', long)]
    device_id: Option<String>,
    /// Mount point on broker connected to, note that broker might not accept any path.
    #[arg(short, long)]
    mount: Option<String>,
    /// Device tries to reconnect to broker after this interval, if connection to broker is lost.
    /// Example values: 1s, 1h, etc.
    #[arg(short, long)]
    reconnect_interval: Option<String>,
    /// Client should ping broker with this interval. Broker will disconnect device, if ping is not received twice.
    /// Example values: 1s, 1h, etc.
    #[arg(long, default_value = "1m")]
    heartbeat_interval: String,
    /// Verbose mode (module, .)
    #[arg(short, long)]
    verbose: Option<String>,
}

fn init_logger(cli_opts: &Opts) {
    let mut logger = SimpleLogger::new();
    logger = logger.with_level(LevelFilter::Info);
    if let Some(module_names) = &cli_opts.verbose {
        for (module, level) in parse_log_verbosity(module_names, module_path!()) {
            if let Some(module) = module {
                logger = logger.with_module_level(module, level);
            } else {
                logger = logger.with_level(level);
            }
        }
    }
    logger.init().unwrap();
}

fn load_client_config(cli_opts: Opts) -> shvrpc::Result<ClientConfig> {
    let mut config = if let Some(config_file) = &cli_opts.config {
        ClientConfig::from_file_or_default(config_file, cli_opts.create_default_config)?
    } else {
        Default::default()
    };
    config.url = match &cli_opts.url {
        Some(url_str) => Url::parse(url_str)?,
        None => config.url,
    };
    config.device_id = cli_opts.device_id.or(config.device_id);
    config.mount = cli_opts.mount.or(config.mount);
    config.reconnect_interval = match cli_opts.reconnect_interval {
        Some(interval_str) => Some(duration_str::parse(interval_str)?),
        None => config.reconnect_interval,
    };
    config.heartbeat_interval = duration_str::parse(cli_opts.heartbeat_interval)?;
    Ok(config)
}


type AppState = Arc<RwLock<i32>>;

struct DelayNode {
    state: AppState,
}

shvclient::impl_static_node! {
    DelayNode(&self, request, client_cmd_tx) {
        "getDelayed" [IsGetter, Browse, "", ""] => {
            let mut resp = request.prepare_response().unwrap_or_default();
            let state = self.state.clone();
            smol::spawn(async move {
                let mut counter = state
                    .write_arc()
                    .await;
                let ret_val = {
                    *counter += 1;
                    *counter
                };
                drop(counter);
                futures_time::task::sleep(Duration::from_secs(3)).await;
                resp.set_result(ret_val);
                if let Err(e) = client_cmd_tx.send_message(resp) {
                    error!("delay_node_process_request: Cannot send response ({e})");
                }
            }).detach();
            None
        }
    }
}

async fn emit_chng_task(
    client_cmd_tx: ClientCommandSender,
    mut client_evt_rx: ClientEventsReceiver,
    app_state: AppState,
) -> shvrpc::Result<()> {
    info!("signal task started");

    let mut cnt = 0;
    let mut emit_signal = true;
    loop {
        select! {
            rx_event = client_evt_rx.recv_event().fuse() => match rx_event {
                Ok(ClientEvent::ConnectionFailed(_)) => {
                    warn!("Connection failed");
                }
                Ok(ClientEvent::Connected(_)) => {
                    emit_signal = true;
                    warn!("Device connected");
                },
                Ok(ClientEvent::Disconnected) => {
                    emit_signal = false;
                    warn!("Device disconnected");
                },
                Err(err) => {
                    error!("Device event error: {err}");
                    break;
                },
            },
            _ = futures_time::task::sleep(futures_time::time::Duration::from_secs(3)).fuse() => { }

        }
        if emit_signal {
            let sig = RpcMessage::new_signal("status/delayed", SIG_CHNG, Some(cnt.into()));
            client_cmd_tx.send_message(sig)?;
            info!("signal task emits a value: {cnt}");
            cnt += 1;
        }
        if cnt == 10 {
            client_cmd_tx.terminate_client();
        }
        let state = app_state.read().await;
        info!("state: {state}");
    }
    info!("signal task finished");
    Ok(())
}

fn main() -> shvrpc::Result<()> {
    let cli_opts = Opts::parse();
    init_logger(&cli_opts);

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    let client_config = load_client_config(cli_opts).expect("Invalid config");

    let counter = AppState::new(RwLock::new(-10));

    let app_tasks = {
        let counter = counter.clone();
        move |client_cmd_tx, client_evt_rx| {
            smol::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, counter)).detach();
        }
    };

    const SMOL_THREADS: &str = "SMOL_THREADS";
    if std::env::var(SMOL_THREADS).is_err()
        && let Ok(num_threads) = std::thread::available_parallelism() {
            // set_var called before any other threads and smol runtime
            unsafe { std::env::set_var(SMOL_THREADS, num_threads.to_string()); }
        }
    smol::block_on(async move {
        shvclient::Client::new()
            .app(DotAppNode::new("simple_device_smol"))
            .device(DotDeviceNode::new("simple_device", "0.1", Some("00000".into())))
            .mount_static("status/delayed", DelayNode { state: counter.clone() })
            .mount_dynamic("status/dyn", move |rq, _client_cmd_tx| {
                let counter = counter.clone();
                async move {
                    let shv_path = rq.shv_path().unwrap_or_default();
                    if shv_path.is_empty() {
                        return err_unresolved_request();
                    }

                    match Method::from_request(&rq) {
                        Method::Dir(dir) => dir.resolve(PROPERTY_METHODS),
                        Method::Ls(ls) => ls.resolve(PROPERTY_METHODS, async || {
                            Ok(vec![])
                        }),
                        Method::Other(m) => {
                            let method = m.method();
                            match method {
                                METH_GET => m.resolve(PROPERTY_METHODS, async move || {
                                    Ok(*counter.read().await)
                                }),
                                METH_SET => m.resolve_opt(PROPERTY_METHODS, async move || {
                                    let param: i32 = match rq.param().unwrap_or_default().try_into() {
                                        Ok(v) => v,
                                        Err(err) => return Some(Err(RpcError::new(RpcErrorCode::InvalidParam, err))),
                                    };
                                    *counter.write().await = param;
                                    Some(Ok(true))
                                }),
                                _ => err_unresolved_request(),
                            }
                        }
                    }
                }
            })
            .run_with_init(&client_config, app_tasks)
            .await
    })
}

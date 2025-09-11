use async_std::sync::RwLock;

use clap::Parser;
use futures::{select, FutureExt};
use futures_time::time::Duration;
use log::*;
use shvrpc::metamethod::{Flag, MetaMethod};
use shvrpc::{client::ClientConfig, util::parse_log_verbosity};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use shvclient::appnodes::{DotAppNode, DotDeviceNode};
use shvclient::clientnode::{ClientNode, SIG_CHNG};
use shvclient::RequestHandler;
use shvclient::{ClientCommandSender, ClientEvent, ClientEventsReceiver, Route, AppState};
use simple_logger::SimpleLogger;
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

const METH_GET_DELAYED: &str = "getDelayed";

const DELAY_METHODS: &[MetaMethod] = &[MetaMethod {
    name: METH_GET_DELAYED,
    flags: Flag::IsGetter as u32,
    access: shvrpc::metamethod::AccessLevel::Browse,
    param: "",
    result: "",
    signals: &[],
    description: "",
}];

type State = RwLock<i32>;

async fn delay_node_process_request(
    request: RpcMessage,
    client_cmd_tx: ClientCommandSender<State>,
    state: Option<AppState<State>>,
) {
    if request.shv_path().unwrap_or_default().is_empty() {
        assert_eq!(request.method(), Some(METH_GET_DELAYED));
        let mut resp = request.prepare_response().unwrap_or_default();
        async_std::task::spawn(async move {
            let mut counter = state
                .expect("Missing state for delay node")
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
        });
    }
}


async fn emit_chng_task(
    client_cmd_tx: ClientCommandSender<State>,
    mut client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
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
        if cnt == 5 {
            client_cmd_tx.terminate_client();
        }
        let state = app_state.read().await;
        info!("state: {state}");
    }
    info!("signal task finished");
    Ok(())
}

#[async_std::main]
pub(crate) async fn main() -> shvrpc::Result<()> {
    let cli_opts = Opts::parse();
    init_logger(&cli_opts);

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    let client_config = load_client_config(cli_opts).expect("Invalid config");

    let counter = AppState::new(RwLock::new(-10));
    let cnt = counter.clone();

    let app_tasks = move |client_cmd_tx, client_evt_rx| {
        async_std::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, counter));
    };

    shvclient::Client::new()
        .app(DotAppNode::new("simple_device_async_std"))
        .device(DotDeviceNode::new("simple_device", "0.1", Some("00000".into())))
        .mount("status/delayed", ClientNode::fixed(
                DELAY_METHODS,
                [Route::new(
                    [METH_GET_DELAYED],
                    RequestHandler::stateful(delay_node_process_request),
                )]
        ))
        .with_app_state(cnt)
        .run_with_init(&client_config, app_tasks)
        // .run(&client_config)
        .await
}

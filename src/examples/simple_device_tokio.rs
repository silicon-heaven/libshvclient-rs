use shvclient::appnodes::DotAppNode;
use shvclient::client::MetaMethods;
use tokio::sync::RwLock;

use clap::Parser;
use futures::{select, FutureExt, StreamExt};
use log::*;
use shvrpc::{client::ClientConfig, util::parse_log_verbosity};
use shvrpc::RpcMessage;
use shvclient::{MethodsGetter, RequestHandler};
use shvclient::clientnode::{ClientNode, PROPERTY_METHODS, SIG_CHNG};
use shvclient::{ClientCommandSender, ClientEvent, ClientEventsReceiver, AppState};
use simple_logger::SimpleLogger;
use shvproto::{RpcValue, TryFromRpcValue};
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

type State = RwLock<i32>;

async fn emit_chng_task(
    client_cmd_tx: ClientCommandSender<State>,
    client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
) -> shvrpc::Result<()> {
    info!("signal task started");
    let mut client_evt_rx = client_evt_rx.fuse();
    let mut cnt = 0;
    let mut emit_signal = true;
    loop {
        select! {
            rx_event = client_evt_rx.next() => match rx_event {
                Some(ClientEvent::ConnectionFailed(_)) => {
                    info!("Connection failed");
                }
                Some(ClientEvent::Connected(_)) => {
                    emit_signal = true;
                    info!("Device connected");

                    client_cmd_tx.mount_node("onfly", shvclient::fixed_node! {
                        device_handler<State>(request, _tx ) {
                            "echo" [IsGetter, Browse, "", ""] (param: RpcValue) => {
                                println!("echo: {}", param);
                                Some(Ok(param))
                            }
                        }
                    });
                },
                Some(ClientEvent::Disconnected) => {
                    emit_signal = false;
                    info!("Device disconnected");
                },
                None => break,
            },
            _ = futures_time::task::sleep(futures_time::time::Duration::from_secs(3)).fuse() => { }

        }
        if emit_signal {
            let sig = RpcMessage::new_signal("status/delayed", SIG_CHNG, Some(cnt.into()));
            client_cmd_tx.send_message(sig)?;
            info!("signal task emits a value: {cnt}");
            cnt += 1;
        }
        let state = app_state.read().await;
        info!("state: {state}");
        if cnt == 10 {
            client_cmd_tx.unmount_node("onfly");
        }
        if cnt == 20 {
            client_cmd_tx.terminate_client();
        }
    }
    info!("signal task finished");
    Ok(())
}


#[derive(Default, Clone, TryFromRpcValue)]
struct CustomParam {
    data: Vec<String>,
    data2: Vec<RpcValue>,
}

#[tokio::main]
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
        tokio::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, counter));
    };

    async fn dyn_methods_getter(_path: String, _: Option<AppState<State>>) -> Option<MetaMethods> {
        Some(MetaMethods::from(&PROPERTY_METHODS))
    }
    async fn dyn_handler(_request: RpcMessage, _client_cmd_tx: ClientCommandSender<State>) {
    }

    let stateless_node = shvclient::fixed_node!{
        device_handler<State>(request, _tx ) {
            "something" [IsGetter, Browse, "", ""] (param: i32) => {
                println!("param: {}", param);
                Some(Ok(RpcValue::from("name result")))
            }
            "setString" [IsSetter, Write, "String", ""] (param: Vec<String>) => {
                for s in &param {
                    if s.contains("foo") {
                        return Some(Err(shvrpc::rpcmessage::RpcError::new(
                                    shvrpc::rpcmessage::RpcErrorCode::InvalidParam,
                                    "err".to_string()))
                        );
                    }
                }
                println!("param: {:?}", param);
                Some(Ok(RpcValue::from("name result")))
            }
            "setCustomParam" [IsSetter, Write, "List", ""] (param: Vec<CustomParam>) => {
                for item in &param {
                    for i in &item.data {
                        println!("param data: {}", i);
                        if i == "foo" {
                            return Some(Ok(().into()));
                        }
                    }
                }
                Some(Ok(param.into()))
            }
            "setVecString" [IsSetter, Write, "List", ""] (param: Vec<String>) => {
                println!("param data: {:?}", &param);
                Some(Ok(().into()))
            }
            "42" [IsGetter, Browse, "", ""] => {
                Some(Ok(RpcValue::from(42)))
            }
        }
    };

    let delay_node = shvclient::fixed_node!(
        delay_handler<State>(request, client_cmd_tx, app_state) {
            "getDelayed" [None, Browse, "", ""] { ("delayedmod", None) } => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let mut counter = app_state
                        .write()
                        .await;
                    let ret_val = {
                        *counter += 1;
                        *counter
                    };
                    drop(counter);
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    resp.set_result(ret_val);
                    if let Err(e) = client_cmd_tx.send_message(resp) {
                        error!("delay_node_process_request: Cannot send response ({e})");
                    }
                });

                // The response is sent in the task above, so we need
                // to tell the library to not send any response.
                None

                // Otherwise, return either RpcValue or RpcError
                // Some(Ok(shvrpc::RpcValue::from(true)))
            }
        }
    );

    let root_node = shvclient::fixed_node!(
        root_handler<State>(request, _client_cmd_tx) {
            "info" [None, Read, "", ""] => {
                Some(Ok("Simple device tokio".into()))
            }
        }
    );

    shvclient::Client::new(DotAppNode::new("simple_device_tokio"))
        .mount("", root_node)
        .mount("stateless", stateless_node)
        .mount("status/delayed", delay_node)
        .mount("status/dyn", ClientNode::dynamic(
                MethodsGetter::new(dyn_methods_getter),
                RequestHandler::stateless(dyn_handler)))
        .with_app_state(cnt)
        .run_with_init(&client_config, app_tasks)
        // .run(&client_config)
        .await
}

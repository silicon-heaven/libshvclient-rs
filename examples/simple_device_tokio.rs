use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use shvclient::appnodes::DotAppNode;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use tokio::sync::RwLock;

use clap::Parser;
use futures::{select, FutureExt, StreamExt};
use log::*;
use shvrpc::{client::ClientConfig, util::parse_log_verbosity};
use shvrpc::{RpcMessage, RpcMessageMetaTags as _};
use shvclient::clientnode::{err_unresolved_request, Method, RequestHandlerResult, METH_GET, METH_SET, PROPERTY_METHODS, SIG_CHNG};
use shvclient::{ClientCommandSender, ClientEvent, ClientEventsReceiver};
use simple_logger::SimpleLogger;
use shvproto::{RpcValue, FromRpcValue, ToRpcValue};
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
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: Arc<State>,
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
        if cnt == 20 {
            client_cmd_tx.terminate_client();
        }
    }
    info!("signal task finished");
    Ok(())
}


#[derive(Default, Clone, FromRpcValue, ToRpcValue)]
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

    let counter = Arc::new(RwLock::new(-10));

    let app_tasks = {
        let counter = counter.clone();
        move |client_cmd_tx, client_evt_rx| {
            tokio::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, counter));
        }
    };

    async fn dyn_request_handler(rq: RpcMessage, _client_cmd_tx: ClientCommandSender, counter: Arc<RwLock<i32>>) -> RequestHandlerResult {
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

    let params_node = shvclient::static_node!{
        ParamsNode(request, _tx ) {
            "something" [IsGetter, Browse, "", ""] (param: i32) => {
                println!("param: {param}");
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
                println!("param: {param:?}");
                Some(Ok(RpcValue::from("name result")))
            }
            "setCustomParam" [IsSetter, Write, "List", ""] (param: Vec<CustomParam>) => {
                for item in &param {
                    for i in &item.data {
                        println!("param data: {i}");
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


    struct DelayNode {
        app_state: Arc<RwLock<i32>>,
    }
    shvclient::impl_static_node!(
        DelayNode(&self, request, client_cmd_tx) {
            "getDelayed" [None, Browse, "", ""] { ("delayedmod", None) } => {
                let mut resp = request.prepare_response().unwrap_or_default();
                let app_state = self.app_state.clone();
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

    let root_node = shvclient::static_node!(
        RootNode(request, _client_cmd_tx) {
            "info" [None, Read, "", ""] => {
                Some(Ok("Simple device tokio".into()))
            }
        }
    );


    struct CustomNode {
        foo: AtomicI32,
    }

    shvclient::impl_static_node!(
        CustomNode(&self, request, _tx) {
            "secret" [IsGetter, Browse, "", ""] (param: i32) => {
                println!("param: {param}, {method:?}", method = request.method());
                Some(Ok(self.foo.fetch_add(1, std::sync::atomic::Ordering::SeqCst).into()))
            }
        }
    );

    let static_node = shvclient::static_node! {
        DeviceNode(request, _tx) {
            "something" [IsGetter, Browse, "", ""] (param: i32) => {
                println!("param: {param}, {method:?}", method = request.method());
                Some(Ok(RpcValue::from("name result")))
            }
            "get" [IsGetter, Browse, "", ""] => {
                Some(Ok(RpcValue::from(42)))
            }
            "setName" [IsGetter|IsSetter, Browse, "", ""] { ("nameChanged", Some("String")) } (param: String) => {
                println!("updated to: {param}");
                Some(Ok(RpcValue::from(true)))
            }
        }
    };

    shvclient::Client::new()
        .app(DotAppNode::new("simple_device_tokio"))
        .mount_static("", root_node)
        .mount_static("static", static_node)
        .mount_static("static/custom", CustomNode { foo: 1234.into() })
        .mount_static("status/delayed", DelayNode { app_state: counter.clone() })
        .mount_static("status/params", params_node)
        .mount_dynamic("status/dyn", move |rq, client_cmd_tx| {
            let counter = counter.clone();
            async move {
                dyn_request_handler(rq, client_cmd_tx, counter).await
            }
        })
        .run_with_init(&client_config, app_tasks)
        .await
}

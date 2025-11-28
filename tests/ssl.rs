use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures_time::future::FutureExt;
use log::{error, info, warn};
use rcgen::{BasicConstraints, CertificateParams, DnType, DnValue, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType, PKCS_ECDSA_P256_SHA256};
use shvbroker::brokerimpl::{run_broker, BrokerImpl};
use shvbroker::config::{BrokerConfig, Listen};
use shvclient::clientapi::{RpcCallDirExists, RpcCallDirList};
use shvclient::{ClientCommandSender, ClientEvent, ClientEventsReceiver};
use shvrpc::client::ClientConfig;
use tempfile::TempDir;
use url::Url;

const BROKER_ADDRESS: &str = "localhost:37568";

async fn start_broker(broker_config: BrokerConfig, broker_address: &str) {
    let access_config = broker_config.access.clone();
    let broker_config = Arc::new(broker_config);
    shvclient::runtime::spawn_task(async {
        run_broker(BrokerImpl::new(broker_config, access_config, None))
            .await
            .expect("broker accept_loop failed")
    }).detach();
    // Wait for the broker
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(5) {
        if smol::net::TcpStream::connect(broker_address).await.is_ok() {
            return;
        }
        smol::Timer::after(std::time::Duration::from_millis(200)).await;
    }
    panic!("Could not start the broker");
}

async fn start_client(ca_crt_path: impl Into<String>) -> Option<(ClientCommandSender, ClientEventsReceiver)> {
    let (tx, rx) = futures::channel::oneshot::channel();
    let ca_crt_path = ca_crt_path.into();
    shvclient::runtime::spawn_task(async move {
        let client_config = ClientConfig {
            url: Url::parse(&format!("ssl://admin:admin@{BROKER_ADDRESS}?ca={ca_crt_path}")).unwrap(),
            device_id: None,
            mount: None,
            heartbeat_interval: Duration::from_secs(60),
            reconnect_interval: None,
        };
        shvclient::client::Client::new_plain()
            .run_with_init(&client_config, |commands_tx, events_rx| {
                tx.send((commands_tx, events_rx))
                    .unwrap_or_else(|(commands_tx, _)| {
                        warn!("Client channels dropped before handed to the caller. Terminating the client");
                        commands_tx.terminate_client();
                    })
            })
            .await
            .unwrap_or_else(|e| error!("Client finished with error: {e}"));
        }
    ).detach();
    rx.await.ok()
}

static TEST_TEMP_DIR: LazyLock<TempDir> = LazyLock::new(|| {
    TempDir::new().expect("failed to create global test tempdir")
});

/// Returns: (root_ca_path, server_cert_path, server_key_path)
fn generate_test_cert_files() -> anyhow::Result<(PathBuf, PathBuf, PathBuf)> {
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name.push(DnType::CommonName, "Local Test Root");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let server_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

    let mut server_params = CertificateParams::new(vec!["localhost".to_string()])?;
    server_params.distinguished_name.push(
        DnType::CommonName,
        DnValue::Utf8String("localhost".to_string()),
    );
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into()?),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    ];

    // Issue server cert signed by our CA (using the server public key and the issuer)
    let server_cert = server_params.signed_by(&server_key, &issuer)?;

    let ca_crt_path = TEST_TEMP_DIR.path().join("ca.crt");
    let server_crt_path = TEST_TEMP_DIR.path().join("server.crt");
    let server_key_path = TEST_TEMP_DIR.path().join("server.key");

    std::fs::write(&ca_crt_path, ca_cert.pem())?;
    std::fs::write(&server_crt_path, server_cert.pem())?;
    std::fs::write(&server_key_path, server_key.serialize_pem())?;

    Ok((ca_crt_path, server_crt_path, server_key_path))
}

fn create_broker_config(cert: &str, key: &str) -> BrokerConfig {
    BrokerConfig {
        listen: vec![
            Listen { url: Url::parse(&format!("ssl://{BROKER_ADDRESS}?cert={cert}&key={key}")).unwrap() },
        ],
        ..Default::default()
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    #[cfg(feature = "tokio")]
    { tokio::runtime::Runtime::new().unwrap().block_on(future) }

    #[cfg(feature = "async_std")]
    { async_std::task::block_on(future) }

    #[cfg(feature = "smol")]
    { smol::block_on(future) }
}

#[test]
fn ssl() {
    block_on(async {
        simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Debug)
            .init()
            .unwrap();

        let (ca_crt_path, server_crt_path, server_key_path) = generate_test_cert_files()
            .expect("Cannot generate test certificates");
        let broker_config = create_broker_config(server_crt_path.to_str().unwrap(), server_key_path.to_str().unwrap());
        start_broker(broker_config, BROKER_ADDRESS).await;

        let (client_cmd, mut client_events) = start_client(ca_crt_path.to_str().unwrap())
            .await
            .expect("Client start");

        match client_events
            .wait_for_event()
            .timeout(futures_time::time::Duration::from_secs(5))
            .await {
            Ok(Ok(ClientEvent::Connected(..))) => { },
            Ok(_) => panic!("Client connection to broker error"),
            Err(_) => panic!("Client connection to broker timed out"),
        };

        let res = RpcCallDirList::new(".app")
            .timeout(Duration::from_secs(3))
            .exec_full(&client_cmd)
            .await;
        info!(".app:dir:\n{res:?}");
        assert!(!res.unwrap().is_empty());

        let res = RpcCallDirExists::new(".broker/currentClient", "subscriptions")
            .timeout(Duration::from_secs(3))
            .exec(&client_cmd)
            .await;
        assert!(res.unwrap());
    });
}

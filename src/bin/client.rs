use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, ORIGIN, REFERER, USER_AGENT};
use serde::Deserialize;
use serde_json::{json, Value};
use tungstenite::client::IntoClientRequest;
use tungstenite::{connect, Message};
use uuid::Uuid;

const USER_AGENT_VALUE: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0";
const BUF_SIZE: usize = 65_535;

#[derive(Parser, Debug)]
#[command(name = "client")]
struct Args {
    /// Yandex Telemost link: https://telemost.yandex.ru/j/<ID>
    #[arg(long = "yandex-link")]
    yandex_link: String,

    /// Local UDP listener for Xray/V2Ray
    #[arg(long = "listen-host", default_value = "127.0.0.1")]
    listen_host: String,

    /// Local UDP listener port
    #[arg(long = "listen-port", default_value_t = 10_000)]
    listen_port: u16,

    /// Upstream target IP for UDP forwarding (QUIC-safe)
    #[arg(long = "target-ip", default_value = "217.28.222.148")]
    target_ip: String,

    /// Upstream target UDP port
    #[arg(long = "target-port", default_value_t = 443)]
    target_port: u16,
}

#[derive(Debug, Deserialize)]
struct ConferenceResponse {
    room_id: String,
    peer_id: String,
    credentials: String,
    client_configuration: ClientConfiguration,
}

#[derive(Debug, Deserialize)]
struct ClientConfiguration {
    media_server_url: String,
}

fn extract_telemost_id(link: &str) -> String {
    let tail = link.split("/j/").last().unwrap_or(link);
    let mut id = tail.to_owned();
    if let Some(idx) = id.find(['/', '?', '#']) {
        id.truncate(idx);
    }
    id
}

fn get_yandex_turn_creds(conference_link_id: &str) -> Result<(String, String, String)> {
    let endpoint = format!(
        "https://cloud-api.yandex.ru/telemost_front/v2/telemost/conferences/https%3A%2F%2Ftelemost.yandex.ru%2Fj%2F{conference_link_id}/connection?next_gen_media_platform_allowed=false"
    );

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        REFERER,
        HeaderValue::from_static("https://telemost.yandex.ru/"),
    );
    headers.insert(
        ORIGIN,
        HeaderValue::from_static("https://telemost.yandex.ru"),
    );
    headers.insert(
        "Client-Instance-Id",
        HeaderValue::from_str(&Uuid::new_v4().to_string())
            .context("invalid Client-Instance-Id header")?,
    );

    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(100)
        .build()
        .context("failed to build HTTP client")?;

    let response = client
        .get(&endpoint)
        .headers(headers)
        .send()
        .context("failed to request conference data")?
        .error_for_status()
        .context("conference API returned error status")?
        .json::<ConferenceResponse>()
        .context("failed to decode conference response")?;

    let hello_request = json!({
        "uid": Uuid::new_v4().to_string(),
        "hello": {
            "participantMeta": {
                "name": "Гость",
                "role": "SPEAKER",
                "description": "",
                "sendAudio": false,
                "sendVideo": false
            },
            "participantAttributes": {
                "name": "Гость",
                "role": "SPEAKER",
                "description": ""
            },
            "sendAudio": false,
            "sendVideo": false,
            "sendSharing": false,
            "participantId": response.peer_id,
            "roomId": response.room_id,
            "serviceName": "telemost",
            "credentials": response.credentials,
            "sdkInfo": {
                "implementation": "browser",
                "version": "5.15.0",
                "userAgent": USER_AGENT_VALUE,
                "hwConcurrency": 4
            },
            "sdkInitializationId": Uuid::new_v4().to_string(),
            "disablePublisher": false,
            "disableSubscriber": false,
            "disableSubscriberAudio": false,
            "capabilitiesOffer": {
                "offerAnswerMode": ["SEPARATE"]
            }
        }
    });

    // WebSocket handshake with explicit headers (matching Python approach).
    let mut request = response
        .client_configuration
        .media_server_url
        .as_str()
        .into_client_request()
        .context("invalid WebSocket URL")?;
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_static("https://telemost.yandex.ru"),
    );
    request
        .headers_mut()
        .insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));

    let (mut ws, _) = connect(request).context("WebSocket connect failed")?;

    ws.send(Message::Text(hello_request.to_string().into()))
        .context("failed to send HELLO request")?;

    loop {
        let msg = ws.read().context("failed to read WebSocket message")?;
        let text = match msg {
            Message::Text(s) => s,
            _ => continue,
        };

        let data: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let Some(ice_servers) = data
            .get("serverHello")
            .and_then(|v| v.get("rtcConfiguration"))
            .and_then(|v| v.get("iceServers"))
            .and_then(|v| v.as_array())
        else {
            continue;
        };

        for server in ice_servers {
            let username = server
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let credential = server
                .get("credential")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();

            let Some(urls) = server.get("urls").and_then(|v| v.as_array()) else {
                continue;
            };

            for url in urls {
                let Some(url_value) = url.as_str() else {
                    continue;
                };
                if !url_value.starts_with("turn:") || url_value.contains("transport=tcp") {
                    continue;
                }

                let turn_address = url_value
                    .split('?')
                    .next()
                    .unwrap_or(url_value)
                    .trim_start_matches("turn:")
                    .trim_start_matches("turns:")
                    .to_owned();

                // close immediately after receiving needed creds (same behavior as Python callback)
                let _ = ws.close(None);
                return Ok((username, credential, turn_address));
            }
        }
    }
}

fn disable_udp_connreset_on_windows(_sock: &UdpSocket) {
    // TODO: add a tiny optional winapi shim for SIO_UDP_CONNRESET if needed.
}

fn run_udp_forwarder(listen_addr: SocketAddr, target_addr: SocketAddr) -> Result<()> {
    let local_sock = UdpSocket::bind(listen_addr)
        .with_context(|| format!("failed to bind local socket on {listen_addr}"))?;
    local_sock
        .set_nonblocking(false)
        .context("failed to set blocking mode for local socket")?;
    local_sock
        .set_read_timeout(Some(Duration::from_millis(500)))
        .context("failed to set local socket timeout")?;
    disable_udp_connreset_on_windows(&local_sock);

    let remote_sock = UdpSocket::bind("0.0.0.0:0").context("failed to bind upstream socket")?;
    remote_sock
        .set_nonblocking(false)
        .context("failed to set blocking mode for upstream socket")?;
    remote_sock
        .set_read_timeout(Some(Duration::from_millis(500)))
        .context("failed to set upstream socket timeout")?;
    disable_udp_connreset_on_windows(&remote_sock);

    remote_sock
        .connect(target_addr)
        .with_context(|| format!("failed to connect upstream UDP socket to {target_addr}"))?;

    eprintln!("Forwarder listening on {listen_addr}");
    eprintln!(
        "Forwarding UDP to {} via local upstream socket {}",
        target_addr,
        remote_sock
            .local_addr()
            .context("upstream local addr failed")?
    );

    let running = Arc::new(AtomicBool::new(true));
    let signal_flag = Arc::clone(&running);
    ctrlc::set_handler(move || {
        signal_flag.store(false, Ordering::SeqCst);
    })
    .context("failed to set Ctrl+C handler")?;

    let (client_tx, client_rx) = mpsc::channel::<SocketAddr>();

    let c2s_local = local_sock
        .try_clone()
        .context("failed to clone local socket")?;
    let c2s_remote = remote_sock
        .try_clone()
        .context("failed to clone remote socket")?;
    let run_c2s = Arc::clone(&running);

    let t1 = thread::spawn(move || {
        let mut buf = [0u8; BUF_SIZE];
        while run_c2s.load(Ordering::SeqCst) {
            match c2s_local.recv_from(&mut buf) {
                Ok((n, client_addr)) => {
                    let _ = client_tx.send(client_addr);
                    if let Err(err) = c2s_remote.send(&buf[..n]) {
                        eprintln!("c2s send error: {err}");
                    }
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::TimedOut
                        || err.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(err) => eprintln!("c2s recv error: {err}"),
            }
        }
    });

    let s2c_local = local_sock
        .try_clone()
        .context("failed to clone local socket")?;
    let run_s2c = Arc::clone(&running);

    let t2 = thread::spawn(move || {
        let mut buf = [0u8; BUF_SIZE];
        let mut client_addr: Option<SocketAddr> = None;

        while run_s2c.load(Ordering::SeqCst) {
            while let Ok(addr) = client_rx.try_recv() {
                client_addr = Some(addr);
            }

            match remote_sock.recv(&mut buf) {
                Ok(n) => {
                    if let Some(addr) = client_addr {
                        if let Err(err) = s2c_local.send_to(&buf[..n], addr) {
                            eprintln!("s2c send error: {err}");
                        }
                    }
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::TimedOut
                        || err.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(err) => eprintln!("s2c recv error: {err}"),
            }
        }
    });

    let _ = t1.join();
    let _ = t2.join();
    eprintln!("Forwarder stopped");
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let conference_id = extract_telemost_id(&args.yandex_link);
    if conference_id.is_empty() {
        return Err(anyhow!("failed to parse conference ID from --yandex-link"));
    }

    let (turn_user, turn_cred, turn_addr) = get_yandex_turn_creds(&conference_id)?;
    eprintln!("Telemost TURN server: {turn_addr}");
    eprintln!("Telemost TURN username: {turn_user}");
    eprintln!("Telemost TURN password length: {}", turn_cred.len());

    let listen_addr: SocketAddr = format!("{}:{}", args.listen_host, args.listen_port)
        .parse()
        .context("invalid listen host/port")?;
    let target_addr: SocketAddr = format!("{}:{}", args.target_ip, args.target_port)
        .parse()
        .context("invalid target ip/port")?;

    run_udp_forwarder(listen_addr, target_addr)
}

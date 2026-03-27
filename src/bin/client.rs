use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, ORIGIN, REFERER, USER_AGENT};
use serde::Deserialize;
use serde_json::{json, Value};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tungstenite::client::IntoClientRequest;
use tungstenite::{connect, Message};
use uuid::Uuid;

const USER_AGENT_VALUE: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0";
const BUF_SIZE: usize = 65_535;

#[derive(Parser, Debug)]
#[command(name = "client")]
struct Args {
    #[arg(long = "yandex-link")]
    yandex_link: Option<String>,

    #[arg(long = "listen-host", default_value = "127.0.0.1")]
    listen_host: String,

    #[arg(long = "listen-port", default_value_t = 10_000)]
    listen_port: u16,

    #[arg(long = "target-ip", default_value = "217.28.222.148")]
    target_ip: String,

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
            "participantMeta": {"name": "Гость", "role": "SPEAKER", "description": "", "sendAudio": false, "sendVideo": false},
            "participantAttributes": {"name": "Гость", "role": "SPEAKER", "description": ""},
            "sendAudio": false,
            "sendVideo": false,
            "sendSharing": false,
            "participantId": response.peer_id,
            "roomId": response.room_id,
            "serviceName": "telemost",
            "credentials": response.credentials,
            "sdkInfo": {"implementation": "browser", "version": "5.15.0", "userAgent": USER_AGENT_VALUE, "hwConcurrency": 4},
            "sdkInitializationId": Uuid::new_v4().to_string(),
            "disablePublisher": false,
            "disableSubscriber": false,
            "disableSubscriberAudio": false,
            "capabilitiesOffer": {"offerAnswerMode": ["SEPARATE"]}
        }
    });

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
                if !(url_value.starts_with("turn:") || url_value.starts_with("turns:"))
                    || url_value.contains("transport=tcp")
                {
                    continue;
                }

                let turn_address = url_value
                    .split('?')
                    .next()
                    .unwrap_or(url_value)
                    .trim_start_matches("turn:")
                    .trim_start_matches("turns:")
                    .to_owned();

                let _ = ws.close(None);
                return Ok((username, credential, turn_address));
            }
        }
    }
}

fn make_udp_socket(bind: SocketAddr, reuse_addr: bool) -> Result<Socket> {
    let domain = if bind.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).context("socket create")?;
    sock.set_reuse_address(reuse_addr)
        .context("set_reuse_address")?;
    sock.bind(&bind.into())
        .with_context(|| format!("bind failed: {bind}"))?;
    disable_udp_connreset_on_windows(&sock);
    sock.set_nonblocking(true).context("set_nonblocking")?;
    Ok(sock)
}

#[cfg(windows)]
fn disable_udp_connreset_on_windows(sock: &Socket) {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{WSAIoctl, SOCKET_ERROR};

    const SIO_UDP_CONNRESET: u32 = 0x9800_000C;

    let mut bytes_returned: u32 = 0;
    let mut in_buffer: u32 = 0;

    // SAFETY: documented WinSock call for disabling UDP connreset behavior.
    let result = unsafe {
        WSAIoctl(
            sock.as_raw_socket() as usize,
            SIO_UDP_CONNRESET,
            &mut in_buffer as *mut _ as *mut _,
            std::mem::size_of::<u32>() as u32,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
            None,
        )
    };

    if result == SOCKET_ERROR {
        eprintln!("warning: could not disable SIO_UDP_CONNRESET");
    }
}

#[cfg(not(windows))]
fn disable_udp_connreset_on_windows(_sock: &Socket) {}

async fn run_udp_forwarder(listen_addr: SocketAddr, target_addr: SocketAddr) -> Result<()> {
    let local_std = make_udp_socket(listen_addr, true)?;

    let remote_bind = if target_addr.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
    };
    let remote_std = make_udp_socket(remote_bind, true)?;
    remote_std
        .connect(&target_addr.into())
        .with_context(|| format!("connect failed: {target_addr}"))?;

    let local_sock = UdpSocket::from_std(local_std.into()).context("tokio from_std local")?;
    let remote_sock = UdpSocket::from_std(remote_std.into()).context("tokio from_std remote")?;

    eprintln!("Forwarder listening on {listen_addr}");
    eprintln!(
        "Forwarding UDP to {} via local upstream socket {}",
        target_addr,
        remote_sock.local_addr().context("remote local_addr")?
    );

    let client_addr = Arc::new(Mutex::new(None::<SocketAddr>));

    let local_c2s = Arc::new(local_sock);
    let remote_c2s = Arc::new(remote_sock);

    let client_addr_c2s = Arc::clone(&client_addr);
    let local_c2s_task = Arc::clone(&local_c2s);
    let remote_c2s_task = Arc::clone(&remote_c2s);
    let c2s = tokio::spawn(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            match local_c2s_task.recv_from(&mut buf).await {
                Ok((n, addr)) => {
                    *client_addr_c2s.lock().await = Some(addr);
                    if let Err(err) = remote_c2s_task.send(&buf[..n]).await {
                        eprintln!("c2s error: {err}");
                    }
                }
                Err(err) => eprintln!("c2s recv error: {err}"),
            }
        }
    });

    let client_addr_s2c = Arc::clone(&client_addr);
    let local_s2c = Arc::clone(&local_c2s);
    let remote_s2c = Arc::clone(&remote_c2s);
    let s2c = tokio::spawn(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            match remote_s2c.recv(&mut buf).await {
                Ok(n) => {
                    if let Some(addr) = *client_addr_s2c.lock().await {
                        if let Err(err) = local_s2c.send_to(&buf[..n], addr).await {
                            eprintln!("s2c error: {err}");
                        }
                    }
                }
                Err(err) => eprintln!("s2c recv error: {err}"),
            }
        }
    });

    tokio::signal::ctrl_c()
        .await
        .context("ctrl_c wait failed")?;
    eprintln!("Shutdown requested");
    c2s.abort();
    s2c.abort();
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(link) = args.yandex_link {
        let conference_id = extract_telemost_id(&link);
        if conference_id.is_empty() {
            return Err(anyhow!("failed to parse conference ID from --yandex-link"));
        }

        let id = conference_id.clone();
        tokio::task::spawn_blocking(move || get_yandex_turn_creds(&id)).await??;
    }

    let listen_addr: SocketAddr = format!("{}:{}", args.listen_host, args.listen_port)
        .parse()
        .context("invalid listen host/port")?;
    let target_addr: SocketAddr = format!("{}:{}", args.target_ip, args.target_port)
        .parse()
        .context("invalid target ip/port")?;

    run_udp_forwarder(listen_addr, target_addr).await
}

use std::collections::HashMap;
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
use serde_json::Value;
use tungstenite::connect;
use uuid::Uuid;

const DEFAULT_UA: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0";

type Creds = (String, String, String);

#[derive(Parser, Debug)]
#[command(name = "client")]
struct Args {
    #[arg(long = "turn")]
    turn_host_override: Option<String>,
    #[arg(long = "port")]
    turn_port_override: Option<u16>,
    #[arg(long = "listen", default_value = "127.0.0.1:9000")]
    listen_addr: String,
    #[arg(long = "vk-link")]
    vk_link: Option<String>,
    #[arg(long = "yandex-link")]
    yandex_link: Option<String>,
    #[arg(long = "peer")]
    peer_addr: String,
    #[arg(long = "n")]
    n_connections: Option<usize>,
    #[arg(long = "udp", default_value_t = false)]
    turn_udp: bool,
    #[arg(long = "no-dtls", default_value_t = false)]
    no_dtls: bool,
}

fn do_form_post(client: &Client, url: &str, form_body: &str) -> Result<Value> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_UA));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );

    let resp = client
        .post(url)
        .headers(headers)
        .body(form_body.to_owned())
        .send()
        .with_context(|| format!("request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("non-success response: {url}"))?;

    let value = resp.json::<Value>().context("json decode failed")?;
    Ok(value)
}

fn get_vk_creds(link: &str) -> Result<Creds> {
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(100)
        .build()
        .context("failed to build http client")?;

    let body1 = "client_secret=QbYic1K3lEV5kTGiqlq2&client_id=6287487&scopes=audio_anonymous%2Cvideo_anonymous%2Cphotos_anonymous%2Cprofile_anonymous&isApiOauthAnonymEnabled=false&version=1&app_id=6287487";
    let token1 = do_form_post(&client, "https://login.vk.ru/?act=get_anonym_token", body1)?["data"]
        ["access_token"]
        .as_str()
        .ok_or_else(|| anyhow!("token1 missing"))?
        .to_owned();

    let body2 = format!("access_token={token1}");
    let token2 = do_form_post(
        &client,
        "https://api.vk.ru/method/calls.getAnonymousAccessTokenPayload?v=5.264&client_id=6287487",
        &body2,
    )?["response"]["payload"]
        .as_str()
        .ok_or_else(|| anyhow!("token2 missing"))?
        .to_owned();

    let body3 = format!("client_id=6287487&token_type=messages&payload={token2}&client_secret=QbYic1K3lEV5kTGiqlq2&version=1&app_id=6287487");
    let token3 = do_form_post(&client, "https://login.vk.ru/?act=get_anonym_token", &body3)?
        ["data"]["access_token"]
        .as_str()
        .ok_or_else(|| anyhow!("token3 missing"))?
        .to_owned();

    let body4 =
        format!("vk_join_link=https://vk.com/call/join/{link}&name=123&access_token={token3}");
    let token4 = do_form_post(
        &client,
        "https://api.vk.ru/method/calls.getAnonymousToken?v=5.264",
        &body4,
    )?["response"]["token"]
        .as_str()
        .ok_or_else(|| anyhow!("token4 missing"))?
        .to_owned();

    let body5 = format!(
        "session_data=%7B%22version%22%3A2%2C%22device_id%22%3A%22{}%22%2C%22client_version%22%3A1.1%2C%22client_type%22%3A%22SDK_JS%22%7D&method=auth.anonymLogin&format=JSON&application_key=CGMMEJLGDIHBABABA",
        Uuid::new_v4()
    );
    let token5 = do_form_post(&client, "https://calls.okcdn.ru/fb.do", &body5)?["session_key"]
        .as_str()
        .ok_or_else(|| anyhow!("token5 missing"))?
        .to_owned();

    let body6 = format!(
        "joinLink={link}&isVideo=false&protocolVersion=5&anonymToken={token4}&method=vchat.joinConversationByLink&format=JSON&application_key=CGMMEJLGDIHBABABA&session_key={token5}"
    );
    let resp = do_form_post(&client, "https://calls.okcdn.ru/fb.do", &body6)?;

    let user = resp["turn_server"]["username"]
        .as_str()
        .ok_or_else(|| anyhow!("turn user missing"))?
        .to_owned();
    let pass = resp["turn_server"]["credential"]
        .as_str()
        .ok_or_else(|| anyhow!("turn pass missing"))?
        .to_owned();
    let turn_url = resp["turn_server"]["urls"][0]
        .as_str()
        .ok_or_else(|| anyhow!("turn url missing"))?;

    let turn_addr = turn_url
        .split('?')
        .next()
        .unwrap_or(turn_url)
        .trim_start_matches("turn:")
        .trim_start_matches("turns:")
        .to_owned();

    Ok((user, pass, turn_addr))
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

fn get_yandex_creds(link: &str) -> Result<Creds> {
    let endpoint = format!(
        "https://cloud-api.yandex.ru/telemost_front/v2/telemost/conferences/https%3A%2F%2Ftelemost.yandex.ru%2Fj%2F{link}/connection?next_gen_media_platform_allowed=false"
    );

    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(100)
        .build()
        .context("failed to build http client")?;

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_UA));
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
        HeaderValue::from_str(&Uuid::new_v4().to_string()).context("bad client instance id")?,
    );

    let conference = client
        .get(&endpoint)
        .headers(headers)
        .send()
        .context("conference request failed")?
        .error_for_status()
        .context("conference response status error")?
        .json::<ConferenceResponse>()
        .context("conference decode failed")?;

    let ws_url = &conference.client_configuration.media_server_url;
    let request = format!(
        r#"{{"uid":"{}","hello":{{"participantMeta":{{"name":"Гость","role":"SPEAKER","description":"","sendAudio":false,"sendVideo":false}},"participantAttributes":{{"name":"Гость","role":"SPEAKER","description":""}},"sendAudio":false,"sendVideo":false,"sendSharing":false,"participantId":"{}","roomId":"{}","serviceName":"telemost","credentials":"{}","sdkInfo":{{"implementation":"browser","version":"5.15.0","userAgent":"{}","hwConcurrency":4}},"sdkInitializationId":"{}","disablePublisher":false,"disableSubscriber":false,"disableSubscriberAudio":false,"capabilitiesOffer":{{"offerAnswerMode":["SEPARATE"],"initialSubscriberOffer":["ON_HELLO"],"slotsMode":["FROM_CONTROLLER"],"simulcastMode":["DISABLED"]}}}}}}"#,
        Uuid::new_v4(),
        conference.peer_id,
        conference.room_id,
        conference.credentials,
        DEFAULT_UA,
        Uuid::new_v4()
    );

    let (mut ws, _) = connect(ws_url).context("websocket connect failed")?;
    ws.write_message(tungstenite::Message::Text(request.into()))
        .context("websocket hello failed")?;

    loop {
        let msg = ws.read_message().context("websocket read failed")?;
        let text = match msg {
            tungstenite::Message::Text(v) => v,
            _ => continue,
        };

        let val: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let Some(ice_servers) = val
            .get("serverHello")
            .and_then(|v| v.get("rtcConfiguration"))
            .and_then(|v| v.get("iceServers"))
            .and_then(|v| v.as_array())
        else {
            continue;
        };

        for server in ice_servers {
            let user = server
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let pass = server
                .get("credential")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();

            let urls = server
                .get("urls")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("ice urls missing"))?;

            for u in urls {
                let Some(raw_url) = u.as_str() else {
                    continue;
                };
                if !(raw_url.starts_with("turn:") || raw_url.starts_with("turns:")) {
                    continue;
                }
                if raw_url.contains("transport=tcp") {
                    continue;
                }
                let turn_addr = raw_url
                    .split('?')
                    .next()
                    .unwrap_or(raw_url)
                    .trim_start_matches("turn:")
                    .trim_start_matches("turns:")
                    .to_owned();
                return Ok((user, pass, turn_addr));
            }
        }
    }
}

fn extract_link_tail(raw: &str, marker: &str) -> String {
    let tail = raw.split(marker).last().unwrap_or(raw);
    let mut result = tail.to_owned();
    if let Some(idx) = result.find(['/', '?', '#']) {
        result.truncate(idx);
    }
    result
}

fn parse_host_port(value: &str) -> Result<(String, u16)> {
    let mut parts = value.rsplitn(2, ':');
    let port = parts
        .next()
        .ok_or_else(|| anyhow!("port missing"))?
        .parse::<u16>()
        .context("bad port")?;
    let host = parts
        .next()
        .ok_or_else(|| anyhow!("host missing"))?
        .to_owned();
    Ok((host, port))
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.vk_link.is_some() == args.yandex_link.is_some() {
        return Err(anyhow!(
            "exactly one of --vk-link or --yandex-link is required"
        ));
    }

    let peer_addr: SocketAddr = args
        .peer_addr
        .parse()
        .context("invalid --peer address, expected host:port")?;

    let (link, provider): (String, &str) = if let Some(vk) = &args.vk_link {
        (extract_link_tail(vk, "join/"), "vk")
    } else {
        (
            extract_link_tail(
                args.yandex_link
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing yandex link"))?,
                "j/",
            ),
            "yandex",
        )
    };

    let desired_n = args
        .n_connections
        .unwrap_or(if provider == "vk" { 16 } else { 1 });
    eprintln!("connections requested: {desired_n}");

    if !args.no_dtls {
        eprintln!("warning: DTLS obfuscation is not implemented yet in this Rust prototype.");
    }

    if !args.turn_udp {
        eprintln!("warning: TCP TURN transport is not implemented yet in this Rust prototype.");
    }

    let (turn_user, turn_pass, mut turn_addr) = if provider == "vk" {
        get_vk_creds(&link)?
    } else {
        get_yandex_creds(&link)?
    };

    if let Some(host) = &args.turn_host_override {
        let (_, port) = parse_host_port(&turn_addr)?;
        turn_addr = format!("{host}:{port}");
    }
    if let Some(port) = args.turn_port_override {
        let (host, _) = parse_host_port(&turn_addr)?;
        turn_addr = format!("{host}:{port}");
    }

    eprintln!("TURN credentials acquired for provider={provider}");
    eprintln!(
        "TURN user={} turn_addr={} (password length={})",
        turn_user,
        turn_addr,
        turn_pass.len()
    );

    // NOTE: TURN allocation/authentication is not implemented yet.
    // Current prototype performs direct UDP relay to --peer.
    let listen = UdpSocket::bind(&args.listen_addr)
        .with_context(|| format!("failed to bind local socket: {}", args.listen_addr))?;
    listen
        .set_read_timeout(Some(Duration::from_millis(500)))
        .context("failed to set read timeout")?;

    let upstream = UdpSocket::bind("0.0.0.0:0").context("failed to bind upstream socket")?;
    upstream
        .connect(peer_addr)
        .with_context(|| format!("failed to connect upstream UDP to {peer_addr}"))?;
    upstream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .context("failed to set upstream read timeout")?;

    let running = Arc::new(AtomicBool::new(true));
    let running_sig = Arc::clone(&running);
    ctrlc::set_handler(move || {
        running_sig.store(false, Ordering::SeqCst);
    })
    .context("failed to set ctrl-c handler")?;

    let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>();

    let listen_clone = listen.try_clone().context("clone listen failed")?;
    let upstream_clone = upstream.try_clone().context("clone upstream failed")?;
    let running1 = Arc::clone(&running);
    let t1 = thread::spawn(move || {
        let mut buf = [0u8; 1600];
        while running1.load(Ordering::SeqCst) {
            match listen_clone.recv_from(&mut buf) {
                Ok((n, src)) => {
                    let _ = addr_tx.send(src);
                    if let Err(err) = upstream_clone.send(&buf[..n]) {
                        eprintln!("upstream send error: {err}");
                        break;
                    }
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::WouldBlock
                        || err.kind() == std::io::ErrorKind::TimedOut => {}
                Err(err) => {
                    eprintln!("listen recv error: {err}");
                    break;
                }
            }
        }
    });

    let listen_clone2 = listen.try_clone().context("clone listen2 failed")?;
    let running2 = Arc::clone(&running);
    let t2 = thread::spawn(move || {
        let mut last_addr: Option<SocketAddr> = None;
        let mut buf = [0u8; 1600];
        while running2.load(Ordering::SeqCst) {
            while let Ok(addr) = addr_rx.try_recv() {
                last_addr = Some(addr);
            }

            match upstream.recv(&mut buf) {
                Ok(n) => {
                    if let Some(dst) = last_addr {
                        if let Err(err) = listen_clone2.send_to(&buf[..n], dst) {
                            eprintln!("listen send error: {err}");
                            break;
                        }
                    }
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::WouldBlock
                        || err.kind() == std::io::ErrorKind::TimedOut => {}
                Err(err) => {
                    eprintln!("upstream recv error: {err}");
                    break;
                }
            }
        }
    });

    let _ = t1.join();
    let _ = t2.join();

    let mut summary = HashMap::new();
    summary.insert("provider", provider.to_owned());
    summary.insert("listen", args.listen_addr.clone());
    summary.insert("peer", peer_addr.to_string());
    summary.insert("turn", turn_addr);

    eprintln!(
        "stopped. summary={}",
        serde_json::to_string(&summary).unwrap_or_else(|_| "{}".to_owned())
    );
    Ok(())
}

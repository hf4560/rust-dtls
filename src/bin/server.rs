use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

const BUF_SIZE: usize = 65_535;

#[derive(Parser, Debug)]
#[command(name = "server")]
struct Args {
    #[arg(long = "listen", default_value = "0.0.0.0:8443")]
    listen: String,

    #[arg(long = "connect", default_value = "127.0.0.1:443")]
    connect: String,
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
    let listen_addr: SocketAddr = args.listen.parse().context("invalid --listen")?;
    let target_addr: SocketAddr = args.connect.parse().context("invalid --connect")?;
    run_udp_forwarder(listen_addr, target_addr).await
}

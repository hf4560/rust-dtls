use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

const BUF_SIZE: usize = 65_535;

#[derive(Parser, Debug)]
#[command(name = "server")]
struct Args {
    /// UDP listener (client target)
    #[arg(long = "listen", default_value = "0.0.0.0:8443")]
    listen: String,

    /// Upstream destination on server side
    #[arg(long = "connect", default_value = "127.0.0.1:443")]
    connect: String,
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
    let listen_addr: SocketAddr = args.listen.parse().context("invalid --listen")?;
    let target_addr: SocketAddr = args.connect.parse().context("invalid --connect")?;
    run_udp_forwarder(listen_addr, target_addr)
}

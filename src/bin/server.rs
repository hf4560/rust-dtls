use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "server")]
struct Args {
    #[arg(long = "listen", default_value = "0.0.0.0:56000")]
    listen: String,
    #[arg(long = "connect")]
    connect: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.connect.is_empty() {
        return Err(anyhow!("--connect is required"));
    }

    eprintln!("warning: DTLS listener is not implemented yet in this Rust prototype.");
    eprintln!(
        "starting plain UDP relay: {} -> {}",
        args.listen, args.connect
    );

    let listen = UdpSocket::bind(&args.listen)
        .with_context(|| format!("failed to bind listen socket: {}", args.listen))?;
    listen
        .set_read_timeout(Some(Duration::from_millis(500)))
        .context("failed to set read timeout")?;

    let upstream_addr: SocketAddr = args
        .connect
        .parse()
        .with_context(|| format!("invalid --connect: {}", args.connect))?;
    let upstream = UdpSocket::bind("0.0.0.0:0").context("failed to bind outgoing socket")?;
    upstream
        .connect(upstream_addr)
        .with_context(|| format!("failed to connect outgoing socket to {upstream_addr}"))?;
    upstream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .context("failed to set outgoing read timeout")?;

    let running = Arc::new(AtomicBool::new(true));
    let running_sig = Arc::clone(&running);
    ctrlc::set_handler(move || {
        running_sig.store(false, Ordering::SeqCst);
    })
    .context("failed to set ctrl-c handler")?;

    let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>();

    let listen_clone = listen.try_clone().context("failed to clone listen")?;
    let upstream_clone = upstream.try_clone().context("failed to clone upstream")?;
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

    let listen_clone2 = listen.try_clone().context("failed to clone listen2")?;
    let running2 = Arc::clone(&running);
    let t2 = thread::spawn(move || {
        let mut last_client: Option<SocketAddr> = None;
        let mut buf = [0u8; 1600];
        while running2.load(Ordering::SeqCst) {
            while let Ok(addr) = addr_rx.try_recv() {
                last_client = Some(addr);
            }

            match upstream.recv(&mut buf) {
                Ok(n) => {
                    if let Some(dst) = last_client {
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

    eprintln!("server stopped");
    Ok(())
}

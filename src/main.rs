mod data;
mod env;
mod fd;
mod http;
mod json;
mod knn;
mod response;
mod vector;

use mimalloc::MiMalloc;
use monoio::buf::IoBufMut;
use monoio::io::AsyncReadRent;
use monoio::net::{ListenerOpts, TcpStream, UnixListener};
use monoio::{IoUringDriver, RuntimeBuilder};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::{FromRawFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> std::io::Result<()> {
    data::init();
    knn::warmup();

    let env = env::from_env();
    let sock_path = PathBuf::from(env.sock_path);

    if let Some(parent) = sock_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&sock_path);

    let (notify_tx_std, notify_rx_std) = StdUnixStream::pair()?;
    notify_rx_std.set_nonblocking(true)?;

    let fd_queue: Arc<Mutex<Vec<RawFd>>> = Arc::new(Mutex::new(Vec::new()));
    let ctrl_path = format!("{}.ctrl", sock_path.display());

    {
        let fd_queue = fd_queue.clone();
        let notify_tx = Arc::new(notify_tx_std);
        std::thread::spawn(move || fd::control_thread(ctrl_path, fd_queue, notify_tx));
    }

    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(1024)
        .build()
        .expect("build monoio runtime");

    rt.block_on(async move {
        let opts = ListenerOpts::new().reuse_addr(false).reuse_port(false);
        let listener = UnixListener::bind_with_config(&sock_path, &opts).expect("bind UDS");
        let _ = std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o666));

        monoio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        monoio::spawn(http::serve_connection(stream));
                    }
                    Err(_) => continue,
                }
            }
        });

        let mut notify_rx =
            monoio::net::UnixStream::from_std(notify_rx_std).expect("notify UnixStream::from_std");

        let mut buf: Box<[u8]> = vec![0u8; 64].into_boxed_slice();

        loop {
            let (res, b) = notify_rx.read(buf.slice_mut(..)).await;
            buf = b.into_inner();
            match res {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let fds: Vec<RawFd> = fd_queue.lock().unwrap().drain(..).collect();
                    for fd in fds {
                        let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
                        std_stream.set_nonblocking(true).unwrap();
                        match TcpStream::from_std(std_stream) {
                            Ok(stream) => {
                                monoio::spawn(http::serve_connection(stream));
                            }
                            Err(_) => {}
                        }
                    }
                }
            }
        }
    });

    Ok(())
}

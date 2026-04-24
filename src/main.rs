mod data;
mod env;
mod handlers;
mod json;
mod knn;
mod response;
mod vector;

use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tokio::net::UnixListener;

fn main() -> std::io::Result<()> {
    data::init();
    knn::warmup();

    let env = env::from_env();
    let sock_path = PathBuf::from(env.sock_path);

    if let Some(parent) = sock_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let _ = std::fs::remove_file(&sock_path);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        let listener = UnixListener::bind(&sock_path).expect("bind UDS");
        let _ = std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o666));

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => continue,
            };

            let io = TokioIo::new(stream);
            tokio::task::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .keep_alive(true)
                    .half_close(true)
                    .pipeline_flush(true)
                    .serve_connection(io, service_fn(handlers::handle))
                    .await;
            });
        }
    })
}

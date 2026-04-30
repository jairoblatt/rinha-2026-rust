mod data;
mod env;
mod http;
mod json;
mod knn;
mod response;
mod vector;

use mimalloc::MiMalloc;
use monoio::net::{ListenerOpts, UnixListener};
use monoio::{FusionDriver, RuntimeBuilder};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

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

    let mut rt = RuntimeBuilder::<FusionDriver>::new()
        .with_entries(256)
        .build()
        .expect("build monoio runtime");

    rt.block_on(async move {
        let opts = ListenerOpts::new()
            .reuse_port(false)
            .reuse_addr(false)
            .backlog(4096);
        let listener = UnixListener::bind_with_config(&sock_path, &opts).expect("bind UDS");
        let _ = std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o666));

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    monoio::spawn(http::serve_connection(stream));
                }
                Err(_) => continue,
            }
        }
    });
    Ok(())
}

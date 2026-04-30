mod data;
mod env;
mod handlers;
mod json;
mod knn;
mod response;
mod vector;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use mimalloc::MiMalloc;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tokio::net::UnixListener;

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

    // current_thread runtime com event_interval aumentado.
    //
    // Default: 61 ticks entre checagens de I/O. Para um servidor com 1-2
    // conexões keep-alive permanentes do HAProxy, processando uma request
    // por vez, checar I/O com tanta frequência é desperdício.
    // Aumentamos para 127 — varremos menos vezes o driver de I/O entre
    // execuções de tasks.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .event_interval(127)
        .global_queue_interval(127)
        .build()?;

    rt.block_on(async move {
        let listener = UnixListener::bind(&sock_path).expect("bind UDS");
        let _ = std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o666));

        // Builder configurado uma única vez fora do loop (evita reconfigurar
        // a cada conexão).
        let mut builder = http1::Builder::new();
        builder
            .keep_alive(true)
            .half_close(true)
            .pipeline_flush(true)
            // Buffer pequeno: payload ~700 bytes + headers ~200 bytes = ~1KB.
            // Default ~400KB é desperdício de cache. Mínimo permitido: 8192.
            .max_buf_size(8192)
            // Servidor interno atrás de HAProxy não precisa do header `Date`.
            // Removendo evita formatação de timestamp por response.
            .auto_date_header(false)
            // Sem timeout de leitura de header — sem registrar timer no driver
            // de tempo por conexão. HAProxy já controla timeouts upstream.
            .header_read_timeout(None)
            // Response é sempre um único buffer pequeno (status line + headers
            // + body curto). Vectored writes (`writev`) só compensam quando
            // body está em buffers separados. Forçar flat write evita
            // a heurística do modo `auto`.
            .writev(false);

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => continue,
            };

            let io = TokioIo::new(stream);
            let builder = builder.clone();
            tokio::task::spawn(async move {
                let _ = builder
                    .serve_connection(io, service_fn(handlers::handle))
                    .await;
            });
        }
    })
}
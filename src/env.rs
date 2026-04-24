pub struct Env {
    pub sock_path: String,
}

pub fn from_env() -> Env {
    Env {
        sock_path: std::env::var("SOCK").unwrap_or_else(|_| "/run/sock/api.sock".to_string()),
    }
}

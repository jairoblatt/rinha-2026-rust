use crate::json;
use crate::knn;
use crate::response;
use crate::vector;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use std::convert::Infallible;

const APPROVE_FALLBACK: &[u8] = response::BODIES[0];

pub async fn handle(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method();
    let path = req.uri().path();

    if method == Method::POST && path == "/fraud-score" {
        return Ok(fraud_score_handler(req).await);
    }

    if method == Method::GET && path == "/ready" {
        return Ok(ready_handler());
    }

    Ok(not_found_handler())
}

pub fn not_found_handler() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

pub fn ready_handler() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

async fn fraud_score_handler(req: Request<Incoming>) -> Response<Full<Bytes>> {
    let body_bytes = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return ok_json(APPROVE_FALLBACK),
    };

    let payload = match json::parse(&body_bytes) {
        Some(p) => p,
        None => return ok_json(APPROVE_FALLBACK),
    };

    let query = vector::vectorize(&payload);
    let top = knn::knn5(&query.0);
    let frauds = knn::fraud_count(&top.idx);
    ok_json(response::body_for(frauds))
}

#[inline]
fn ok_json(body: &'static [u8]) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::CONTENT_LENGTH, body.len())
        .body(Full::new(Bytes::from_static(body)))
        .unwrap()
}

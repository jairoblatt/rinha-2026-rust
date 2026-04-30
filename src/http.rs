use crate::data;
use crate::json;
use crate::knn;
use crate::response;
use crate::vector;

use monoio::buf::{IoBufMut, IoVecBuf};
use monoio::io::{AsyncReadRent, AsyncWriteRent};
use monoio::net::UnixStream;

const RX_CAP: usize = 8192;
const MAX_IOVECS: usize = 16;

enum Parsed {
    Incomplete,
    Bad,
    Ready {
        consumed: usize,
    },
    NotFound {
        consumed: usize,
    },
    Fraud {
        body_start: usize,
        body_end: usize,
        consumed: usize,
    },
}

#[inline(always)]
fn find_header_end(buf: &[u8]) -> Option<usize> {
    memchr::memmem::find(buf, b"\r\n\r\n")
}

#[inline(always)]
fn parse_content_length(headers: &[u8]) -> Option<usize> {
    const N: usize = 15;
    if headers.len() < N {
        return None;
    }
    let limit = headers.len() - N;
    let mut i = 0;
    while i <= limit {
        let off = match memchr::memchr2(b'c', b'C', &headers[i..=limit]) {
            Some(o) => o,
            None => return None,
        };
        i += off;
        let mut j = 1;
        let mut equal = true;
        while j < N {
            let lc = headers[i + j] | 0x20;
            if lc != b"content-length:"[j] {
                equal = false;
                break;
            }
            j += 1;
        }
        if equal {
            let mut p = i + N;
            while p < headers.len() && (headers[p] == b' ' || headers[p] == b'\t') {
                p += 1;
            }
            let mut v = 0usize;
            while p < headers.len() && headers[p].is_ascii_digit() {
                v = v
                    .wrapping_mul(10)
                    .wrapping_add((headers[p] - b'0') as usize);
                p += 1;
            }
            return Some(v);
        }
        i += 1;
    }
    None
}

#[inline]
fn parse(buf: &[u8]) -> Parsed {
    if buf.len() < 16 {
        return Parsed::Incomplete;
    }

    let header_end = match find_header_end(buf) {
        Some(p) => p,
        None => return Parsed::Incomplete,
    };

    let line_end = match memchr::memchr(b'\r', &buf[..header_end]) {
        Some(p) => p,
        None => return Parsed::Bad,
    };
    let line = &buf[..line_end];

    if line.starts_with(b"POST ") {
        let rest = &line[5..];
        if path_eq(rest, b"/fraud-score") {
            let cl = parse_content_length(&buf[line_end..header_end]).unwrap_or(0);
            let body_start = header_end + 4;
            let body_end = body_start + cl;
            if buf.len() < body_end {
                return Parsed::Incomplete;
            }
            return Parsed::Fraud {
                body_start,
                body_end,
                consumed: body_end,
            };
        }
        return Parsed::NotFound {
            consumed: header_end + 4,
        };
    }

    if line.starts_with(b"GET ") {
        let rest = &line[4..];
        if path_eq(rest, b"/ready") {
            return Parsed::Ready {
                consumed: header_end + 4,
            };
        }
        return Parsed::NotFound {
            consumed: header_end + 4,
        };
    }

    Parsed::Bad
}

#[inline(always)]
fn path_eq(rest: &[u8], path: &[u8]) -> bool {
    if rest.len() < path.len() + 1 {
        return false;
    }
    if &rest[..path.len()] != path {
        return false;
    }
    let next = rest[path.len()];
    next == b' ' || next == b'?'
}

#[inline]
fn process_fraud(body: &[u8]) -> &'static [u8] {
    match json::parse(body) {
        Some(payload) => {
            let q = vector::vectorize(&payload);
            let frauds = knn::knn5_fraud_count(&q, data::dataset());
            response::http_body_for(frauds)
        }
        None => response::HTTP_FRAUD_FALLBACK,
    }
}

#[inline(always)]
fn push_iovec(iovecs: &mut Vec<libc::iovec>, resp: &'static [u8]) {
    iovecs.push(libc::iovec {
        iov_base: resp.as_ptr() as *mut libc::c_void,
        iov_len: resp.len(),
    });
}

struct OwnedIoVec(Vec<libc::iovec>);

unsafe impl IoVecBuf for OwnedIoVec {
    #[inline]
    fn read_iovec_ptr(&self) -> *const libc::iovec {
        self.0.as_ptr()
    }
    #[inline]
    fn read_iovec_len(&self) -> usize {
        self.0.len()
    }
}

pub async fn serve_connection(mut stream: UnixStream) {
    let mut rx: Box<[u8]> = vec![0u8; RX_CAP].into_boxed_slice();
    let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(MAX_IOVECS);
    let mut head = 0usize;
    let mut tail = 0usize;

    loop {
        iovecs.clear();
        let mut close_after = false;

        while head < tail {
            if iovecs.len() == MAX_IOVECS {
                break;
            }
            match parse(&rx[head..tail]) {
                Parsed::Incomplete => break,
                Parsed::Bad => {
                    push_iovec(&mut iovecs, response::RESP_BAD_REQ);
                    close_after = true;
                    break;
                }
                Parsed::Ready { consumed } => {
                    push_iovec(&mut iovecs, response::RESP_READY);
                    head += consumed;
                }
                Parsed::NotFound { consumed } => {
                    push_iovec(&mut iovecs, response::RESP_NOT_FOUND);
                    head += consumed;
                }
                Parsed::Fraud {
                    body_start,
                    body_end,
                    consumed,
                } => {
                    let resp = process_fraud(&rx[head + body_start..head + body_end]);
                    push_iovec(&mut iovecs, resp);
                    head += consumed;
                }
            }
        }

        if !iovecs.is_empty() {
            let mut remaining: usize = iovecs.iter().map(|v| v.iov_len).sum();
            let mut owned = OwnedIoVec(std::mem::replace(&mut iovecs, Vec::with_capacity(0)));
            loop {
                let (res, back) = stream.writev(owned).await;
                owned = back;
                let n = match res {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                };
                if n >= remaining {
                    break;
                }
                remaining -= n;
                let mut sent = n;
                let mut idx = 0;
                while idx < owned.0.len() && sent >= owned.0[idx].iov_len {
                    sent -= owned.0[idx].iov_len;
                    idx += 1;
                }
                owned.0.drain(..idx);
                if sent > 0 && !owned.0.is_empty() {
                    let head_iov = &mut owned.0[0];
                    head_iov.iov_base =
                        unsafe { (head_iov.iov_base as *mut u8).add(sent) as *mut libc::c_void };
                    head_iov.iov_len -= sent;
                }
            }
            iovecs = owned.0;
        }

        if close_after {
            return;
        }

        if head == tail {
            head = 0;
            tail = 0;
        }

        if tail == RX_CAP {
            return;
        }

        let slice = rx.slice_mut(tail..RX_CAP);
        let (res, slice) = stream.read(slice).await;
        rx = slice.into_inner();
        match res {
            Ok(0) | Err(_) => return,
            Ok(n) => tail += n,
        }
    }
}

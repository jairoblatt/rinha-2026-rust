use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::sync::{Arc, Mutex};

pub unsafe fn recv_fd(sock: libc::c_int) -> Option<RawFd> {
    let cmsg_space = libc::CMSG_SPACE(std::mem::size_of::<libc::c_int>() as u32) as usize;
    let mut cmsg_buf: Vec<u8> = vec![0u8; cmsg_space];

    let mut dummy: u8 = 0;
    let mut iov = libc::iovec {
        iov_base: &mut dummy as *mut u8 as *mut libc::c_void,
        iov_len: 1,
    };

    let mut msg: libc::msghdr = std::mem::zeroed();
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space;

    let ret = libc::recvmsg(sock, &mut msg, 0);
    if ret <= 0 {
        return None;
    }

    let cmsg = libc::CMSG_FIRSTHDR(&msg);
    if cmsg.is_null() {
        return None;
    }
    if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
        let fd = *(libc::CMSG_DATA(cmsg) as *const libc::c_int);
        return Some(fd);
    }
    None
}

pub fn handle_ctrl_conn(
    conn: StdUnixStream,
    fd_queue: Arc<Mutex<Vec<RawFd>>>,
    notify_tx: Arc<StdUnixStream>,
) {
    use std::os::unix::io::AsRawFd;
    loop {
        match unsafe { recv_fd(conn.as_raw_fd()) } {
            None => break,
            Some(fd) => {
                fd_queue.lock().unwrap().push(fd);
                use std::io::Write;
                let _ = (&*notify_tx).write(&[1u8]);
            }
        }
    }
}

pub fn control_thread(
    ctrl_path: String,
    fd_queue: Arc<Mutex<Vec<RawFd>>>,
    notify_tx: Arc<StdUnixStream>,
) {
    let _ = std::fs::remove_file(&ctrl_path);
    let listener = std::os::unix::net::UnixListener::bind(&ctrl_path)
        .unwrap_or_else(|e| panic!("bind ctrl {ctrl_path}: {e}"));
    let _ = std::fs::set_permissions(&ctrl_path, std::fs::Permissions::from_mode(0o666));

    for conn in listener.incoming() {
        match conn {
            Ok(conn) => {
                let fd_queue = fd_queue.clone();
                let notify_tx = notify_tx.clone();
                std::thread::spawn(move || handle_ctrl_conn(conn, fd_queue, notify_tx));
            }
            Err(_) => {}
        }
    }
}

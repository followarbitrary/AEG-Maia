use std::io::{Read, Write};
use std::process::{Command, Stdio};

#[cfg(unix)]
use std::os::fd::AsRawFd;

use aeg_maia::process::{ControlFrameCodec, FrameKind, ListenerMetadata, WorkerMetadata};

fn config_frame() -> Vec<u8> {
    ControlFrameCodec::encode(
        FrameKind::Config,
        b"[worker]\nprogram='/bin/worker'\ncount=1\n",
    )
    .unwrap()
}

fn metadata_frame(metadata: &WorkerMetadata) -> Vec<u8> {
    let encoded = toml::to_string(metadata).unwrap();
    ControlFrameCodec::encode(FrameKind::Metadata, encoded.as_bytes()).unwrap()
}

fn read_frame(stdout: &mut impl Read) -> (FrameKind, Vec<u8>) {
    let mut header = [0_u8; 6];
    stdout.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    let mut bytes = header.to_vec();
    let mut payload = vec![0_u8; length];
    stdout.read_exact(&mut payload).unwrap();
    bytes.extend_from_slice(&payload);
    ControlFrameCodec::decode(&mut bytes).unwrap().unwrap()
}

#[test]
fn worker_exits_nonzero_when_startup_frames_are_missing() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_aeg-maia"))
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdin.take());
    let status = child.wait().unwrap();
    assert!(!status.success());
}

#[test]
fn worker_sends_ready_after_config_and_metadata_then_stops_on_quit() {
    let metadata = WorkerMetadata {
        generation_id: 1,
        worker_id: 0,
        listeners: Vec::new(),
        listener_owners: Default::default(),
        entrypoint: Some("internal-worker".into()),
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_aeg-maia"))
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.as_mut().unwrap();
    stdin.write_all(&config_frame()).unwrap();
    stdin.write_all(&metadata_frame(&metadata)).unwrap();
    stdin.flush().unwrap();
    let (kind, payload) = read_frame(child.stdout.as_mut().unwrap());
    assert_eq!(kind, FrameKind::Ready);
    assert!(payload.is_empty());

    stdin
        .write_all(&ControlFrameCodec::encode(FrameKind::Quit, &[]).unwrap())
        .unwrap();
    stdin.flush().unwrap();
    assert!(child.wait().unwrap().success());
}

#[test]
fn worker_rejects_listener_metadata_that_does_not_match_config() {
    let metadata = WorkerMetadata {
        generation_id: 1,
        worker_id: 0,
        listeners: vec![ListenerMetadata {
            name: "other".into(),
            fd: -1,
        }],
        listener_owners: Default::default(),
        entrypoint: Some("internal-worker".into()),
    };
    let config = b"[worker]\nprogram='/bin/worker'\ncount=1\n[[listen]]\nname='main'\naddress='127.0.0.1:8080'\n";
    let mut child = Command::new(env!("CARGO_BIN_EXE_aeg-maia"))
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.as_mut().unwrap();
    stdin
        .write_all(&ControlFrameCodec::encode(FrameKind::Config, config).unwrap())
        .unwrap();
    stdin.write_all(&metadata_frame(&metadata)).unwrap();
    stdin.flush().unwrap();
    let (kind, payload) = read_frame(child.stdout.as_mut().unwrap());
    assert_eq!(kind, FrameKind::StartupFailed);
    assert!(String::from_utf8_lossy(&payload).contains("metadata mismatch"));
    assert!(!child.wait().unwrap().success());
}

#[test]
fn worker_rejects_an_invalid_fd_for_a_claimed_listener_before_ready() {
    let metadata = WorkerMetadata {
        generation_id: 1,
        worker_id: 0,
        listeners: vec![ListenerMetadata {
            name: "main".into(),
            fd: -1,
        }],
        listener_owners: Default::default(),
        entrypoint: Some("internal-worker".into()),
    };
    let config = b"[worker]\nprogram='/bin/worker'\ncount=1\n[[listen]]\nname='main'\naddress='127.0.0.1:8080'\n";
    let mut child = Command::new(env!("CARGO_BIN_EXE_aeg-maia"))
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.as_mut().unwrap();
    stdin
        .write_all(&ControlFrameCodec::encode(FrameKind::Config, config).unwrap())
        .unwrap();
    stdin.write_all(&metadata_frame(&metadata)).unwrap();
    stdin.flush().unwrap();
    let (kind, payload) = read_frame(child.stdout.as_mut().unwrap());
    assert_eq!(kind, FrameKind::StartupFailed);
    assert!(String::from_utf8_lossy(&payload).contains("fd"));
    assert!(!child.wait().unwrap().success());
}

#[cfg(unix)]
#[test]
fn worker_net_module_accepts_an_inherited_listener() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let fd = listener.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
        0
    );

    let config = format!(
        "[worker]\nprogram='/bin/worker'\ncount=1\nlistener_fds=[{fd}]\n[[listen]]\nname='main'\naddress='{address}'\n"
    );
    let metadata = WorkerMetadata {
        generation_id: 1,
        worker_id: 0,
        listeners: vec![ListenerMetadata {
            name: "main".into(),
            fd,
        }],
        listener_owners: Default::default(),
        entrypoint: Some("internal-worker".into()),
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_aeg-maia"))
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    drop(listener);
    let stdin = child.stdin.as_mut().unwrap();
    stdin
        .write_all(&ControlFrameCodec::encode(FrameKind::Config, config.as_bytes()).unwrap())
        .unwrap();
    stdin.write_all(&metadata_frame(&metadata)).unwrap();
    stdin.flush().unwrap();
    let (kind, payload) = read_frame(child.stdout.as_mut().unwrap());
    assert_eq!(kind, FrameKind::Ready);
    assert!(payload.is_empty());

    let mut client = std::net::TcpStream::connect(address).unwrap();
    client.write_all(b"hello").unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    stdin
        .write_all(&ControlFrameCodec::encode(FrameKind::Quit, &[]).unwrap())
        .unwrap();
    stdin.flush().unwrap();
    assert!(child.wait().unwrap().success());
}

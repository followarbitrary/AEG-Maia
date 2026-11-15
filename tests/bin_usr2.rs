#![cfg(unix)]

use std::{
    fs,
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tempfile::tempdir;

fn config(port: u16, count: &str) -> String {
    format!(
        "[worker]\nprogram='/bin/ignored'\ncount={count}\n[[listen]]\nname='main'\naddress='127.0.0.1:{port}'\n"
    )
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn start(dir: &Path, port: u16) -> (Child, std::path::PathBuf, std::path::PathBuf) {
    let config_path = dir.join("aeg.toml");
    let pid_path = dir.join("aeg.pid");
    let log_path = dir.join("aeg.log");
    fs::write(&config_path, config(port, "1")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_aeg-maia"))
        .arg("-c")
        .arg(&config_path)
        .arg("-p")
        .arg(&pid_path)
        .arg("-l")
        .arg(&log_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    (child, pid_path, config_path)
}

fn wait_pid(path: &Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline {
        if let Ok(value) = fs::read_to_string(path) {
            if let Ok(pid) = value.trim().parse() {
                return pid;
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("pid file not created");
}

fn wait_exit(child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    panic!("old master did not exit");
}

fn signal(pid: u32, signal: i32) {
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, signal) }, 0);
}

fn start_with_config_pid(dir: &Path, port: u16) -> (Child, std::path::PathBuf, std::path::PathBuf) {
    let config_path = dir.join("aeg.toml");
    let pid_path = dir.join("configured.pid");
    let log_path = dir.join("aeg.log");
    fs::write(
        &config_path,
        format!(
            "[master]\npid_file='{}'\n[worker]\nprogram='/bin/ignored'\ncount=1\n[[listen]]\nname='main'\naddress='127.0.0.1:{port}'\n",
            pid_path.display()
        ),
    )
    .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_aeg-maia"))
        .arg("-c")
        .arg(&config_path)
        .arg("-l")
        .arg(&log_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    (child, pid_path, config_path)
}

#[test]
fn usr2_replaces_pid_after_successor_workers_are_ready() {
    let dir = tempdir().unwrap();
    let port = free_port();
    let (mut old_master, pid_path, _) = start(dir.path(), port);
    let old_pid = wait_pid(&pid_path);
    signal(old_pid, libc::SIGUSR2);

    let deadline = Instant::now() + Duration::from_secs(6);
    let new_pid = loop {
        if let Ok(value) = fs::read_to_string(&pid_path) {
            if let Ok(pid) = value.trim().parse::<u32>() {
                if pid != old_pid {
                    break pid;
                }
            }
        }
        assert!(Instant::now() < deadline, "successor never replaced pid");
        thread::sleep(Duration::from_millis(25));
    };
    wait_exit(&mut old_master);
    assert_ne!(new_pid, old_pid);
    signal(new_pid, libc::SIGQUIT);
    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    while pid_path.exists() && Instant::now() < cleanup_deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(!pid_path.exists());
}

#[test]
fn usr2_startup_failure_keeps_old_master_and_pid() {
    let dir = tempdir().unwrap();
    let port = free_port();
    let (mut old_master, pid_path, config_path) = start(dir.path(), port);
    let old_pid = wait_pid(&pid_path);
    fs::write(&config_path, config(port, "0")).unwrap();
    signal(old_pid, libc::SIGUSR2);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(wait_pid(&pid_path), old_pid);
    assert!(old_master.try_wait().unwrap().is_none());
    fs::write(&config_path, config(port, "1")).unwrap();
    signal(old_pid, libc::SIGQUIT);
    wait_exit(&mut old_master);
}

#[test]
fn usr2_uses_pid_path_from_config_when_cli_omits_it() {
    let dir = tempdir().unwrap();
    let port = free_port();
    let (mut old_master, pid_path, _) = start_with_config_pid(dir.path(), port);
    let old_pid = wait_pid(&pid_path);
    signal(old_pid, libc::SIGUSR2);

    let deadline = Instant::now() + Duration::from_secs(6);
    let new_pid = loop {
        if let Ok(value) = fs::read_to_string(&pid_path) {
            if let Ok(pid) = value.trim().parse::<u32>() {
                if pid != old_pid {
                    break pid;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "successor never replaced configured pid"
        );
        thread::sleep(Duration::from_millis(25));
    };
    wait_exit(&mut old_master);
    signal(new_pid, libc::SIGQUIT);
    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    while pid_path.exists() && Instant::now() < cleanup_deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(!pid_path.exists());
}

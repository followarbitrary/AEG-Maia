#![cfg(unix)]

use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tempfile::tempdir;

fn config(count: u32, listener: bool) -> String {
    let listener = if listener {
        "\n[[listen]]\nname='main'\naddress='127.0.0.1:0'\n"
    } else {
        ""
    };
    format!("[worker]\nprogram='/bin/ignored'\ncount={count}\n{listener}")
}

fn spawn_master(dir: &Path, listener: bool) -> (Child, std::path::PathBuf, std::path::PathBuf) {
    let config_path = dir.join("aeg.toml");
    let pid_path = dir.join("aeg.pid");
    let log_path = dir.join("aeg.log");
    fs::write(&config_path, config(1, listener)).unwrap();
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
    (child, pid_path, log_path)
}

fn wait_for_pid(path: &Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if let Ok(value) = fs::read_to_string(path) {
            if let Ok(pid) = value.trim().parse() {
                return pid;
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("pid file was not created: {}", path.display());
}

fn signal(pid: u32, signal: i32) {
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, signal) }, 0);
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
    panic!("master did not exit");
}

#[test]
fn foreground_master_creates_pid_and_quit_removes_it() {
    let dir = tempdir().unwrap();
    let (mut child, pid_path, log_path) = spawn_master(dir.path(), false);
    let pid = wait_for_pid(&pid_path);
    signal(pid, libc::SIGQUIT);
    wait_exit(&mut child);
    assert!(!pid_path.exists());
    assert!(log_path.exists());
}

#[test]
fn hup_reloads_worker_count_without_replacing_master() {
    let dir = tempdir().unwrap();
    let (mut child, pid_path, _) = spawn_master(dir.path(), false);
    let pid = wait_for_pid(&pid_path);
    fs::write(dir.path().join("aeg.toml"), config(2, false)).unwrap();
    signal(pid, libc::SIGHUP);
    thread::sleep(Duration::from_millis(250));
    assert!(child.try_wait().unwrap().is_none());
    signal(pid, libc::SIGQUIT);
    wait_exit(&mut child);
}

#[test]
fn listener_change_is_rejected_and_old_master_stays_alive() {
    let dir = tempdir().unwrap();
    let (mut child, pid_path, _) = spawn_master(dir.path(), false);
    let pid = wait_for_pid(&pid_path);
    fs::write(dir.path().join("aeg.toml"), config(1, true)).unwrap();
    signal(pid, libc::SIGHUP);
    thread::sleep(Duration::from_millis(250));
    assert!(child.try_wait().unwrap().is_none());
    signal(pid, libc::SIGQUIT);
    wait_exit(&mut child);
}

#[test]
fn term_performs_fast_shutdown() {
    let dir = tempdir().unwrap();
    let (mut child, pid_path, _) = spawn_master(dir.path(), false);
    let pid = wait_for_pid(&pid_path);
    signal(pid, libc::SIGTERM);
    wait_exit(&mut child);
    assert!(!pid_path.exists());
}

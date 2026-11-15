#[cfg(unix)]
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::fd::{AsRawFd, FromRawFd, RawFd},
    os::unix::fs::MetadataExt,
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

#[cfg(unix)]
pub type PlatformResult<T> = std::result::Result<T, String>;

#[cfg(unix)]
pub struct PidFile {
    path: PathBuf,
    _file: File,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl PidFile {
    pub fn acquire(path: impl AsRef<Path>, pid: u32) -> PlatformResult<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| error.to_string())?;
        let fd = file.as_raw_fd();
        let lock = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if lock != 0 {
            return Err(format!("pid file is already locked: {}", path.display()));
        }
        file.set_len(0).map_err(|error| error.to_string())?;
        let mut owner = &file;
        writeln!(owner, "{pid}").map_err(|error| error.to_string())?;
        owner.flush().map_err(|error| error.to_string())?;
        let metadata = file.metadata().map_err(|error| error.to_string())?;
        Ok(Self {
            path,
            _file: file,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    #[cfg(test)]
    fn write_new_and_replace(&self, pid: u32) -> PlatformResult<()> {
        let new_path = self.path.with_extension("pid.new");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&new_path)
            .map_err(|error| error.to_string())?;
        writeln!(file, "{pid}").map_err(|error| error.to_string())?;
        file.flush().map_err(|error| error.to_string())?;
        fs::rename(&new_path, &self.path).map_err(|error| error.to_string())
    }

    pub fn acquire_successor(path: impl AsRef<Path>, pid: u32) -> PlatformResult<Self> {
        let path = path.as_ref().to_path_buf();
        let new_path = path.with_extension("pid.new");
        let mut new_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&new_path)
            .map_err(|error| error.to_string())?;
        writeln!(new_file, "{pid}").map_err(|error| error.to_string())?;
        new_file.flush().map_err(|error| error.to_string())?;
        fs::rename(&new_path, &path).map_err(|error| error.to_string())?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| error.to_string())?;
        let lock = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if lock != 0 {
            return Err(format!(
                "successor pid file is already locked: {}",
                path.display()
            ));
        }
        let metadata = file.metadata().map_err(|error| error.to_string())?;
        Ok(Self {
            path,
            _file: file,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub fn remove_if_owner(&self) -> PlatformResult<()> {
        let Ok(metadata) = fs::metadata(&self.path) else {
            return Ok(());
        };
        if metadata.dev() == self.device && metadata.ino() == self.inode {
            fs::remove_file(&self.path).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[cfg(unix)]
pub struct ListenerFdSet {
    entries: Vec<(String, String, RawFd)>,
}

#[cfg(unix)]
impl ListenerFdSet {
    pub fn new(entries: Vec<(String, String, RawFd)>) -> Self {
        Self { entries }
    }

    pub fn clear_cloexec(&mut self) -> PlatformResult<()> {
        for (_, _, fd) in &self.entries {
            let flags = unsafe { libc::fcntl(*fd, libc::F_GETFD) };
            if flags < 0 {
                return Err(io::Error::last_os_error().to_string());
            }
            let result = unsafe { libc::fcntl(*fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) };
            if result < 0 {
                return Err(io::Error::last_os_error().to_string());
            }
        }
        Ok(())
    }

    pub fn manifest(&self) -> String {
        self.entries
            .iter()
            .map(|(name, address, fd)| format!("{name}={address}:{fd}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn entries(&self) -> &[(String, String, RawFd)] {
        &self.entries
    }
}

#[cfg(unix)]
pub struct ReadinessReader {
    stream: UnixStream,
}

#[cfg(unix)]
pub struct ReadinessWriter {
    stream: UnixStream,
}

#[cfg(unix)]
pub struct ReadinessPipe;

#[cfg(unix)]
impl ReadinessPipe {
    pub fn pair() -> io::Result<(ReadinessReader, ReadinessWriter)> {
        let (reader, writer) = UnixStream::pair()?;
        Ok((
            ReadinessReader { stream: reader },
            ReadinessWriter { stream: writer },
        ))
    }
}

#[cfg(unix)]
impl ReadinessWriter {
    pub fn from_raw_fd(fd: RawFd) -> Self {
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        Self { stream }
    }

    pub fn clear_cloexec(&self) -> PlatformResult<()> {
        let flags = unsafe { libc::fcntl(self.stream.as_raw_fd(), libc::F_GETFD) };
        if flags < 0 {
            return Err(io::Error::last_os_error().to_string());
        }
        let result = unsafe {
            libc::fcntl(
                self.stream.as_raw_fd(),
                libc::F_SETFD,
                flags & !libc::FD_CLOEXEC,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error().to_string())
        } else {
            Ok(())
        }
    }

    pub fn raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    pub fn write_ready(&self) -> io::Result<()> {
        let mut stream = self.stream.try_clone()?;
        stream.write_all(b"R")
    }
}

#[cfg(unix)]
impl ReadinessReader {
    pub fn wait(&mut self, timeout: Duration) -> io::Result<bool> {
        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        let mut poll_fd = libc::pollfd {
            fd: self.stream.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let poll_result = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
        if poll_result < 0 {
            return Err(io::Error::last_os_error());
        }
        if poll_result == 0 {
            return Ok(false);
        }
        let mut byte = [0_u8; 1];
        match self.stream.read_exact(&mut byte) {
            Ok(()) => Ok(byte == [b'R']),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof | io::ErrorKind::TimedOut
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlSignal {
    Hup,
    Quit,
    Term,
    Int,
    Usr2,
}

#[cfg(unix)]
impl ControlSignal {
    pub fn from_raw(signal: i32) -> Option<Self> {
        match signal {
            libc::SIGHUP => Some(Self::Hup),
            libc::SIGQUIT => Some(Self::Quit),
            libc::SIGTERM => Some(Self::Term),
            libc::SIGINT => Some(Self::Int),
            libc::SIGUSR2 => Some(Self::Usr2),
            _ => None,
        }
    }
}

#[cfg(unix)]
pub trait SignalSource: Send {
    fn try_recv(&mut self) -> Option<ControlSignal>;
}

#[cfg(unix)]
pub struct UnixSignalSource {
    receiver: Receiver<i32>,
}

#[cfg(unix)]
impl UnixSignalSource {
    pub fn new() -> PlatformResult<Self> {
        let signal_numbers = [
            libc::SIGHUP,
            libc::SIGQUIT,
            libc::SIGTERM,
            libc::SIGINT,
            libc::SIGUSR2,
        ];
        set_signal_mask(&signal_numbers, libc::SIG_BLOCK).map_err(|error| error.to_string())?;
        let signals = signal_hook::iterator::Signals::new(signal_numbers)
            .map_err(|error| error.to_string())?;
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let _ = set_signal_mask(&signal_numbers, libc::SIG_UNBLOCK);
            let mut signals = signals;
            for signal in signals.forever() {
                if sender.send(signal).is_err() {
                    break;
                }
            }
        });
        Ok(Self { receiver })
    }
}

#[cfg(unix)]
fn set_signal_mask(signals: &[i32], how: i32) -> io::Result<()> {
    let mut set = unsafe { std::mem::zeroed::<libc::sigset_t>() };
    if unsafe { libc::sigemptyset(&mut set) } != 0 {
        return Err(io::Error::last_os_error());
    }
    for signal in signals {
        if unsafe { libc::sigaddset(&mut set, *signal) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    let result = unsafe { libc::pthread_sigmask(how, &set, std::ptr::null_mut()) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(unix)]
impl SignalSource for UnixSignalSource {
    fn try_recv(&mut self) -> Option<ControlSignal> {
        self.receiver
            .try_recv()
            .ok()
            .and_then(ControlSignal::from_raw)
    }
}

#[cfg(unix)]
pub fn daemonize(log_path: Option<&Path>) -> PlatformResult<()> {
    let first = unsafe { libc::fork() };
    if first < 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    if first > 0 {
        std::process::exit(0);
    }
    if unsafe { libc::setsid() } < 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    let second = unsafe { libc::fork() };
    if second < 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    if second > 0 {
        std::process::exit(0);
    }
    if unsafe { libc::chdir(c"/".as_ptr()) } != 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    let null = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
        .map_err(|error| error.to_string())?;
    let output = if let Some(path) = log_path {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?
    } else {
        null.try_clone().map_err(|error| error.to_string())?
    };
    for (target, source) in [
        (0, null.as_raw_fd()),
        (1, output.as_raw_fd()),
        (2, output.as_raw_fd()),
    ] {
        if unsafe { libc::dup2(source, target) } < 0 {
            return Err(io::Error::last_os_error().to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, os::fd::AsRawFd, time::Duration};

    use tempfile::tempdir;

    use super::{ControlSignal, ListenerFdSet, PidFile, ReadinessPipe};

    #[cfg(unix)]
    #[test]
    fn pid_replacement_is_atomic_and_old_owner_cannot_remove_successor() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("aeg.pid");
        let owner = PidFile::acquire(&path, 11).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "11\n");

        owner.write_new_and_replace(22).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "22\n");
        owner.remove_if_owner().unwrap();
        assert!(path.exists(), "old pid owner must not remove successor pid");
    }

    #[cfg(unix)]
    #[test]
    fn locked_pid_file_is_not_truncated_by_a_second_acquire() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("aeg.pid");
        let _owner = PidFile::acquire(&path, 11).unwrap();
        assert!(PidFile::acquire(&path, 22).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "11\n");
    }

    #[cfg(unix)]
    #[test]
    fn listener_fd_manifest_is_ordered_and_clears_close_on_exec() {
        let (reader, _writer) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut listeners =
            ListenerFdSet::new(vec![("b".into(), "b.addr".into(), reader.as_raw_fd())]);
        listeners.clear_cloexec().unwrap();
        assert_eq!(
            listeners.manifest(),
            format!("b=b.addr:{}", reader.as_raw_fd())
        );
    }

    #[cfg(unix)]
    #[test]
    fn readiness_pipe_accepts_r_and_rejects_eof() {
        let (mut reader, writer) = ReadinessPipe::pair().unwrap();
        writer.write_ready().unwrap();
        assert!(reader.wait(Duration::from_secs(1)).unwrap());

        let (mut reader, writer) = ReadinessPipe::pair().unwrap();
        drop(writer);
        assert!(!reader.wait(Duration::from_millis(10)).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn signal_source_maps_handled_unix_signals() {
        assert_eq!(
            ControlSignal::from_raw(libc::SIGHUP),
            Some(ControlSignal::Hup)
        );
        assert_eq!(
            ControlSignal::from_raw(libc::SIGQUIT),
            Some(ControlSignal::Quit)
        );
        assert_eq!(
            ControlSignal::from_raw(libc::SIGTERM),
            Some(ControlSignal::Term)
        );
        assert_eq!(
            ControlSignal::from_raw(libc::SIGUSR2),
            Some(ControlSignal::Usr2)
        );
        assert_eq!(ControlSignal::from_raw(libc::SIGPIPE), None);
    }
}

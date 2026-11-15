#[cfg(unix)]
mod unix_master {
    use std::{
        collections::VecDeque,
        fs::OpenOptions,
        io::Write,
        net::{SocketAddr, TcpListener, ToSocketAddrs},
        os::fd::{AsRawFd, FromRawFd},
        path::{Path, PathBuf},
        process::{Command, Stdio},
        thread,
        time::{Duration, Instant},
    };

    use aeg_maia::{
        CommandWorkerFactory, ConfigManager, ConfigSnapshot, ConfigSource, Error,
        GenerationManager, ListenSpec, ModuleRegistryFactory, ProcessEvent, Result,
        TomlConfigSource, WorkerConfig,
    };

    use super::super::{modules::ReferenceRegistryFactory, platform, Cli};

    struct Logger {
        file: Option<std::fs::File>,
        path: Option<PathBuf>,
    }

    impl Logger {
        fn new(path: Option<&Path>) -> Result<Self> {
            let file = path
                .map(|path| {
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .map_err(|error| Error::Process(error.to_string()))
                })
                .transpose()?;
            Ok(Self {
                file,
                path: path.map(Path::to_path_buf),
            })
        }

        fn reopen(&mut self, path: Option<&Path>) -> Result<()> {
            let next_path = path.map(Path::to_path_buf);
            self.file = next_path
                .as_deref()
                .map(|path| {
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .map_err(|error| Error::Process(error.to_string()))
                })
                .transpose()?;
            self.path = next_path;
            Ok(())
        }

        fn reopen_current(&mut self) -> Result<()> {
            let path = self.path.clone();
            self.reopen(path.as_deref())
        }

        fn line(&mut self, message: impl AsRef<str>) {
            let message = message.as_ref();
            if let Some(file) = &mut self.file {
                let _ = writeln!(file, "{message}");
                let _ = file.flush();
            } else {
                eprintln!("aeg-maia: {message}");
            }
        }
    }

    struct BoundListeners {
        _sockets: Vec<TcpListener>,
        fds: Vec<i32>,
        manifest: String,
        identities: Vec<(String, String)>,
    }

    impl BoundListeners {
        fn bind(specs: &[ListenSpec]) -> Result<Self> {
            let mut sockets = Vec::with_capacity(specs.len());
            let mut entries = Vec::with_capacity(specs.len());
            for listener in specs {
                let socket = bind_listener(listener)?;
                let fd = socket.as_raw_fd();
                entries.push((listener.name.clone(), listener.address.clone(), fd));
                sockets.push(socket);
            }
            let mut manifest = platform::ListenerFdSet::new(entries);
            manifest.clear_cloexec().map_err(Error::Process)?;
            let fds = manifest.entries().iter().map(|(_, _, fd)| *fd).collect();
            let manifest_text = manifest.manifest();
            Ok(Self {
                _sockets: sockets,
                fds,
                manifest: manifest_text,
                identities: specs
                    .iter()
                    .map(|listener| (listener.name.clone(), listener.address.clone()))
                    .collect(),
            })
        }

        fn adopt(manifest: &str) -> Result<Self> {
            let mut sockets = Vec::new();
            let mut entries = Vec::new();
            for item in manifest.split(',').filter(|item| !item.is_empty()) {
                let (name, address_fd) = item
                    .split_once('=')
                    .ok_or_else(|| Error::Process("invalid inherited listener manifest".into()))?;
                let (address, fd) = address_fd
                    .rsplit_once(':')
                    .ok_or_else(|| Error::Process("invalid inherited listener fd".into()))?;
                let fd = fd
                    .parse::<i32>()
                    .map_err(|error| Error::Process(error.to_string()))?;
                let socket = unsafe { TcpListener::from_raw_fd(fd) };
                socket
                    .set_nonblocking(true)
                    .map_err(|error| Error::Process(error.to_string()))?;
                entries.push((name.to_owned(), address.to_owned(), fd));
                sockets.push(socket);
            }
            let identities = entries
                .iter()
                .map(|(name, address, _)| (name.clone(), address.clone()))
                .collect();
            let manifest_text = platform::ListenerFdSet::new(entries).manifest();
            let fds = sockets.iter().map(AsRawFd::as_raw_fd).collect();
            Ok(Self {
                _sockets: sockets,
                fds,
                manifest: manifest_text,
                identities,
            })
        }

        fn manifest(&self) -> &str {
            &self.manifest
        }
    }

    fn bind_listener(spec: &ListenSpec) -> Result<TcpListener> {
        let address = spec
            .address
            .to_socket_addrs()
            .map_err(|error| Error::Process(error.to_string()))?
            .next()
            .ok_or_else(|| Error::Process(format!("listener {} has no address", spec.name)))?;
        let domain = match address {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(Error::Process(std::io::Error::last_os_error().to_string()));
        }
        let failed = |message: String| {
            unsafe { libc::close(fd) };
            Error::Process(message)
        };
        if spec.reuse_port {
            let enabled: libc::c_int = 1;
            let result = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_REUSEPORT,
                    (&enabled as *const libc::c_int).cast(),
                    std::mem::size_of_val(&enabled) as libc::socklen_t,
                )
            };
            if result < 0 {
                return Err(failed(std::io::Error::last_os_error().to_string()));
            }
        }
        let result = match address {
            SocketAddr::V4(address) => {
                let mut sockaddr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
                sockaddr.sin_family = libc::AF_INET as libc::sa_family_t;
                sockaddr.sin_port = address.port().to_be();
                sockaddr.sin_addr = libc::in_addr {
                    s_addr: u32::from_ne_bytes(address.ip().octets()),
                };
                unsafe {
                    libc::bind(
                        fd,
                        (&sockaddr as *const libc::sockaddr_in).cast(),
                        std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                    )
                }
            }
            SocketAddr::V6(address) => {
                let mut sockaddr: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
                sockaddr.sin6_family = libc::AF_INET6 as libc::sa_family_t;
                sockaddr.sin6_port = address.port().to_be();
                sockaddr.sin6_flowinfo = address.flowinfo();
                sockaddr.sin6_scope_id = address.scope_id();
                sockaddr.sin6_addr = libc::in6_addr {
                    s6_addr: address.ip().octets(),
                };
                unsafe {
                    libc::bind(
                        fd,
                        (&sockaddr as *const libc::sockaddr_in6).cast(),
                        std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                    )
                }
            }
        };
        if result < 0 {
            return Err(failed(std::io::Error::last_os_error().to_string()));
        }
        let backlog = spec.backlog.clamp(1, libc::c_int::MAX as u32) as libc::c_int;
        if unsafe { libc::listen(fd, backlog) } < 0 {
            return Err(failed(std::io::Error::last_os_error().to_string()));
        }
        let socket = unsafe { TcpListener::from_raw_fd(fd) };
        socket
            .set_nonblocking(true)
            .map_err(|error| Error::Process(error.to_string()))?;
        Ok(socket)
    }

    type Manager = GenerationManager<CommandWorkerFactory>;

    struct UpgradeContext {
        manifest: String,
        ready: platform::ReadinessWriter,
        pid_path: PathBuf,
    }

    fn upgrade_context(cli: &Cli, configured_pid: Option<&Path>) -> Result<Option<UpgradeContext>> {
        if std::env::var("AEG_USR2").ok().as_deref() != Some("1") {
            return Ok(None);
        }
        let manifest = std::env::var("AEG_LISTENER_MANIFEST")
            .map_err(|_| Error::Process("USR2 successor listener manifest is missing".into()))?;
        let ready_fd = std::env::var("AEG_READY_FD")
            .map_err(|_| Error::Process("USR2 successor readiness fd is missing".into()))?
            .parse::<i32>()
            .map_err(|error| Error::Process(error.to_string()))?;
        let pid_path = cli
            .pid
            .clone()
            .or_else(|| configured_pid.map(Path::to_path_buf))
            .ok_or_else(|| Error::Process("USR2 requires a pid path".into()))?;
        Ok(Some(UpgradeContext {
            manifest,
            ready: platform::ReadinessWriter::from_raw_fd(ready_fd),
            pid_path,
        }))
    }

    pub fn run_master(cli: Cli) -> Result<()> {
        let source = TomlConfigSource::new(&cli.config);
        let loaded = source.load()?;
        let effective_pid_path = cli.pid.clone().or_else(|| loaded.master.pid_file.clone());
        let upgrade = upgrade_context(&cli, loaded.master.pid_file.as_deref())?;
        if upgrade.is_none() && (cli.daemon || loaded.master.daemon) && effective_pid_path.is_none()
        {
            return Err(Error::Process(
                "daemon mode requires --pid or [master].pid_file".into(),
            ));
        }
        let registry_factory = ReferenceRegistryFactory;
        let registry = registry_factory.build(&loaded)?;
        let mut initial_cycle = registry.build_cycle()?;
        if let Err(error) = initial_cycle.init_module() {
            let _ = initial_cycle.exit_master();
            return Err(error);
        }
        if let Err(error) = initial_cycle.validate_listener_config(&loaded.listeners) {
            let _ = initial_cycle.exit_master();
            return Err(error);
        }
        let listener_specs = initial_cycle.listeners().to_vec();
        if upgrade.is_none() && (cli.daemon || loaded.master.daemon) {
            if let Err(error) =
                platform::daemonize(cli.log.as_deref().or(loaded.master.log_file.as_deref()))
            {
                let _ = initial_cycle.exit_master();
                return Err(Error::Process(error));
            }
        }
        let listeners = if let Some(upgrade) = &upgrade {
            let listeners = BoundListeners::adopt(&upgrade.manifest)?;
            if listeners.identities != loaded.listener_identity() {
                return Err(Error::Process(
                    "USR2 inherited listener manifest does not match configuration".into(),
                ));
            }
            listeners
        } else {
            match BoundListeners::bind(&listener_specs) {
                Ok(listeners) => listeners,
                Err(error) => {
                    let _ = initial_cycle.exit_master();
                    return Err(error);
                }
            }
        };
        let mut initial = apply_runtime_overrides(loaded, &cli, &listeners.fds)?;
        let mut config_manager = ConfigManager::new(source, initial.clone())?;
        let mut logger = Logger::new(cli.log.as_deref().or(initial.master.log_file.as_deref()))?;
        let mut signals = platform::UnixSignalSource::new().map_err(Error::Process)?;
        let mut pid_file = if upgrade.is_none() {
            effective_pid_path
                .clone()
                .map(|path| {
                    platform::PidFile::acquire(&path, std::process::id()).map_err(Error::Process)
                })
                .transpose()?
        } else {
            None
        };
        initial.master.pid_file = cli.pid.clone().or(initial.master.pid_file.clone());
        initial.master.log_file = cli.log.clone().or(initial.master.log_file.clone());
        let registry_factory: Box<dyn ModuleRegistryFactory> = Box::new(registry_factory);
        let factory = CommandWorkerFactory::new().with_process_group();
        let mut manager = match GenerationManager::new_with_cycle(
            factory,
            initial.clone(),
            registry_factory,
            initial_cycle,
        ) {
            Ok(manager) => manager,
            Err(error) => {
                if let Some(pid_file) = &pid_file {
                    let _ = pid_file.remove_if_owner();
                }
                return Err(error);
            }
        };
        if let Err(error) = wait_for_initial_ready(&mut manager, &mut logger) {
            let _ = manager.force_shutdown();
            if let Some(pid_file) = &pid_file {
                let _ = pid_file.remove_if_owner();
            }
            return Err(error);
        }
        if let Some(upgrade) = upgrade {
            let successor_pid =
                match platform::PidFile::acquire_successor(&upgrade.pid_path, std::process::id()) {
                    Ok(pid) => pid,
                    Err(error) => {
                        let _ = manager.force_shutdown();
                        return Err(Error::Process(error));
                    }
                };
            if let Err(error) = upgrade.ready.write_ready() {
                let _ = successor_pid.remove_if_owner();
                let _ = manager.force_shutdown();
                return Err(Error::Process(error.to_string()));
            }
            pid_file = Some(successor_pid);
        }
        logger.line(format!(
            "master ready pid={} workers={}",
            std::process::id(),
            initial.worker.count
        ));

        let mut deferred_signals = VecDeque::new();
        loop {
            let events = match manager.tick(Instant::now()) {
                Ok(events) => events,
                Err(error) => {
                    cleanup_after_fatal(&mut manager, &pid_file);
                    return Err(error);
                }
            };
            for event in events {
                logger.line(format!("event: {event:?}"));
            }
            let Some(signal) = deferred_signals
                .pop_front()
                .or_else(|| platform::SignalSource::try_recv(&mut signals))
            else {
                if let Err(error) =
                    platform::SignalSource::wait(&mut signals, Duration::from_millis(20))
                {
                    cleanup_after_fatal(&mut manager, &pid_file);
                    return Err(Error::Process(error));
                }
                continue;
            };
            match signal {
                platform::ControlSignal::Usr1 => {
                    if let Err(error) =
                        logger.reopen(cli.log.as_deref().or(initial.master.log_file.as_deref()))
                    {
                        logger.line(format!("log reopen failed: {error}"));
                    }
                    if let Err(error) = manager.reopen_logs() {
                        logger.line(format!("worker log reopen failed: {error}"));
                    }
                }
                platform::ControlSignal::Hup => {
                    if let Err(error) = reload(
                        &mut manager,
                        &mut config_manager,
                        &cli,
                        &listeners,
                        &mut logger,
                        &mut signals,
                        &mut deferred_signals,
                    ) {
                        logger.line(format!("reload rejected: {error}"));
                    }
                }
                platform::ControlSignal::Quit => {
                    if let Err(error) = manager.shutdown() {
                        cleanup_after_fatal(&mut manager, &pid_file);
                        return Err(error);
                    }
                    if let Err(error) = wait_for_retirement(&mut manager, &mut logger, &mut signals)
                    {
                        cleanup_after_fatal(&mut manager, &pid_file);
                        return Err(error);
                    }
                    if let Some(pid_file) = &pid_file {
                        pid_file.remove_if_owner().map_err(Error::Process)?;
                    }
                    return Ok(());
                }
                platform::ControlSignal::Term | platform::ControlSignal::Int => {
                    let _ = manager.force_shutdown();
                    if let Some(pid_file) = &pid_file {
                        pid_file.remove_if_owner().map_err(Error::Process)?;
                    }
                    return Ok(());
                }
                platform::ControlSignal::Usr2 => {
                    if let Err(error) = spawn_successor(
                        &cli,
                        &listeners,
                        &mut manager,
                        &mut logger,
                        &mut signals,
                        initial.generation.handoff_timeout_secs,
                        pid_file.as_ref(),
                        effective_pid_path.as_deref(),
                    ) {
                        logger.line(format!("USR2 handoff failed: {error}"));
                    } else {
                        if let Some(pid_file) = &pid_file {
                            let _ = pid_file.remove_if_owner();
                        }
                        return Ok(());
                    }
                }
            }
        }
    }

    fn apply_runtime_overrides(
        mut snapshot: ConfigSnapshot,
        cli: &Cli,
        listener_fds: &[i32],
    ) -> Result<ConfigSnapshot> {
        snapshot.master.pid_file = cli.pid.clone().or(snapshot.master.pid_file);
        snapshot.master.log_file = cli.log.clone().or(snapshot.master.log_file);
        snapshot.master.daemon |= cli.daemon;
        snapshot.worker = internal_worker_config(snapshot.worker, std::env::current_exe()?);
        snapshot.worker.listener_fds = listener_fds.to_vec();
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn internal_worker_config(mut worker: WorkerConfig, executable: PathBuf) -> WorkerConfig {
        worker.program = executable.to_string_lossy().into_owned();
        worker.args = vec!["--worker".into()];
        worker
    }

    fn wait_for_initial_ready(manager: &mut Manager, logger: &mut Logger) -> Result<()> {
        loop {
            for event in manager.tick(Instant::now())? {
                logger.line(format!("event: {event:?}"));
                match event {
                    ProcessEvent::GenerationReady(_) => return Ok(()),
                    ProcessEvent::ReloadRolledBack(generation) => {
                        return Err(Error::Process(format!(
                            "initial generation {generation} failed before ready"
                        )));
                    }
                    _ => {}
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_retirement(
        manager: &mut Manager,
        logger: &mut Logger,
        signals: &mut platform::UnixSignalSource,
    ) -> Result<()> {
        loop {
            for event in manager.tick(Instant::now())? {
                let retired = matches!(event, ProcessEvent::GenerationRetired(_));
                logger.line(format!("event: {event:?}"));
                if retired {
                    return Ok(());
                }
            }
            while let Some(signal) = platform::SignalSource::try_recv(signals) {
                match signal {
                    platform::ControlSignal::Usr1 => {
                        logger.reopen_current()?;
                    }
                    platform::ControlSignal::Term | platform::ControlSignal::Int => {
                        logger.line(format!("signal {signal:?} escalates graceful retirement"));
                        manager.force_shutdown()?;
                        return Ok(());
                    }
                    _ => {}
                }
            }
            platform::SignalSource::wait(signals, Duration::from_millis(20))
                .map_err(Error::Process)?;
        }
    }

    fn reload(
        manager: &mut Manager,
        config_manager: &mut ConfigManager<TomlConfigSource>,
        cli: &Cli,
        listeners: &BoundListeners,
        logger: &mut Logger,
        signals: &mut platform::UnixSignalSource,
        deferred_signals: &mut VecDeque<platform::ControlSignal>,
    ) -> Result<()> {
        let loaded = config_manager.prepare_reload()?;
        if loaded.listener_identity() != config_manager.current().listener_identity() {
            logger.line("reload rejected: listener configuration changes require restart");
            return Ok(());
        }
        let next = apply_runtime_overrides(loaded, cli, &listeners.fds)?;
        let id = manager.reload(next.clone())?;
        loop {
            for event in manager.tick(Instant::now())? {
                logger.line(format!("event: {event:?}"));
                match event {
                    ProcessEvent::GenerationReady(generation) if generation == id => {
                        config_manager.commit(next)?;
                        return Ok(());
                    }
                    ProcessEvent::ReloadRolledBack(generation) if generation == id => {
                        return Err(Error::Process(format!(
                            "reload generation {generation} rolled back"
                        )));
                    }
                    _ => {}
                }
            }
            drain_busy_signals(signals, deferred_signals, logger);
            platform::SignalSource::wait(signals, Duration::from_millis(20))
                .map_err(Error::Process)?;
        }
    }

    fn drain_busy_signals(
        signals: &mut platform::UnixSignalSource,
        deferred_signals: &mut VecDeque<platform::ControlSignal>,
        logger: &mut Logger,
    ) {
        while let Some(signal) = platform::SignalSource::try_recv(signals) {
            match signal {
                platform::ControlSignal::Hup
                | platform::ControlSignal::Usr1
                | platform::ControlSignal::Usr2 => {
                    logger.line(format!(
                        "signal {signal:?} rejected while reload is in progress"
                    ));
                }
                platform::ControlSignal::Quit
                | platform::ControlSignal::Term
                | platform::ControlSignal::Int => deferred_signals.push_back(signal),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_successor(
        cli: &Cli,
        listeners: &BoundListeners,
        manager: &mut Manager,
        logger: &mut Logger,
        signals: &mut platform::UnixSignalSource,
        handoff_timeout_secs: u64,
        pid_file: Option<&platform::PidFile>,
        pid_path: Option<&Path>,
    ) -> Result<()> {
        if pid_file.is_none() {
            return Err(Error::Process("USR2 requires a pid file".into()));
        }
        let pid_path = pid_path.ok_or_else(|| Error::Process("USR2 requires a pid path".into()))?;
        let (mut reader, writer) =
            platform::ReadinessPipe::pair().map_err(|error| Error::Process(error.to_string()))?;
        writer.clear_cloexec().map_err(Error::Process)?;
        let mut command = Command::new(std::env::current_exe()?);
        command
            .arg("-c")
            .arg(&cli.config)
            .arg("-p")
            .arg(pid_path)
            .env("AEG_USR2", "1")
            .env("AEG_LISTENER_MANIFEST", listeners.manifest())
            .env("AEG_READY_FD", writer.raw_fd().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(log) = &cli.log {
            command.arg("-l").arg(log);
        }
        let mut child = command
            .spawn()
            .map_err(|error| Error::Process(error.to_string()))?;
        drop(writer);
        let ready = reader.wait(Duration::from_secs(handoff_timeout_secs));
        match ready {
            Ok(true) => {}
            Ok(false) => {
                let _ = child.kill();
                let _ = child.wait();
                if let Some(pid_file) = pid_file {
                    let _ = pid_file.restore_after_failed_handoff();
                }
                return Err(Error::Process("USR2 successor did not report ready".into()));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                if let Some(pid_file) = pid_file {
                    let _ = pid_file.restore_after_failed_handoff();
                }
                return Err(Error::Process(error.to_string()));
            }
        }
        logger.line("USR2 successor ready; draining old master");
        manager.shutdown()?;
        wait_for_retirement(manager, logger, signals)
    }

    fn cleanup_after_fatal(manager: &mut Manager, pid_file: &Option<platform::PidFile>) {
        let _ = manager.force_shutdown();
        if let Some(pid_file) = pid_file {
            let _ = pid_file.remove_if_owner();
        }
    }
}

#[cfg(unix)]
pub use unix_master::run_master;

#[cfg(not(unix))]
pub fn run_master(_cli: super::Cli) -> aeg_maia::Result<()> {
    Err(aeg_maia::Error::Process(
        "master mode is only supported on Unix".into(),
    ))
}

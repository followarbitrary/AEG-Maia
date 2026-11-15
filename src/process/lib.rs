use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
};

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{
    config::{ConfigSnapshot, OnAllFailed, RestartConfig},
    error::{Error, Result},
    module::{ModuleCycle, ModuleRegistry},
};

pub use crate::module::WorkerId;
pub type GenerationId = u64;

pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Config = 1,
    Metadata = 2,
    Quit = 3,
    Terminate = 4,
    Ready = 5,
    StartupFailed = 6,
}

impl TryFrom<u8> for FrameKind {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Config),
            2 => Ok(Self::Metadata),
            3 => Ok(Self::Quit),
            4 => Ok(Self::Terminate),
            5 => Ok(Self::Ready),
            6 => Ok(Self::StartupFailed),
            other => Err(Error::Process(format!(
                "unknown worker control frame kind: {other}"
            ))),
        }
    }
}

pub struct ControlFrameCodec;

impl ControlFrameCodec {
    pub const VERSION: u8 = 1;

    pub fn encode(kind: FrameKind, payload: &[u8]) -> Result<Vec<u8>> {
        if payload.len() > MAX_FRAME_BYTES {
            return Err(Error::Process(format!(
                "worker control frame exceeds {} bytes",
                MAX_FRAME_BYTES
            )));
        }
        let length = u32::try_from(payload.len())
            .map_err(|_| Error::Process("worker control frame length overflow".into()))?;
        let mut frame = Vec::with_capacity(6 + payload.len());
        frame.push(Self::VERSION);
        frame.push(kind as u8);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(payload);
        Ok(frame)
    }

    pub fn decode(buffer: &mut Vec<u8>) -> Result<Option<(FrameKind, Vec<u8>)>> {
        if buffer.len() < 6 {
            return Ok(None);
        }
        if buffer[0] != Self::VERSION {
            return Err(Error::Process(format!(
                "unsupported worker control frame version: {}",
                buffer[0]
            )));
        }
        let kind = FrameKind::try_from(buffer[1])?;
        let length = u32::from_be_bytes([buffer[2], buffer[3], buffer[4], buffer[5]]) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(Error::Process(format!(
                "worker control frame exceeds {} bytes",
                MAX_FRAME_BYTES
            )));
        }
        let total = 6 + length;
        if buffer.len() < total {
            return Ok(None);
        }
        let payload = buffer[6..total].to_vec();
        buffer.drain(..total);
        Ok(Some((kind, payload)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerMetadata {
    pub name: String,
    pub fd: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerMetadata {
    pub generation_id: GenerationId,
    pub worker_id: WorkerId,
    #[serde(default)]
    pub listeners: Vec<ListenerMetadata>,
    pub entrypoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExitStatus {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub control_channel_eof: bool,
}

impl ExitStatus {
    pub fn code(code: i32) -> Self {
        Self {
            code: Some(code),
            signal: None,
            control_channel_eof: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCommand {
    Configure {
        snapshot: Box<ConfigSnapshot>,
        metadata: WorkerMetadata,
    },
    Quit,
    Terminate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerEvent {
    Ready,
    StartupFailed(String),
    ControlChannelEof,
}

pub trait ControlChannel: Send {
    fn send(&mut self, command: WorkerCommand) -> Result<()>;
    fn try_recv(&mut self) -> Result<Option<WorkerEvent>>;
}

pub trait WorkerHandle: Send {
    fn worker_id(&self) -> WorkerId;
    fn channel(&mut self) -> &mut dyn ControlChannel;
    fn poll_exit(&mut self) -> Result<Option<ExitStatus>>;
    fn terminate(&mut self) -> Result<()>;
    fn kill(&mut self) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct WorkerSpawnRequest {
    pub generation_id: GenerationId,
    pub worker_id: WorkerId,
    pub config: ConfigSnapshot,
    pub entrypoint: Option<String>,
    pub inherited_listener_fds: Vec<i32>,
}

impl WorkerSpawnRequest {
    pub fn metadata(&self) -> WorkerMetadata {
        let listeners = self
            .config
            .listeners
            .iter()
            .zip(self.inherited_listener_fds.iter().copied())
            .map(|(listener, fd)| ListenerMetadata {
                name: listener.name.clone(),
                fd,
            })
            .collect();
        WorkerMetadata {
            generation_id: self.generation_id,
            worker_id: self.worker_id,
            listeners,
            entrypoint: self.entrypoint.clone(),
        }
    }
}

pub trait WorkerFactory: Send + Sync {
    fn spawn(&self, request: WorkerSpawnRequest) -> Result<Box<dyn WorkerHandle>>;
}

pub trait ControlChannelFactory: Send + Sync {
    fn create(&self, child: &mut Child) -> Result<Box<dyn ControlChannel>>;
}

#[derive(Default)]
pub struct NullControlChannel;

impl ControlChannel for NullControlChannel {
    fn send(&mut self, _command: WorkerCommand) -> Result<()> {
        Ok(())
    }
    fn try_recv(&mut self) -> Result<Option<WorkerEvent>> {
        Ok(None)
    }
}

#[derive(Default)]
pub struct NullControlChannelFactory;

impl ControlChannelFactory for NullControlChannelFactory {
    fn create(&self, _child: &mut Child) -> Result<Box<dyn ControlChannel>> {
        Ok(Box::new(NullControlChannel))
    }
}

pub struct PipeControlChannelFactory;

impl ControlChannelFactory for PipeControlChannelFactory {
    fn create(&self, child: &mut Child) -> Result<Box<dyn ControlChannel>> {
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Process("worker stdin is not piped".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Process("worker stdout is not piped".into()))?;
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = stdout;
            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 8192];
            loop {
                match reader.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(read) => {
                        buffer.extend_from_slice(&chunk[..read]);
                        loop {
                            match ControlFrameCodec::decode(&mut buffer) {
                                Ok(Some((FrameKind::Ready, _))) => {
                                    let _ = sender.send(WorkerEvent::Ready);
                                }
                                Ok(Some((FrameKind::StartupFailed, payload))) => {
                                    let message = String::from_utf8_lossy(&payload).into_owned();
                                    let _ = sender.send(WorkerEvent::StartupFailed(message));
                                }
                                Ok(Some((_kind, _payload))) => {}
                                Ok(None) => break,
                                Err(error) => {
                                    let _ =
                                        sender.send(WorkerEvent::StartupFailed(error.to_string()));
                                    return;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(WorkerEvent::StartupFailed(error.to_string()));
                        return;
                    }
                }
            }
            let _ = sender.send(WorkerEvent::ControlChannelEof);
        });
        Ok(Box::new(PipeControlChannel { stdin, receiver }))
    }
}

struct PipeControlChannel {
    stdin: ChildStdin,
    receiver: mpsc::Receiver<WorkerEvent>,
}

impl ControlChannel for PipeControlChannel {
    fn send(&mut self, command: WorkerCommand) -> Result<()> {
        match command {
            WorkerCommand::Configure { snapshot, metadata } => {
                let config = toml::to_string(&snapshot)
                    .map_err(|error| Error::Process(error.to_string()))?;
                let metadata = toml::to_string(&metadata)
                    .map_err(|error| Error::Process(error.to_string()))?;
                let config_frame = ControlFrameCodec::encode(FrameKind::Config, config.as_bytes())?;
                let metadata_frame =
                    ControlFrameCodec::encode(FrameKind::Metadata, metadata.as_bytes())?;
                self.stdin
                    .write_all(&config_frame)
                    .and_then(|_| self.stdin.write_all(&metadata_frame))
                    .map_err(|error| Error::Process(error.to_string()))?;
            }
            WorkerCommand::Quit => self
                .stdin
                .write_all(&ControlFrameCodec::encode(FrameKind::Quit, &[])?)
                .map_err(|error| Error::Process(error.to_string()))?,
            WorkerCommand::Terminate => self
                .stdin
                .write_all(&ControlFrameCodec::encode(FrameKind::Terminate, &[])?)
                .map_err(|error| Error::Process(error.to_string()))?,
        }
        self.stdin
            .flush()
            .map_err(|error| Error::Process(error.to_string()))
    }

    fn try_recv(&mut self) -> Result<Option<WorkerEvent>> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Ok(Some(WorkerEvent::ControlChannelEof)),
        }
    }
}

pub struct CommandWorkerFactory {
    transport: Arc<dyn ControlChannelFactory>,
    process_group: bool,
}

#[cfg(unix)]
fn set_current_process_group() -> io::Result<()> {
    let result = unsafe { libc::setpgid(0, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn signal_process_group(pgid: libc::pid_t, signal: i32) -> io::Result<()> {
    let result = unsafe { libc::kill(-pgid, signal) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

impl Default for CommandWorkerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandWorkerFactory {
    pub fn new() -> Self {
        Self {
            transport: Arc::new(PipeControlChannelFactory),
            process_group: false,
        }
    }

    pub fn with_transport(transport: Arc<dyn ControlChannelFactory>) -> Self {
        Self {
            transport,
            process_group: false,
        }
    }

    pub fn with_process_group(mut self) -> Self {
        self.process_group = true;
        self
    }
}

impl WorkerFactory for CommandWorkerFactory {
    fn spawn(&self, request: WorkerSpawnRequest) -> Result<Box<dyn WorkerHandle>> {
        let mut command = Command::new(&request.config.worker.program);
        command
            .args(&request.config.worker.args)
            .envs(&request.config.worker.env)
            .env("AEG_GENERATION_ID", request.generation_id.to_string())
            .env("AEG_WORKER_ID", request.worker_id.to_string())
            .env(
                "AEG_LISTENER_FDS",
                request
                    .inherited_listener_fds
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        if let Some(entrypoint) = request.entrypoint {
            command.env("AEG_WORKER_ENTRYPOINT", entrypoint);
        }
        #[cfg(unix)]
        if self.process_group {
            use std::os::unix::process::CommandExt;
            unsafe {
                command.pre_exec(set_current_process_group);
            }
        }
        let mut child = command
            .spawn()
            .map_err(|error| Error::Process(error.to_string()))?;
        let channel = match self.transport.create(&mut child) {
            Ok(channel) => channel,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };
        Ok(Box::new(CommandWorkerHandle {
            worker_id: request.worker_id,
            child,
            channel,
            process_group: self.process_group,
        }))
    }
}

struct CommandWorkerHandle {
    worker_id: WorkerId,
    child: Child,
    channel: Box<dyn ControlChannel>,
    process_group: bool,
}

impl WorkerHandle for CommandWorkerHandle {
    fn worker_id(&self) -> WorkerId {
        self.worker_id
    }
    fn channel(&mut self) -> &mut dyn ControlChannel {
        self.channel.as_mut()
    }

    fn poll_exit(&mut self) -> Result<Option<ExitStatus>> {
        let Some(status) = self
            .child
            .try_wait()
            .map_err(|error| Error::Process(error.to_string()))?
        else {
            return Ok(None);
        };
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        Ok(Some(ExitStatus {
            code: status.code(),
            #[cfg(unix)]
            signal: status.signal(),
            #[cfg(not(unix))]
            signal: None,
            control_channel_eof: false,
        }))
    }

    fn terminate(&mut self) -> Result<()> {
        self.channel.send(WorkerCommand::Terminate)
    }

    fn kill(&mut self) -> Result<()> {
        #[cfg(unix)]
        if self.process_group {
            match signal_process_group(self.child.id() as libc::pid_t, libc::SIGKILL) {
                Ok(()) => return Ok(()),
                Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(()),
                Err(error) => return Err(Error::Process(error.to_string())),
            }
        }
        self.child
            .kill()
            .map_err(|error| Error::Process(error.to_string()))
    }
}

pub trait JitterSource: Send {
    fn ratio(&mut self) -> f64;
}

#[derive(Default)]
pub struct RandomJitter;

impl JitterSource for RandomJitter {
    fn ratio(&mut self) -> f64 {
        rand::rng().random_range(0.0..=1.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestartDecision {
    ExpectedStop,
    Disabled,
    Restart { delay: Duration },
    Failed,
}

pub struct RestartPolicy {
    config: RestartConfig,
    jitter: Box<dyn JitterSource>,
    runtime_restarts: VecDeque<Instant>,
    startup_failures: u32,
    backoff_attempt: u32,
    stable_since: Option<Instant>,
    disabled: bool,
}

impl RestartPolicy {
    pub fn new(config: RestartConfig, jitter: Box<dyn JitterSource>) -> Self {
        Self {
            config,
            jitter,
            runtime_restarts: VecDeque::new(),
            startup_failures: 0,
            backoff_attempt: 0,
            stable_since: None,
            disabled: false,
        }
    }

    pub fn mark_ready(&mut self, now: Instant) {
        self.stable_since = Some(now);
        self.startup_failures = 0;
    }

    pub fn disable_restarts(&mut self) {
        self.disabled = true;
    }

    pub fn on_exit(
        &mut self,
        now: Instant,
        stopping: bool,
        ready: bool,
        _status: ExitStatus,
    ) -> RestartDecision {
        if stopping {
            return RestartDecision::ExpectedStop;
        }
        if self.disabled {
            return RestartDecision::Disabled;
        }
        if ready {
            if self.stable_since.is_some_and(|since| {
                now.saturating_duration_since(since)
                    >= Duration::from_secs(self.config.stable_after_secs)
            }) {
                self.runtime_restarts.clear();
                self.backoff_attempt = 0;
            }
            let window = Duration::from_secs(self.config.window_secs);
            while self
                .runtime_restarts
                .front()
                .is_some_and(|time| now.saturating_duration_since(*time) > window)
            {
                self.runtime_restarts.pop_front();
            }
            if self.runtime_restarts.len() >= self.config.max_restarts as usize {
                return RestartDecision::Failed;
            }
            self.runtime_restarts.push_back(now);
        } else {
            self.startup_failures += 1;
            if self.startup_failures >= self.config.startup_failures {
                return RestartDecision::Failed;
            }
        }
        let delay = self.backoff_delay();
        self.backoff_attempt = self.backoff_attempt.saturating_add(1);
        RestartDecision::Restart { delay }
    }

    fn backoff_delay(&mut self) -> Duration {
        let base = Duration::from_millis(self.config.backoff_initial_ms)
            .saturating_mul(2u32.saturating_pow(self.backoff_attempt));
        let cap = Duration::from_secs(self.config.backoff_max_secs);
        let capped = base.min(cap);
        let ratio = self.jitter.ratio().clamp(0.0, 1.0);
        let jitter = f64::from(self.config.jitter_ratio_percent) / 100.0;
        let factor = 1.0 + ((ratio * 2.0) - 1.0) * jitter;
        capped.mul_f64(factor)
    }
}

pub trait ModuleRegistryFactory: Send + Sync {
    fn build(&self, snapshot: &ConfigSnapshot) -> Result<ModuleRegistry>;

    fn worker_entrypoint(&self) -> Option<String> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GenerationState {
    Starting,
    Ready,
    Active,
    Degraded,
    Draining,
    Retired,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProcessEvent {
    GenerationReady(GenerationId),
    ReloadRolledBack(GenerationId),
    GenerationRetired(GenerationId),
    WorkerRestartScheduled {
        generation_id: GenerationId,
        worker_id: WorkerId,
        delay: Duration,
    },
    WorkerFailed {
        generation_id: GenerationId,
        worker_id: WorkerId,
    },
}

struct WorkerSlot {
    handle: Box<dyn WorkerHandle>,
    ready: bool,
    stopping: bool,
    exited: bool,
    restart_due: Option<Instant>,
    startup_deadline: Option<Instant>,
    restart: RestartPolicy,
}

struct Generation {
    id: GenerationId,
    state: GenerationState,
    snapshot: ConfigSnapshot,
    cycle: ModuleCycle,
    workers: Vec<WorkerSlot>,
    ready_deadline: Instant,
    drain_deadline: Option<Instant>,
    kill_deadline: Option<Instant>,
    terminate_sent: bool,
    kill_sent: bool,
}

pub struct GenerationManager<F: WorkerFactory> {
    factory: F,
    registry_factory: Box<dyn ModuleRegistryFactory>,
    active: Option<Generation>,
    pending: Option<Generation>,
    draining: Vec<Generation>,
    next_generation_id: GenerationId,
    shutdown: bool,
}

impl<F: WorkerFactory> GenerationManager<F> {
    pub fn new(
        factory: F,
        initial: ConfigSnapshot,
        registry_factory: Box<dyn ModuleRegistryFactory>,
    ) -> Result<Self> {
        initial.validate()?;
        let mut manager = Self {
            factory,
            registry_factory,
            active: None,
            pending: None,
            draining: Vec::new(),
            next_generation_id: 1,
            shutdown: false,
        };
        manager.pending = Some(manager.start_generation(initial)?);
        Ok(manager)
    }

    /// Construct the first generation from an already initialized module cycle.
    ///
    /// The binary uses this entrypoint so the master can inspect module-owned
    /// listener declarations and bind sockets only after `init_module` has
    /// explicitly registered them.
    pub fn new_with_cycle(
        factory: F,
        initial: ConfigSnapshot,
        registry_factory: Box<dyn ModuleRegistryFactory>,
        mut cycle: ModuleCycle,
    ) -> Result<Self> {
        initial.validate()?;
        if let Err(error) = cycle.validate_listener_config(&initial.listeners) {
            let _ = cycle.exit_master();
            return Err(error);
        }
        let mut manager = Self {
            factory,
            registry_factory,
            active: None,
            pending: None,
            draining: Vec::new(),
            next_generation_id: 1,
            shutdown: false,
        };
        let id = manager.next_generation_id;
        manager.next_generation_id += 1;
        manager.pending = Some(manager.start_generation_with_cycle(initial, cycle, id)?);
        Ok(manager)
    }

    pub fn active_generation(&self) -> Option<GenerationId> {
        self.active.as_ref().map(|generation| generation.id)
    }

    pub fn reload(&mut self, snapshot: ConfigSnapshot) -> Result<GenerationId> {
        snapshot.validate()?;
        if self.shutdown {
            return Err(Error::Process("manager is shutting down".into()));
        }
        if self.draining.len() >= snapshot.generation.max_draining {
            return Err(Error::Process(
                "maximum draining generations reached".into(),
            ));
        }
        if let Some(mut pending) = self.pending.take() {
            self.abort_generation(&mut pending)?;
        }
        let generation = self.start_generation(snapshot)?;
        let id = generation.id;
        self.pending = Some(generation);
        Ok(id)
    }

    pub fn tick(&mut self, now: Instant) -> Result<Vec<ProcessEvent>> {
        let mut events = Vec::new();
        self.poll_pending(now, &mut events)?;
        self.poll_active(now, &mut events)?;
        self.poll_draining(now, &mut events)?;
        Ok(events)
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.shutdown = true;
        let mut errors = Vec::new();
        if let Some(mut pending) = self.pending.take() {
            if let Err(error) = self.abort_generation(&mut pending) {
                errors.push(error.to_string());
            }
        }
        if let Some(mut active) = self.active.take() {
            if let Err(error) = mark_draining(&mut active, Instant::now()) {
                errors.push(error.to_string());
            }
            self.draining.push(active);
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Process(errors.join("; ")))
        }
    }

    pub fn force_shutdown(&mut self) -> Result<()> {
        self.shutdown = true;
        let mut errors = Vec::new();
        if let Some(mut pending) = self.pending.take() {
            if let Err(error) = self.abort_generation(&mut pending) {
                errors.push(error.to_string());
            }
        }
        if let Some(mut active) = self.active.take() {
            if let Err(error) = self.abort_generation(&mut active) {
                errors.push(error.to_string());
            }
        }
        let draining = std::mem::take(&mut self.draining);
        for mut generation in draining {
            if let Err(error) = self.abort_generation(&mut generation) {
                errors.push(error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Process(errors.join("; ")))
        }
    }

    fn start_generation(&mut self, snapshot: ConfigSnapshot) -> Result<Generation> {
        let id = self.next_generation_id;
        self.next_generation_id += 1;
        let registry = self.registry_factory.build(&snapshot)?;
        let mut cycle = registry.build_cycle()?;
        if let Err(error) = cycle.init_module() {
            let _ = cycle.exit_master();
            return Err(error);
        }
        if let Err(error) = cycle.validate_listener_config(&snapshot.listeners) {
            let _ = cycle.exit_master();
            return Err(error);
        }
        self.start_generation_with_cycle(snapshot, cycle, id)
    }

    fn start_generation_with_cycle(
        &mut self,
        snapshot: ConfigSnapshot,
        mut cycle: ModuleCycle,
        id: GenerationId,
    ) -> Result<Generation> {
        let started_at = Instant::now();
        let mut workers: Vec<WorkerSlot> = Vec::with_capacity(snapshot.worker.count as usize);
        let entrypoint = self.registry_factory.worker_entrypoint();
        for worker_id in 0..snapshot.worker.count {
            let request = WorkerSpawnRequest {
                generation_id: id,
                worker_id,
                config: snapshot.clone(),
                entrypoint: entrypoint.clone(),
                inherited_listener_fds: snapshot.worker.listener_fds.clone(),
            };
            let metadata = request.metadata();
            let mut handle = match self.factory.spawn(request) {
                Ok(handle) => handle,
                Err(error) => {
                    for slot in &mut workers {
                        let _ = slot.handle.kill();
                    }
                    let _ = cycle.exit_master();
                    return Err(error);
                }
            };
            if let Err(error) = handle.channel().send(WorkerCommand::Configure {
                snapshot: Box::new(snapshot.clone()),
                metadata,
            }) {
                let _ = handle.kill();
                for slot in &mut workers {
                    let _ = slot.handle.kill();
                }
                let _ = cycle.exit_master();
                return Err(error);
            }
            workers.push(WorkerSlot {
                handle,
                ready: false,
                stopping: false,
                exited: false,
                restart_due: None,
                startup_deadline: None,
                restart: RestartPolicy::new(snapshot.restart.clone(), Box::new(RandomJitter)),
            });
        }
        Ok(Generation {
            id,
            state: GenerationState::Starting,
            snapshot: snapshot.clone(),
            cycle,
            workers,
            ready_deadline: started_at
                + Duration::from_secs(snapshot.generation.ready_timeout_secs),
            drain_deadline: None,
            kill_deadline: None,
            terminate_sent: false,
            kill_sent: false,
        })
    }

    fn poll_pending(&mut self, now: Instant, events: &mut Vec<ProcessEvent>) -> Result<()> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(());
        };
        let mut exited_before_ready = false;
        for slot in &mut pending.workers {
            while let Some(event) = slot.handle.channel().try_recv()? {
                if event == WorkerEvent::Ready && !slot.ready {
                    slot.ready = true;
                    slot.restart.mark_ready(now);
                } else if event == WorkerEvent::ControlChannelEof {
                    slot.exited = true;
                    let _ = slot.restart.on_exit(
                        now,
                        slot.stopping,
                        false,
                        ExitStatus {
                            code: None,
                            signal: None,
                            control_channel_eof: true,
                        },
                    );
                    exited_before_ready = true;
                }
            }
            if !slot.exited && slot.handle.poll_exit()?.is_some() {
                slot.exited = true;
                let _ = slot
                    .restart
                    .on_exit(now, slot.stopping, false, ExitStatus::code(1));
                exited_before_ready = true;
            }
        }
        if exited_before_ready {
            let id = pending.id;
            pending.state = GenerationState::Failed;
            let mut failed = self.pending.take().expect("pending generation exists");
            self.abort_generation(&mut failed)?;
            events.push(ProcessEvent::ReloadRolledBack(id));
            return Ok(());
        }
        let ready_count = pending.workers.iter().filter(|slot| slot.ready).count() as u32;
        if ready_count >= pending.snapshot.generation.min_ready {
            pending.state = GenerationState::Ready;
        } else if now >= pending.ready_deadline {
            let id = pending.id;
            let mut failed = self.pending.take().expect("pending generation exists");
            self.abort_generation(&mut failed)?;
            events.push(ProcessEvent::ReloadRolledBack(id));
            return Ok(());
        } else {
            return Ok(());
        }

        let mut next = self.pending.take().expect("pending generation exists");
        next.state = GenerationState::Active;
        let id = next.id;
        if let Some(mut old) = self.active.take() {
            let drain_result = mark_draining(&mut old, now);
            self.draining.push(old);
            drain_result?;
        }
        self.active = Some(next);
        events.push(ProcessEvent::GenerationReady(id));
        Ok(())
    }

    fn poll_active(&mut self, now: Instant, events: &mut Vec<ProcessEvent>) -> Result<()> {
        let Some(mut active) = self.active.take() else {
            return Ok(());
        };
        for slot in &mut active.workers {
            if slot.exited {
                let Some(restart_due) = slot.restart_due else {
                    continue;
                };
                if self.shutdown || now < restart_due {
                    continue;
                }
                let worker_id = slot.handle.worker_id();
                let request = WorkerSpawnRequest {
                    generation_id: active.id,
                    worker_id,
                    config: active.snapshot.clone(),
                    entrypoint: self.registry_factory.worker_entrypoint(),
                    inherited_listener_fds: active.snapshot.worker.listener_fds.clone(),
                };
                let metadata = request.metadata();
                match self.factory.spawn(request) {
                    Ok(mut handle) => {
                        if handle
                            .channel()
                            .send(WorkerCommand::Configure {
                                snapshot: Box::new(active.snapshot.clone()),
                                metadata,
                            })
                            .is_err()
                        {
                            let _ = handle.kill();
                            slot.restart_due = None;
                            active.state = GenerationState::Degraded;
                            events.push(ProcessEvent::WorkerFailed {
                                generation_id: active.id,
                                worker_id,
                            });
                            continue;
                        }
                        slot.handle = handle;
                        slot.ready = false;
                        slot.stopping = false;
                        slot.exited = false;
                        slot.restart_due = None;
                        slot.startup_deadline = Some(
                            now + Duration::from_secs(
                                active.snapshot.generation.ready_timeout_secs,
                            ),
                        );
                    }
                    Err(_) => {
                        slot.restart_due = None;
                        active.state = GenerationState::Degraded;
                        events.push(ProcessEvent::WorkerFailed {
                            generation_id: active.id,
                            worker_id,
                        });
                    }
                }
                continue;
            }
            if !slot.ready
                && slot
                    .startup_deadline
                    .is_some_and(|deadline| now >= deadline)
            {
                let worker_id = slot.handle.worker_id();
                let _ = slot.handle.kill();
                slot.exited = true;
                slot.startup_deadline = None;
                match slot.restart.on_exit(
                    now,
                    false,
                    false,
                    ExitStatus {
                        code: None,
                        signal: None,
                        control_channel_eof: false,
                    },
                ) {
                    RestartDecision::Restart { delay } => {
                        slot.restart_due = Some(now + delay);
                        events.push(ProcessEvent::WorkerRestartScheduled {
                            generation_id: active.id,
                            worker_id,
                            delay,
                        });
                    }
                    RestartDecision::Failed => {
                        slot.restart_due = None;
                        active.state = GenerationState::Degraded;
                        events.push(ProcessEvent::WorkerFailed {
                            generation_id: active.id,
                            worker_id,
                        });
                    }
                    RestartDecision::ExpectedStop | RestartDecision::Disabled => {}
                }
                continue;
            }
            let mut channel_eof = false;
            while let Some(event) = slot.handle.channel().try_recv()? {
                match event {
                    WorkerEvent::Ready if !slot.ready => {
                        slot.ready = true;
                        slot.startup_deadline = None;
                        slot.restart.mark_ready(now);
                    }
                    WorkerEvent::ControlChannelEof => channel_eof = true,
                    WorkerEvent::StartupFailed(_) => channel_eof = true,
                    WorkerEvent::Ready => {}
                }
            }
            let exit_status = if channel_eof {
                Some(ExitStatus {
                    code: None,
                    signal: None,
                    control_channel_eof: true,
                })
            } else {
                slot.handle.poll_exit()?
            };
            if let Some(status) = exit_status {
                slot.exited = true;
                match slot.restart.on_exit(now, slot.stopping, slot.ready, status) {
                    RestartDecision::Restart { delay } => {
                        slot.restart_due = Some(now + delay);
                        events.push(ProcessEvent::WorkerRestartScheduled {
                            generation_id: active.id,
                            worker_id: slot.handle.worker_id(),
                            delay,
                        })
                    }
                    RestartDecision::Failed => {
                        slot.restart_due = None;
                        active.state = GenerationState::Degraded;
                        events.push(ProcessEvent::WorkerFailed {
                            generation_id: active.id,
                            worker_id: slot.handle.worker_id(),
                        });
                    }
                    RestartDecision::ExpectedStop | RestartDecision::Disabled => {}
                }
            }
        }
        let all_slots_failed = active
            .workers
            .iter()
            .all(|slot| slot.exited && slot.restart_due.is_none());
        if all_slots_failed
            && active.state == GenerationState::Degraded
            && active.snapshot.generation.on_all_failed == OnAllFailed::Exit
        {
            let generation_id = active.id;
            active.state = GenerationState::Failed;
            self.active = Some(active);
            return Err(Error::Process(format!(
                "all workers failed in generation {generation_id}"
            )));
        }
        self.active = Some(active);
        Ok(())
    }

    fn poll_draining(&mut self, now: Instant, events: &mut Vec<ProcessEvent>) -> Result<()> {
        let mut retired = Vec::new();
        let mut errors = Vec::new();
        for (index, generation) in self.draining.iter_mut().enumerate() {
            for slot in &mut generation.workers {
                if !slot.exited {
                    match slot.handle.poll_exit() {
                        Ok(Some(_)) => slot.exited = true,
                        Ok(None) => {}
                        Err(error) => errors.push(error.to_string()),
                    }
                }
            }
            if generation.workers.iter().all(|slot| slot.exited) {
                if let Err(error) = generation.cycle.exit_master() {
                    errors.push(error.to_string());
                }
                generation.state = GenerationState::Retired;
                retired.push((index, generation.id));
                continue;
            }
            if !generation.terminate_sent
                && generation
                    .drain_deadline
                    .is_some_and(|deadline| now >= deadline)
            {
                for slot in &mut generation.workers {
                    if !slot.exited {
                        if let Err(error) = slot.handle.terminate() {
                            errors.push(error.to_string());
                        }
                    }
                }
                generation.terminate_sent = true;
                generation.kill_deadline = Some(
                    now + Duration::from_secs(generation.snapshot.generation.kill_timeout_secs),
                );
            } else if generation.terminate_sent
                && !generation.kill_sent
                && generation
                    .kill_deadline
                    .is_some_and(|deadline| now >= deadline)
            {
                for slot in &mut generation.workers {
                    if !slot.exited {
                        if let Err(error) = slot.handle.kill() {
                            errors.push(error.to_string());
                        }
                    }
                }
                generation.kill_sent = true;
            }
        }
        for (index, id) in retired.into_iter().rev() {
            self.draining.remove(index);
            events.push(ProcessEvent::GenerationRetired(id));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Process(errors.join("; ")))
        }
    }

    fn abort_generation(&mut self, generation: &mut Generation) -> Result<()> {
        let mut errors = Vec::new();
        for slot in &mut generation.workers {
            if !slot.exited {
                slot.stopping = true;
                if let Err(error) = slot.handle.kill() {
                    errors.push(error.to_string());
                }
            }
        }
        if let Err(error) = generation.cycle.exit_master() {
            errors.push(error.to_string());
        }
        generation.state = GenerationState::Failed;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Process(errors.join("; ")))
        }
    }
}

fn mark_draining(generation: &mut Generation, now: Instant) -> Result<()> {
    generation.state = GenerationState::Draining;
    let mut errors = Vec::new();
    for slot in &mut generation.workers {
        if !slot.exited {
            slot.stopping = true;
            if let Err(error) = slot.handle.channel().send(WorkerCommand::Quit) {
                errors.push(error.to_string());
            }
        }
    }
    let timeout = Duration::from_secs(generation.snapshot.generation.drain_timeout_secs);
    generation.drain_deadline = (timeout != Duration::ZERO).then_some(now + timeout);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::Process(errors.join("; ")))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::config::RestartConfig;

    use super::{ExitStatus, JitterSource, RestartDecision, RestartPolicy};

    struct FixedJitter(f64);

    impl JitterSource for FixedJitter {
        fn ratio(&mut self) -> f64 {
            self.0
        }
    }

    fn config() -> RestartConfig {
        RestartConfig {
            max_restarts: 5,
            window_secs: 60,
            backoff_initial_ms: 100,
            backoff_max_secs: 10,
            jitter_ratio_percent: 20,
            stable_after_secs: 60,
            startup_failures: 3,
        }
    }

    #[test]
    fn expected_stopping_exit_never_restarts() {
        let now = Instant::now();
        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(0.5)));
        assert_eq!(
            policy.on_exit(now, true, false, ExitStatus::code(0)),
            RestartDecision::ExpectedStop
        );
    }

    #[test]
    fn unmarked_exit_code_zero_is_restarted() {
        let now = Instant::now();
        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(0.5)));
        assert_eq!(
            policy.on_exit(now, false, true, ExitStatus::code(0)),
            RestartDecision::Restart {
                delay: Duration::from_millis(100)
            }
        );
    }

    #[test]
    fn startup_failure_budget_permanently_fails_after_three_attempts() {
        let now = Instant::now();
        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(0.5)));
        assert!(matches!(
            policy.on_exit(now, false, false, ExitStatus::code(1)),
            RestartDecision::Restart { .. }
        ));
        assert!(matches!(
            policy.on_exit(now, false, false, ExitStatus::code(1)),
            RestartDecision::Restart { .. }
        ));
        assert_eq!(
            policy.on_exit(now, false, false, ExitStatus::code(1)),
            RestartDecision::Failed
        );
    }

    #[test]
    fn runtime_restart_budget_is_sliding_and_stable_workers_reset_it() {
        let now = Instant::now();
        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(0.0)));
        for _ in 0..5 {
            assert!(matches!(
                policy.on_exit(now, false, true, ExitStatus::code(1)),
                RestartDecision::Restart { .. }
            ));
        }
        assert_eq!(
            policy.on_exit(now, false, true, ExitStatus::code(1)),
            RestartDecision::Failed
        );

        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(0.5)));
        policy.mark_ready(now);
        assert!(
            matches!(policy.on_exit(now + Duration::from_secs(61), false, true, ExitStatus::code(1)), RestartDecision::Restart { delay } if delay == Duration::from_millis(100))
        );
    }

    #[test]
    fn backoff_is_capped_and_jittered() {
        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(1.0)));
        let now = Instant::now();
        for _ in 0..5 {
            let decision = policy.on_exit(now, false, true, ExitStatus::code(1));
            assert!(matches!(decision, RestartDecision::Restart { .. }));
        }
        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(1.0)));
        let decision = policy.on_exit(now, false, true, ExitStatus::code(1));
        assert_eq!(
            decision,
            RestartDecision::Restart {
                delay: Duration::from_millis(120)
            }
        );
    }

    #[test]
    fn global_shutdown_disables_restart() {
        let now = Instant::now();
        let mut policy = RestartPolicy::new(config(), Box::new(FixedJitter(0.0)));
        policy.disable_restarts();
        assert_eq!(
            policy.on_exit(now, false, true, ExitStatus::code(1)),
            RestartDecision::Disabled
        );
    }
}

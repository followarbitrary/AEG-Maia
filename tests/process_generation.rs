use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aeg_maia::{
    config::ConfigSnapshot,
    module::{Module, ModuleContext, ModuleRegistry},
    process::{
        ControlChannel, ExitStatus, GenerationManager, ModuleRegistryFactory, ProcessEvent,
        ProcessKind, WorkerCommand, WorkerEvent, WorkerFactory, WorkerHandle, WorkerSpawnRequest,
    },
    Error, Result,
};

#[derive(Clone)]
struct FakeWorkerState {
    events: VecDeque<WorkerEvent>,
    commands: Vec<WorkerCommand>,
    exit: Option<ExitStatus>,
    killed: bool,
}

#[test]
fn command_factory_exposes_explicit_helper_process_kind() {
    assert_eq!(
        aeg_maia::CommandWorkerFactory::new().process_kind(),
        ProcessKind::Worker
    );
    assert_eq!(
        aeg_maia::CommandWorkerFactory::helper().process_kind(),
        ProcessKind::Helper
    );
    assert_eq!(ProcessKind::Helper.as_str(), "helper");
}

type SharedWorkerState = Arc<Mutex<FakeWorkerState>>;
type WorkerRecords = Arc<Mutex<Vec<(u64, SharedWorkerState)>>>;

struct FakeChannel {
    state: Arc<Mutex<FakeWorkerState>>,
}

impl ControlChannel for FakeChannel {
    fn send(&mut self, command: WorkerCommand) -> Result<()> {
        self.state.lock().unwrap().commands.push(command);
        Ok(())
    }

    fn try_recv(&mut self) -> Result<Option<WorkerEvent>> {
        Ok(self.state.lock().unwrap().events.pop_front())
    }
}

struct FakeWorker {
    worker_id: u32,
    state: Arc<Mutex<FakeWorkerState>>,
    channel: FakeChannel,
}

impl WorkerHandle for FakeWorker {
    fn worker_id(&self) -> u32 {
        self.worker_id
    }
    fn channel(&mut self) -> &mut dyn ControlChannel {
        &mut self.channel
    }

    fn poll_exit(&mut self) -> Result<Option<ExitStatus>> {
        let mut state = self.state.lock().unwrap();
        if state.killed {
            state.killed = false;
            return Ok(Some(ExitStatus::code(137)));
        }
        Ok(state.exit.take())
    }

    fn terminate(&mut self) -> Result<()> {
        self.channel.send(WorkerCommand::Terminate)
    }

    fn kill(&mut self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.killed = true;
        Ok(())
    }
}

struct FakeFactory {
    ready: Arc<Mutex<VecDeque<bool>>>,
    workers: WorkerRecords,
}

impl WorkerFactory for FakeFactory {
    fn spawn(&self, request: WorkerSpawnRequest) -> Result<Box<dyn WorkerHandle>> {
        let should_ready = self.ready.lock().unwrap().pop_front().unwrap_or(true);
        let state = Arc::new(Mutex::new(FakeWorkerState {
            events: if should_ready {
                VecDeque::from([WorkerEvent::Ready])
            } else {
                VecDeque::new()
            },
            commands: Vec::new(),
            exit: None,
            killed: false,
        }));
        self.workers
            .lock()
            .unwrap()
            .push((request.generation_id, Arc::clone(&state)));
        Ok(Box::new(FakeWorker {
            worker_id: request.worker_id,
            state: Arc::clone(&state),
            channel: FakeChannel { state },
        }))
    }
}

struct TestModule {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
    fail_init: bool,
}

impl Module for TestModule {
    fn name(&self) -> &str {
        self.name
    }
    fn dependencies(&self) -> &[&str] {
        &[]
    }
    fn init_module(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("init:{}", self.name));
        if self.fail_init {
            Err(Error::Module("init failed".into()))
        } else {
            Ok(())
        }
    }
    fn exit_master(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("exit:{}", self.name));
        Ok(())
    }
}

struct TestRegistryFactory {
    events: Arc<Mutex<Vec<String>>>,
    fail_init: Arc<Mutex<bool>>,
}

impl ModuleRegistryFactory for TestRegistryFactory {
    fn build(&self, _snapshot: &ConfigSnapshot) -> Result<ModuleRegistry> {
        let mut registry = ModuleRegistry::new();
        registry.register(Box::new(TestModule {
            name: "test",
            events: Arc::clone(&self.events),
            fail_init: *self.fail_init.lock().unwrap(),
        }))?;
        Ok(registry)
    }
}

fn snapshot(count: u32, min_ready: u32) -> ConfigSnapshot {
    ConfigSnapshot::from_toml(&format!(
        "[worker]\nprogram='/bin/worker'\ncount={count}\n[generation]\nmin_ready={min_ready}\n"
    ))
    .unwrap()
}

fn manager(
    ready: Vec<bool>,
    events: Arc<Mutex<Vec<String>>>,
    fail_init: Arc<Mutex<bool>>,
    workers: WorkerRecords,
) -> GenerationManager<FakeFactory> {
    let factory = FakeFactory {
        ready: Arc::new(Mutex::new(ready.into_iter().collect())),
        workers,
    };
    GenerationManager::new(
        factory,
        snapshot(1, 1),
        Box::new(TestRegistryFactory { events, fail_init }),
    )
    .unwrap()
}

#[test]
fn generation_rejects_unregistered_listener_before_worker_spawn() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let factory = FakeFactory {
        ready: Arc::new(Mutex::new(VecDeque::from([true]))),
        workers: Arc::clone(&workers),
    };
    let snapshot = ConfigSnapshot::from_toml(
        "[worker]\nprogram='/bin/worker'\ncount=1\n[[listen]]\nname='missing'\naddress='127.0.0.1:8080'\n",
    )
    .unwrap();
    let result = GenerationManager::new(
        factory,
        snapshot,
        Box::new(TestRegistryFactory { events, fail_init }),
    );
    assert!(result.is_err());
    assert!(workers.lock().unwrap().is_empty());
}

#[test]
fn ready_handshake_is_required_before_active_cutover() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(vec![false, true], events, fail_init, workers);
    let now = Instant::now();

    assert!(manager.tick(now).unwrap().is_empty());
    assert_eq!(manager.active_generation(), None);
    manager.reload(snapshot(1, 1)).unwrap();
    assert!(manager
        .tick(now + Duration::from_millis(1))
        .unwrap()
        .contains(&ProcessEvent::GenerationReady(2)));
    assert_eq!(manager.active_generation(), Some(2));
}

#[test]
fn failed_init_or_ready_timeout_keeps_old_generation_active() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(
        vec![true, false],
        Arc::clone(&events),
        Arc::clone(&fail_init),
        Arc::clone(&workers),
    );
    let now = Instant::now();
    manager.tick(now).unwrap();
    assert_eq!(manager.active_generation(), Some(1));

    *fail_init.lock().unwrap() = true;
    assert!(manager.reload(snapshot(1, 1)).is_err());
    assert_eq!(manager.active_generation(), Some(1));
    *fail_init.lock().unwrap() = false;

    manager.reload(snapshot(1, 1)).unwrap();
    let events = manager.tick(now + Duration::from_secs(11)).unwrap();
    assert!(events.contains(&ProcessEvent::ReloadRolledBack(3)));
    assert_eq!(manager.active_generation(), Some(1));
}

#[test]
fn cutover_quits_old_generation_and_drains_then_retires_it() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(
        vec![true, true],
        Arc::clone(&events),
        fail_init,
        Arc::clone(&workers),
    );
    let now = Instant::now();
    manager.tick(now).unwrap();
    manager.reload(snapshot(1, 1)).unwrap();
    manager.tick(now + Duration::from_millis(1)).unwrap();
    assert_eq!(manager.active_generation(), Some(2));

    let old_state = workers.lock().unwrap()[0].1.clone();
    assert_eq!(
        old_state.lock().unwrap().commands,
        vec![
            WorkerCommand::Configure {
                snapshot: Box::new(snapshot(1, 1)),
                metadata: aeg_maia::WorkerMetadata {
                    generation_id: 1,
                    worker_id: 0,
                    listeners: Vec::new(),
                    listener_owners: Default::default(),
                    entrypoint: None,
                },
            },
            WorkerCommand::Quit
        ]
    );
    manager.tick(now + Duration::from_secs(31)).unwrap();
    assert_eq!(
        old_state.lock().unwrap().commands,
        vec![
            WorkerCommand::Configure {
                snapshot: Box::new(snapshot(1, 1)),
                metadata: aeg_maia::WorkerMetadata {
                    generation_id: 1,
                    worker_id: 0,
                    listeners: Vec::new(),
                    listener_owners: Default::default(),
                    entrypoint: None,
                },
            },
            WorkerCommand::Quit,
            WorkerCommand::Terminate,
        ]
    );
    manager.tick(now + Duration::from_secs(36)).unwrap();
    assert!(manager
        .tick(now + Duration::from_secs(37))
        .unwrap()
        .contains(&ProcessEvent::GenerationRetired(1)));
    assert!(events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "exit:test"));
}

#[test]
fn abnormal_exit_restarts_the_same_worker_slot_after_backoff() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(vec![true, true], events, fail_init, Arc::clone(&workers));
    let now = Instant::now();
    manager.tick(now).unwrap();
    let first_state = workers.lock().unwrap()[0].1.clone();
    first_state.lock().unwrap().exit = Some(ExitStatus::code(0));

    let events = manager.tick(now + Duration::from_millis(1)).unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        ProcessEvent::WorkerRestartScheduled {
            generation_id: 1,
            worker_id: 0,
            ..
        }
    )));
    assert_eq!(workers.lock().unwrap().len(), 1);

    manager.tick(now + Duration::from_secs(11)).unwrap();
    assert_eq!(workers.lock().unwrap().len(), 2);
}

#[test]
fn shutdown_moves_active_generation_into_draining_and_retires_cycle() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(
        vec![true],
        Arc::clone(&events),
        fail_init,
        Arc::clone(&workers),
    );
    let now = Instant::now();
    manager.tick(now).unwrap();
    manager.shutdown().unwrap();
    assert_eq!(manager.active_generation(), None);
    manager.tick(now + Duration::from_secs(31)).unwrap();
    manager.tick(now + Duration::from_secs(36)).unwrap();
    assert!(manager
        .tick(now + Duration::from_secs(37))
        .unwrap()
        .contains(&ProcessEvent::GenerationRetired(1)));
    assert!(events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "exit:test"));
}

#[test]
fn pending_worker_exit_is_classified_and_rolls_back_without_kill_error() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(vec![false], events, fail_init, Arc::clone(&workers));
    let now = Instant::now();
    workers.lock().unwrap()[0].1.lock().unwrap().exit = Some(ExitStatus::code(1));
    let result = manager.tick(now + Duration::from_millis(1)).unwrap();
    assert!(result.contains(&ProcessEvent::ReloadRolledBack(1)));
}

#[test]
fn control_channel_eof_is_an_abnormal_worker_exit() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(vec![true], events, fail_init, Arc::clone(&workers));
    let now = Instant::now();
    manager.tick(now).unwrap();
    workers.lock().unwrap()[0]
        .1
        .lock()
        .unwrap()
        .events
        .push_back(WorkerEvent::ControlChannelEof);
    let events = manager.tick(now + Duration::from_millis(1)).unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        ProcessEvent::WorkerRestartScheduled {
            generation_id: 1,
            worker_id: 0,
            ..
        }
    )));
}

#[test]
fn all_failed_exit_policy_returns_error_after_restart_budget_is_exhausted() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let factory = FakeFactory {
        ready: Arc::new(Mutex::new(VecDeque::from([true, true]))),
        workers: Arc::clone(&workers),
    };
    let snapshot = ConfigSnapshot::from_toml(
        "[worker]\nprogram='/bin/worker'\ncount=1\n[restart]\nmax_restarts=1\n[generation]\non_all_failed='exit'\n",
    )
    .unwrap();
    let mut manager = GenerationManager::new(
        factory,
        snapshot,
        Box::new(TestRegistryFactory { events, fail_init }),
    )
    .unwrap();
    let now = Instant::now();
    manager.tick(now).unwrap();
    workers.lock().unwrap()[0].1.lock().unwrap().exit = Some(ExitStatus::code(1));
    manager.tick(now + Duration::from_millis(1)).unwrap();
    manager.tick(now + Duration::from_secs(11)).unwrap();
    workers.lock().unwrap()[1].1.lock().unwrap().exit = Some(ExitStatus::code(1));
    assert!(manager.tick(now + Duration::from_secs(12)).is_err());
}

#[test]
fn restarted_worker_must_become_ready_before_its_startup_deadline() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_init = Arc::new(Mutex::new(false));
    let workers = Arc::new(Mutex::new(Vec::new()));
    let mut manager = manager(vec![true, false], events, fail_init, Arc::clone(&workers));
    let now = Instant::now();
    manager.tick(now).unwrap();
    workers.lock().unwrap()[0].1.lock().unwrap().exit = Some(ExitStatus::code(1));
    manager.tick(now + Duration::from_millis(1)).unwrap();
    manager.tick(now + Duration::from_secs(11)).unwrap();
    let events = manager.tick(now + Duration::from_secs(22)).unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        ProcessEvent::WorkerRestartScheduled {
            generation_id: 1,
            worker_id: 0,
            ..
        }
    )));
}

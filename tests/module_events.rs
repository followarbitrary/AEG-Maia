use std::sync::{Arc, Mutex};
use std::time::Duration;

use aeg_maia::{
    Error, EventInterest, ListenerClaims, Module, ModuleContext, ModuleEvent, ModuleRegistry,
    Result, WorkerContext, WorkerEventRegistry, WorkerModule, WorkerModuleFactory,
};
use mio::net::UnixStream;
use std::io::Write;

struct EventModule {
    events: Arc<Mutex<Vec<String>>>,
    source: Option<UnixStream>,
    fail_event: bool,
}

impl Module for EventModule {
    fn name(&self) -> &str {
        "event"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn init_module(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }

    fn exit_master(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }
}

impl WorkerModule for EventModule {
    fn init_process(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.events.lock().unwrap().push("init".into());
        let mut source = self.source.take().unwrap();
        let result = ctx.register_source(&mut source, EventInterest::readable());
        self.source = Some(source);
        assert!(result.is_err(), "init_process must not register events");
        Ok(())
    }

    fn register_events(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.events.lock().unwrap().push("register".into());
        let source = self.source.as_mut().unwrap();
        ctx.register_source(source, EventInterest::readable())?;
        Ok(())
    }

    fn start(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.events.lock().unwrap().push("start".into());
        Ok(())
    }

    fn on_event(
        &mut self,
        _event: ModuleEvent,
        _ctx: &mut aeg_maia::EventContext<'_>,
    ) -> Result<()> {
        self.events.lock().unwrap().push("event".into());
        if self.fail_event {
            Err(Error::Event("callback failed".into()))
        } else {
            Ok(())
        }
    }

    fn stop(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        Ok(())
    }

    fn exit_process(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        Ok(())
    }
}

struct Factory {
    events: Arc<Mutex<Vec<String>>>,
    source: Arc<Mutex<Option<UnixStream>>>,
    fail_event: bool,
}

impl WorkerModuleFactory for Factory {
    fn build(
        &self,
        _name: &str,
        _worker_id: u32,
        _snapshot: &aeg_maia::ConfigSnapshot,
    ) -> Result<Box<dyn WorkerModule>> {
        Ok(Box::new(EventModule {
            events: Arc::clone(&self.events),
            source: self.source.lock().unwrap().take(),
            fail_event: self.fail_event,
        }))
    }
}

fn worker(
    events: Arc<Mutex<Vec<String>>>,
    source: Arc<Mutex<Option<UnixStream>>>,
    fail_event: bool,
) -> aeg_maia::module::WorkerCycle {
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(EventModule {
            events: Arc::clone(&events),
            source: None,
            fail_event,
        }))
        .unwrap();
    let cycle = registry.build_cycle().unwrap();
    let snapshot =
        aeg_maia::ConfigSnapshot::from_toml("[worker]\nprogram='/bin/worker'\ncount=1\n").unwrap();
    cycle
        .build_worker_with_registrar(
            &Factory {
                events,
                source,
                fail_event,
            },
            0,
            &snapshot,
            Box::new(WorkerEventRegistry::new(ListenerClaims::default(), 8).unwrap()),
        )
        .unwrap()
}

#[test]
fn worker_runs_register_events_between_init_and_start() {
    let (source, mut writer) = UnixStream::pair().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let source = Arc::new(Mutex::new(Some(source)));
    let mut cycle = worker(Arc::clone(&events), source, false);

    cycle.init_process().unwrap();
    cycle.register_events().unwrap();
    cycle.validate_event_registrations().unwrap();
    cycle.start().unwrap();
    writer.write_all(&[1]).unwrap();
    let events_ready = cycle.poll_events(Some(Duration::from_millis(100))).unwrap();
    assert_eq!(events_ready.len(), 1);
    cycle.dispatch_event(events_ready[0]).unwrap();
    assert_eq!(
        &*events.lock().unwrap(),
        &["init", "register", "start", "event"]
    );
}

#[test]
fn callback_error_unregisters_only_the_failing_source() {
    let (_source, _writer) = UnixStream::pair().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let source = Arc::new(Mutex::new(Some(UnixStream::pair().unwrap().0)));
    let mut cycle = worker(Arc::clone(&events), source, true);
    cycle.init_process().unwrap();
    cycle.register_events().unwrap();
    cycle.validate_event_registrations().unwrap();
    cycle.start().unwrap();
    assert!(cycle
        .dispatch_event(ModuleEvent::new(
            aeg_maia::EventKey::from_raw(1),
            true,
            false,
            false,
            false,
        ))
        .is_ok());
}

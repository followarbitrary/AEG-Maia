use std::sync::{Arc, Mutex};

use aeg_maia::{
    config::ConfigSnapshot,
    module::{
        Module, ModuleContext, ModuleRegistry, WorkerContext, WorkerModule, WorkerModuleFactory,
    },
    Error, Result,
};

struct RecordingModule {
    name: &'static str,
    deps: &'static [&'static str],
    events: Arc<Mutex<Vec<String>>>,
    fail_init: bool,
}

impl Module for RecordingModule {
    fn name(&self) -> &str {
        self.name
    }
    fn dependencies(&self) -> &[&str] {
        self.deps
    }

    fn init_module(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("init:{}", self.name));
        if self.fail_init {
            Err(Error::Module(format!("{} failed", self.name)))
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

fn module(
    name: &'static str,
    deps: &'static [&'static str],
    events: &Arc<Mutex<Vec<String>>>,
) -> RecordingModule {
    RecordingModule {
        name,
        deps,
        events: Arc::clone(events),
        fail_init: false,
    }
}

#[test]
fn rejects_duplicate_missing_and_cyclic_graphs_before_hooks() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut duplicate = ModuleRegistry::new();
    duplicate
        .register(Box::new(module("a", &[], &events)))
        .unwrap();
    assert!(duplicate
        .register(Box::new(module("a", &[], &events)))
        .is_err());

    let mut missing = ModuleRegistry::new();
    missing
        .register(Box::new(module("a", &["missing"], &events)))
        .unwrap();
    let error = missing.validate().expect_err("missing dependency");
    assert!(error.to_string().contains("missing"));
    assert!(events.lock().unwrap().is_empty());

    let mut cyclic = ModuleRegistry::new();
    cyclic
        .register(Box::new(module("a", &["b"], &events)))
        .unwrap();
    cyclic
        .register(Box::new(module("b", &["c"], &events)))
        .unwrap();
    cyclic
        .register(Box::new(module("c", &["a"], &events)))
        .unwrap();
    let error = cyclic.validate().expect_err("cycle");
    assert!(error.to_string().contains("a -> b -> c -> a"));

    let mut duplicate_dependency = ModuleRegistry::new();
    duplicate_dependency
        .register(Box::new(module("a", &[], &events)))
        .unwrap();
    duplicate_dependency
        .register(Box::new(module("b", &["a", "a"], &events)))
        .unwrap();
    let error = duplicate_dependency
        .validate()
        .expect_err("duplicate dependency");
    assert!(error.to_string().contains("duplicate dependency"));
}

#[test]
fn orders_dependencies_then_lexicographic_names() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(module("z", &[], &events)))
        .unwrap();
    registry
        .register(Box::new(module("b", &["z"], &events)))
        .unwrap();
    registry
        .register(Box::new(module("a", &[], &events)))
        .unwrap();

    let order = registry.validate().unwrap();
    assert_eq!(order.names(), &["a", "z", "b"]);
}

#[test]
fn init_failure_unwinds_only_successful_modules_in_reverse_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(module("a", &[], &events)))
        .unwrap();
    let mut failing = module("b", &["a"], &events);
    failing.fail_init = true;
    registry.register(Box::new(failing)).unwrap();
    registry
        .register(Box::new(module("c", &["b"], &events)))
        .unwrap();

    let mut cycle = registry.build_cycle().unwrap();
    assert!(cycle.init_module().is_err());
    assert_eq!(&*events.lock().unwrap(), &["init:a", "init:b", "exit:a"]);
}

struct RecordingWorker {
    name: String,
    events: Arc<Mutex<Vec<String>>>,
    fail_start: bool,
}

impl WorkerModule for RecordingWorker {
    fn init_process(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("worker-init:{}", self.name));
        Ok(())
    }

    fn start(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("worker-start:{}", self.name));
        if self.fail_start {
            return Err(Error::Module(format!("{} start failed", self.name)));
        }
        Ok(())
    }

    fn stop(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("worker-stop:{}", self.name));
        Ok(())
    }

    fn exit_process(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("worker-exit:{}", self.name));
        Ok(())
    }
}

struct RecordingWorkerFactory {
    events: Arc<Mutex<Vec<String>>>,
    fail_start: bool,
}

impl WorkerModuleFactory for RecordingWorkerFactory {
    fn build(
        &self,
        name: &str,
        _worker_id: u32,
        _snapshot: &ConfigSnapshot,
    ) -> Result<Box<dyn WorkerModule>> {
        Ok(Box::new(RecordingWorker {
            name: name.to_owned(),
            events: Arc::clone(&self.events),
            fail_start: self.fail_start && name == "b",
        }))
    }
}

#[test]
fn worker_cycle_runs_hooks_in_the_same_dependency_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(module("b", &["a"], &events)))
        .unwrap();
    registry
        .register(Box::new(module("a", &[], &events)))
        .unwrap();
    let cycle = registry.build_cycle().unwrap();
    let factory = RecordingWorkerFactory {
        events: Arc::clone(&events),
        fail_start: false,
    };
    let snapshot = ConfigSnapshot::from_toml("[worker]\nprogram='/bin/worker'\ncount=1\n").unwrap();
    let mut worker = cycle.build_worker(&factory, 0, &snapshot).unwrap();

    worker.init_process().unwrap();
    worker.start().unwrap();
    worker.stop().unwrap();
    worker.exit_process().unwrap();

    assert_eq!(
        &*events.lock().unwrap(),
        &[
            "worker-init:a",
            "worker-init:b",
            "worker-start:a",
            "worker-start:b",
            "worker-stop:b",
            "worker-stop:a",
            "worker-exit:b",
            "worker-exit:a"
        ]
    );
}

#[test]
fn worker_start_failure_stops_only_started_modules_before_exit() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(module("a", &[], &events)))
        .unwrap();
    registry
        .register(Box::new(module("b", &["a"], &events)))
        .unwrap();
    let cycle = registry.build_cycle().unwrap();
    let factory = RecordingWorkerFactory {
        events: Arc::clone(&events),
        fail_start: true,
    };
    let snapshot = ConfigSnapshot::from_toml("[worker]\nprogram='/bin/worker'\ncount=1\n").unwrap();
    let mut worker = cycle.build_worker(&factory, 0, &snapshot).unwrap();

    worker.init_process().unwrap();
    assert!(worker.start().is_err());
    assert_eq!(
        &*events.lock().unwrap(),
        &[
            "worker-init:a",
            "worker-init:b",
            "worker-start:a",
            "worker-start:b",
            "worker-stop:a"
        ]
    );
    worker.exit_process().unwrap();
    assert_eq!(
        &*events.lock().unwrap(),
        &[
            "worker-init:a",
            "worker-init:b",
            "worker-start:a",
            "worker-start:b",
            "worker-stop:a",
            "worker-exit:b",
            "worker-exit:a"
        ]
    );
}

struct ProviderModule;

impl Module for ProviderModule {
    fn name(&self) -> &str {
        "provider"
    }
    fn dependencies(&self) -> &[&str] {
        &[]
    }
    fn init_module(&mut self, ctx: &mut ModuleContext) -> Result<()> {
        ctx.provide(Arc::new(42usize))
    }
    fn exit_master(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }
}

struct ConsumerModule {
    declared: bool,
}

impl Module for ConsumerModule {
    fn name(&self) -> &str {
        if self.declared {
            "consumer"
        } else {
            "bad-consumer"
        }
    }
    fn dependencies(&self) -> &[&str] {
        if self.declared {
            &["provider"]
        } else {
            &[]
        }
    }
    fn init_module(&mut self, ctx: &mut ModuleContext) -> Result<()> {
        ctx.require::<usize>("provider").map(|_| ())
    }
    fn exit_master(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }
}

#[test]
fn services_require_declared_provider_dependencies() {
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(ConsumerModule { declared: true }))
        .unwrap();
    registry.register(Box::new(ProviderModule)).unwrap();
    let mut cycle = registry.build_cycle().unwrap();
    cycle.init_module().unwrap();

    let mut missing = ModuleRegistry::new();
    missing
        .register(Box::new(ConsumerModule { declared: false }))
        .unwrap();
    let mut missing_cycle = missing.build_cycle().unwrap();
    assert!(missing_cycle
        .init_module()
        .unwrap_err()
        .to_string()
        .contains("must declare service provider"));
}

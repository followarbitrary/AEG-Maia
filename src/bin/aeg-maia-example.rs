use std::sync::{Arc, Mutex};
use std::time::Instant;

use aeg_maia::{
    config::ConfigSnapshot,
    memory::{Pool, PoolFactory},
    module::{Module, ModuleContext, ModuleRegistry},
    process::{
        ControlChannel, ExitStatus, GenerationManager, ModuleRegistryFactory, WorkerCommand,
        WorkerEvent, WorkerFactory, WorkerHandle, WorkerSpawnRequest,
    },
    EventHandler, EventLoop, Result,
};

struct StringFactory;

impl PoolFactory<String> for StringFactory {
    fn create(&mut self) -> Result<String> {
        Ok(String::from("pooled"))
    }
    fn reset(&mut self, value: &mut String) -> Result<()> {
        value.clear();
        value.push_str("pooled");
        Ok(())
    }
}

struct ExampleModule;

impl Module for ExampleModule {
    fn name(&self) -> &str {
        "example"
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

struct RegistryFactory;

impl ModuleRegistryFactory for RegistryFactory {
    fn build(&self, _snapshot: &ConfigSnapshot) -> Result<ModuleRegistry> {
        let mut registry = ModuleRegistry::new();
        registry.register(Box::new(ExampleModule))?;
        Ok(registry)
    }
}

#[derive(Default)]
struct NoopChannel {
    ready: bool,
}

impl ControlChannel for NoopChannel {
    fn send(&mut self, _command: WorkerCommand) -> Result<()> {
        Ok(())
    }

    fn try_recv(&mut self) -> Result<Option<WorkerEvent>> {
        if self.ready {
            self.ready = false;
            Ok(Some(WorkerEvent::Ready))
        } else {
            Ok(None)
        }
    }
}

struct NoopWorker {
    worker_id: u32,
    channel: NoopChannel,
}

impl WorkerHandle for NoopWorker {
    fn worker_id(&self) -> u32 {
        self.worker_id
    }
    fn channel(&mut self) -> &mut dyn ControlChannel {
        &mut self.channel
    }
    fn poll_exit(&mut self) -> Result<Option<ExitStatus>> {
        Ok(None)
    }
    fn terminate(&mut self) -> Result<()> {
        Ok(())
    }
    fn kill(&mut self) -> Result<()> {
        Ok(())
    }
}

struct NoopFactory {
    _created: Arc<Mutex<usize>>,
}

impl WorkerFactory for NoopFactory {
    fn spawn(&self, request: WorkerSpawnRequest) -> Result<Box<dyn WorkerHandle>> {
        *self._created.lock().unwrap() += 1;
        Ok(Box::new(NoopWorker {
            worker_id: request.worker_id,
            channel: NoopChannel { ready: true },
        }))
    }
}

struct StopHandler;

impl EventHandler for StopHandler {
    fn on_event(&mut self, _event: &mio::event::Event) -> Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    let snapshot = ConfigSnapshot::from_toml(
        "[worker]\nprogram='/bin/worker'\ncount=1\n[memory]\ninitial=1\nsoft_max=2\nhard_max=2\n",
    )?;

    let pool = Pool::new(snapshot.memory.clone(), StringFactory)?;
    let item = pool.checkout()?;
    println!("checked out {}", &*item);
    drop(item);

    let mut registry = ModuleRegistry::new();
    registry.register(Box::new(ExampleModule))?;
    println!("module order: {:?}", registry.validate()?.names());

    let mut event_loop = EventLoop::new(8)?;
    event_loop.stop();
    event_loop.run(&mut StopHandler)?;

    let created = Arc::new(Mutex::new(0));
    let mut manager = GenerationManager::new(
        NoopFactory {
            _created: Arc::clone(&created),
        },
        snapshot.clone(),
        Box::new(RegistryFactory),
    )?;
    manager.tick(Instant::now())?;
    manager.reload(snapshot)?;
    manager.tick(Instant::now())?;
    println!("worker spawns: {}", created.lock().unwrap());
    Ok(())
}

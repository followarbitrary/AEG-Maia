use aeg_maia::{
    ConfigSnapshot, ConnHandler, ListenSpec, ModuleRegistry, ModuleRegistryFactory, NetConfig,
    NetModule, NetWorker, Result, WorkerModule, WorkerModuleFactory,
};

#[derive(Clone, Copy, Default)]
struct ReferenceConnHandler;

impl ConnHandler for ReferenceConnHandler {}

#[derive(Clone, Copy, Default)]
pub struct ReferenceWorkerFactory;

impl WorkerModuleFactory for ReferenceWorkerFactory {
    fn build(
        &self,
        name: &str,
        _worker_id: u32,
        snapshot: &ConfigSnapshot,
    ) -> Result<Box<dyn WorkerModule>> {
        let listener = snapshot
            .listeners
            .iter()
            .find(|listener| listener.name == name)
            .ok_or_else(|| {
                aeg_maia::Error::Module(format!("unknown reference net module: {name}"))
            })?;
        Ok(Box::new(NetWorker::new_unbound(
            name,
            listener.name.clone(),
            ReferenceConnHandler,
            NetConfig::from_worker_config(&snapshot.worker.net),
        )))
    }
}

#[derive(Clone, Copy, Default)]
pub struct ReferenceRegistryFactory;

impl ModuleRegistryFactory for ReferenceRegistryFactory {
    fn build(&self, _snapshot: &ConfigSnapshot) -> Result<ModuleRegistry> {
        let mut registry = ModuleRegistry::new();
        for listener in &_snapshot.listeners {
            registry.register(Box::new(NetModule::new(
                listener.name.clone(),
                ListenSpec {
                    name: listener.name.clone(),
                    address: listener.address.clone(),
                    backlog: 1024,
                    reuse_port: false,
                },
            )))?;
        }
        Ok(registry)
    }

    fn worker_entrypoint(&self) -> Option<String> {
        Some("reference".into())
    }
}

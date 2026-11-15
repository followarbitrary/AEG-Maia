use aeg_maia::{
    ConfigSnapshot, ConnHandler, ListenSpec, ModuleRegistry, ModuleRegistryFactory, NetConfig,
    NetModule, NetWorker, Result, WorkerModule, WorkerModuleFactory,
};

#[cfg(unix)]
use std::os::fd::FromRawFd;

use mio::net::TcpListener;

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
        let index = snapshot
            .listeners
            .iter()
            .position(|listener| listener.name == name)
            .ok_or_else(|| {
                aeg_maia::Error::Module(format!("unknown reference net module: {name}"))
            })?;
        let listener = &snapshot.listeners[index];
        let fd = snapshot
            .worker
            .listener_fds
            .get(index)
            .copied()
            .ok_or_else(|| {
                aeg_maia::Error::Process(format!("listener {} has no inherited fd", listener.name))
            })?;
        if fd < 0 {
            return Err(aeg_maia::Error::Process(format!(
                "listener {} has invalid inherited fd {}",
                listener.name, fd
            )));
        }
        let std_listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
        let socket = TcpListener::from_std(std_listener);
        Ok(Box::new(NetWorker::new(
            name,
            listener.name.clone(),
            socket,
            ReferenceConnHandler,
            NetConfig::default(),
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

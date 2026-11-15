use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

use aeg_maia::{
    ConfigSnapshot, ConnHandler, ListenSpec, ListenerClaims, ModuleRegistry, NetConfig, NetModule,
    NetWorker, Result, WorkerEventRegistry, WorkerModule, WorkerModuleFactory,
};
use mio::net::TcpListener;

#[derive(Clone, Default)]
struct Handler(Arc<Mutex<Vec<String>>>);

impl ConnHandler for Handler {
    fn on_accept(
        &mut self,
        _id: aeg_maia::ConnectionId,
        _peer: std::net::SocketAddr,
    ) -> Result<()> {
        self.0.lock().unwrap().push("accept".into());
        Ok(())
    }
}

struct Factory {
    listener: Mutex<Option<TcpListener>>,
    handler: Handler,
}

impl WorkerModuleFactory for Factory {
    fn build(
        &self,
        _name: &str,
        _worker_id: u32,
        _snapshot: &ConfigSnapshot,
    ) -> Result<Box<dyn WorkerModule>> {
        Ok(Box::new(NetWorker::new(
            "net",
            "main",
            self.listener.lock().unwrap().take().unwrap(),
            self.handler.clone(),
            NetConfig::default(),
        )))
    }
}

#[test]
fn net_module_declares_listener_and_worker_uses_full_lifecycle() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(listener);
    let handler = Handler::default();

    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(NetModule::new(
            "net",
            ListenSpec {
                name: "main".into(),
                address: address.to_string(),
                backlog: 128,
                reuse_port: false,
            },
        )))
        .unwrap();
    let mut cycle = registry.build_cycle().unwrap();
    cycle.init_module().unwrap();
    assert_eq!(cycle.listeners()[0].name, "main");

    let snapshot = ConfigSnapshot::from_toml(&format!(
        "[worker]\nprogram='/bin/worker'\ncount=1\n[[listen]]\nname='main'\naddress='{}'\n",
        address
    ))
    .unwrap();
    let claims = ListenerClaims::from_cycle(&cycle);
    let factory = Factory {
        listener: Mutex::new(Some(listener)),
        handler: handler.clone(),
    };
    let mut worker = cycle
        .build_worker_with_registrar(
            &factory,
            0,
            &snapshot,
            Box::new(WorkerEventRegistry::new(claims, 8).unwrap()),
        )
        .unwrap();
    worker.init_process().unwrap();
    worker.ensure_all_listeners_claimed().unwrap();
    worker.register_events().unwrap();
    worker.validate_event_registrations().unwrap();
    worker.start().unwrap();

    let _client = TcpStream::connect(address).unwrap();
    for event in worker.poll_events(Some(Duration::from_secs(1))).unwrap() {
        worker.dispatch_event(event).unwrap();
    }
    assert!(handler
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|entry| entry == "accept"));
}

#![cfg(feature = "net")]

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use aeg_maia::{
    ConnHandler, EventDispatcher, ListenerClaims, NetConfig, NetService, WorkerEventRegistrar,
    WorkerEventRegistry,
};
use mio::net::TcpListener;

#[test]
fn claim_tcp_moves_listener_out_of_core_registry() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut owners = BTreeMap::new();
    owners.insert("main".to_owned(), "net".to_owned());
    let claims = ListenerClaims::with_listeners(
        owners,
        [("main".to_owned(), TcpListener::from_std(listener))],
    );
    let mut registry = WorkerEventRegistry::new(claims, 8).unwrap();
    let listener = registry.claim_tcp("net", "main").unwrap();
    assert_eq!(registry.claimed_listeners(), vec!["main"]);
    assert!(registry.claim_tcp("net", "main").is_err());
    drop(listener);
}

#[derive(Clone, Default)]
struct Echo;

impl ConnHandler for Echo {
    fn on_read_with_writer(
        &mut self,
        _id: aeg_maia::ConnectionId,
        data: &[u8],
        writer: &mut dyn aeg_maia::ConnectionWriter,
    ) -> aeg_maia::Result<()> {
        writer.write(data)
    }
}

#[test]
fn handler_can_write_back_through_connection_writer() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(listener);
    let mut dispatcher = EventDispatcher::new(16).unwrap();
    let mut service = NetService::new("net", Echo, NetConfig::default());
    service
        .register_listener(&mut dispatcher, listener)
        .unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"echo").unwrap();
    let ready = dispatcher.poll(Some(Duration::from_secs(1))).unwrap();
    for event in ready {
        let mut context = dispatcher.context(event.key()).unwrap();
        service.on_event(event, &mut context).unwrap();
    }
    for _ in 0..5 {
        let ready = dispatcher.poll(Some(Duration::from_millis(100))).unwrap();
        for event in ready {
            let mut context = dispatcher.context(event.key()).unwrap();
            service.on_event(event, &mut context).unwrap();
        }
        let mut buf = [0_u8; 4];
        if client.read(&mut buf).ok() == Some(4) {
            assert_eq!(&buf, b"echo");
            return;
        }
    }
    panic!("echo response was not written");
}

#[test]
fn net_config_rejects_oversized_read_buffer() {
    let mut service = NetService::new("net", Echo, NetConfig::default());
    assert!(service.set_max_read_buffer(0).is_err());
}

#[test]
fn idle_timeout_closes_quiet_connection() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(listener);
    let mut dispatcher = EventDispatcher::new(16).unwrap();
    let mut service = NetService::new("net", Echo, NetConfig::default());
    service.set_timeouts(1, 0);
    service
        .register_listener(&mut dispatcher, listener)
        .unwrap();
    let _client = TcpStream::connect(address).unwrap();
    for event in dispatcher.poll(Some(Duration::from_secs(1))).unwrap() {
        let mut context = dispatcher.context(event.key()).unwrap();
        service.on_event(event, &mut context).unwrap();
    }
    assert_eq!(service.connection_count(), 1);
    for event in dispatcher.poll(Some(Duration::from_secs(2))).unwrap() {
        let mut context = dispatcher.context(event.key()).unwrap();
        service.on_event(event, &mut context).unwrap();
    }
    assert_eq!(service.connection_count(), 0);
}

#[test]
fn write_queue_has_a_service_wide_limit() {
    let mut service = NetService::new("net", Echo, NetConfig::default());
    service.set_max_total_write_queue(4).unwrap();
    let first = service.insert_test_connection();
    let second = service.insert_test_connection();
    service.write(first, b"abc").unwrap();
    assert!(service.write(second, b"de").is_err());
    assert_eq!(service.queued_bytes(first).unwrap(), 3);
}

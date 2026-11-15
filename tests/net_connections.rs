#![cfg(feature = "net")]

use std::{
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex},
};

use aeg_maia::{Error, EventDispatcher, NetConfig, NetError, NetService};

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<String>>>);

impl aeg_maia::ConnHandler for Recorder {
    fn on_accept(
        &mut self,
        id: aeg_maia::ConnectionId,
        _peer: std::net::SocketAddr,
    ) -> aeg_maia::Result<()> {
        self.0.lock().unwrap().push(format!("accept:{id}"));
        Ok(())
    }

    fn on_read(&mut self, id: aeg_maia::ConnectionId, data: &[u8]) -> aeg_maia::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("read:{id}:{}", String::from_utf8_lossy(data)));
        Ok(())
    }

    fn on_read_eof(&mut self, id: aeg_maia::ConnectionId) -> aeg_maia::Result<()> {
        self.0.lock().unwrap().push(format!("eof:{id}"));
        Ok(())
    }

    fn on_writable(&mut self, id: aeg_maia::ConnectionId) -> aeg_maia::Result<()> {
        self.0.lock().unwrap().push(format!("write:{id}"));
        Ok(())
    }

    fn on_close(
        &mut self,
        id: aeg_maia::ConnectionId,
        reason: aeg_maia::CloseReason,
    ) -> aeg_maia::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("close:{id}:{reason:?}"));
        Ok(())
    }
}

#[test]
fn accepts_reads_and_closes_once_on_eof() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = mio::net::TcpListener::from_std(listener);
    let mut dispatcher = EventDispatcher::new(32).unwrap();
    let handler = Recorder::default();
    let events = handler.0.clone();
    let mut service = NetService::new("net", handler, NetConfig::default());
    let listener_key = service
        .register_listener(&mut dispatcher, listener)
        .unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"hello").unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();

    let mut ready = dispatcher
        .poll(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    assert!(ready.iter().any(|event| event.key() == listener_key));
    for event in ready.drain(..) {
        let mut context = dispatcher.context(event.key()).unwrap();
        service.on_event(event, &mut context).unwrap();
    }

    for _ in 0..4 {
        let ready = dispatcher
            .poll(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        for event in ready {
            let mut context = dispatcher.context(event.key()).unwrap();
            service.on_event(event, &mut context).unwrap();
        }
        if events
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.starts_with("close:"))
        {
            break;
        }
    }
    let lines = events.lock().unwrap().clone();
    assert!(lines.iter().any(|line| line.starts_with("accept:")));
    assert!(lines
        .iter()
        .any(|line| line.contains("read:") && line.contains("hello")));
    assert!(lines.iter().any(|line| line.starts_with("eof:")));
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("close:"))
            .count(),
        1
    );
}

#[test]
fn queue_full_is_reported_without_enqueuing() {
    let handler = Recorder::default();
    let mut service = NetService::new(
        "net",
        handler,
        NetConfig {
            max_write_queue: 4,
            high_watermark: 3,
            low_watermark: 1,
        },
    );
    let id = service.insert_test_connection();
    assert_eq!(
        service.connection_state(id).unwrap(),
        aeg_maia::ConnectionState::Open
    );
    service.write(id, b"abcd").unwrap();
    let result = service.write(id, b"e");
    assert!(matches!(result, Err(Error::Net(NetError::QueueFull))));
    assert_eq!(service.queued_bytes(id).unwrap(), 4);
}

#[test]
fn draining_deregisters_listener_without_accepting_new_connections() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = mio::net::TcpListener::from_std(listener);
    let mut dispatcher = EventDispatcher::new(8).unwrap();
    let mut service = NetService::new("net", Recorder::default(), NetConfig::default());
    let key = service
        .register_listener(&mut dispatcher, listener)
        .unwrap();
    let mut context = dispatcher.context(key).unwrap();
    service.begin_drain(&mut context).unwrap();
    assert_eq!(service.listener_count(), 0);

    assert!(dispatcher
        .poll(Some(std::time::Duration::from_millis(20)))
        .unwrap()
        .is_empty());
}

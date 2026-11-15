use std::{
    io::Write,
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use aeg_maia::event::{
    accept_loop, EventDispatcher, EventInterest, EventKey, EventRegistrar, ModuleEvent,
};
use mio::net::{TcpListener, UnixStream};

#[test]
fn dispatcher_registers_sources_and_maps_readiness_to_opaque_keys() {
    let mut dispatcher = EventDispatcher::new(8).unwrap();
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    let key = dispatcher
        .register_source("module", &mut reader, EventInterest::readable())
        .unwrap();
    assert_ne!(key, EventKey::from_raw(0));

    writer.write_all(&[1]).unwrap();
    let events = dispatcher.poll(Some(Duration::from_millis(100))).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key(), key);
    assert!(events[0].is_readable());
    assert!(!events[0].is_writable());
}

#[test]
fn dispatcher_reregisters_and_deregisters_sources() {
    let mut dispatcher = EventDispatcher::new(8).unwrap();
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    let key = dispatcher
        .register_source("module", &mut reader, EventInterest::readable())
        .unwrap();
    dispatcher
        .reregister_source(key, &mut reader, EventInterest::readable_writable())
        .unwrap();
    dispatcher.deregister_source(key, &mut reader).unwrap();

    writer.write_all(&[1]).unwrap();
    assert!(dispatcher
        .poll(Some(Duration::from_millis(10)))
        .unwrap()
        .is_empty());
}

#[test]
fn event_context_rejects_foreign_source_mutation() {
    let mut dispatcher = EventDispatcher::new(8).unwrap();
    let (mut first, _first_writer) = UnixStream::pair().unwrap();
    let (mut second, _second_writer) = UnixStream::pair().unwrap();
    let first_key = dispatcher
        .register_source("first", &mut first, EventInterest::readable())
        .unwrap();
    let second_key = dispatcher
        .register_source("second", &mut second, EventInterest::readable())
        .unwrap();
    let mut context = dispatcher.context(first_key).unwrap();
    assert!(context
        .reregister_source_for(second_key, &mut second, EventInterest::writable())
        .is_err());
}

#[test]
fn accept_loop_drains_until_would_block() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let mut listener = TcpListener::from_std(listener);
    let _client = TcpStream::connect(address).unwrap();

    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_millis(200);
    let mut count = 0;
    while count == 0 && Instant::now() < deadline {
        count = accept_loop(&mut listener, |_stream, _peer| {
            accepted += 1;
            Ok(())
        })
        .unwrap();
        if count == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }

    assert_eq!(count, 1);
    assert_eq!(accepted, 1);
}

#[test]
fn module_event_exposes_error_and_hup_flags() {
    let event = ModuleEvent::new(EventKey::from_raw(7), false, false, true, true);
    assert_eq!(event.key(), EventKey::from_raw(7));
    assert!(event.is_error());
    assert!(event.is_hup());
}

#[test]
fn posted_command_wakes_poll_and_runs_on_dispatcher_thread() {
    let mut dispatcher = EventDispatcher::new(8).unwrap();
    let observed = Arc::new(AtomicBool::new(false));
    let posted = Arc::clone(&observed);
    let post = dispatcher.post_handle();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        post.post(move |_dispatcher| {
            posted.store(true, Ordering::Release);
        })
        .unwrap();
    });
    let events = dispatcher.poll(Some(Duration::from_secs(1))).unwrap();
    handle.join().unwrap();
    assert!(events.is_empty());
    assert!(observed.load(Ordering::Acquire));
}

#[test]
fn scheduled_timeout_is_returned_as_a_module_event() {
    let mut dispatcher = EventDispatcher::new(8).unwrap();
    let (mut reader, _writer) = UnixStream::pair().unwrap();
    let key = dispatcher
        .register_source("module", &mut reader, EventInterest::readable())
        .unwrap();
    dispatcher
        .schedule_timeout(key, Duration::from_millis(10))
        .unwrap();
    let events = dispatcher.poll(Some(Duration::from_secs(1))).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key(), key);
    assert!(events[0].is_timeout());
}

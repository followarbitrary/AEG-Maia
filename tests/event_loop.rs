use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use aeg_maia::{
    event::{EventHandler, EventLoop},
    Result,
};
use mio::{net::UnixStream, Interest, Token};

struct Handler {
    seen: Arc<Mutex<Vec<Token>>>,
    stop: aeg_maia::event::EventLoopHandle,
}

impl EventHandler for Handler {
    fn on_event(&mut self, event: &mio::event::Event) -> Result<()> {
        self.seen.lock().unwrap().push(event.token());
        self.stop.stop();
        Ok(())
    }
}

#[test]
fn dispatches_registered_source_and_stops_from_handler() {
    let mut event_loop = EventLoop::new(8).unwrap();
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    event_loop
        .register(&mut reader, Token(7), Interest::READABLE)
        .unwrap();
    writer.write_all(&[1]).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut handler = Handler {
        seen: Arc::clone(&seen),
        stop: event_loop.stop_handle(),
    };

    event_loop.run(&mut handler).unwrap();

    assert_eq!(&*seen.lock().unwrap(), &[Token(7)]);
}

#[test]
fn stop_wakes_a_blocked_poll_from_another_thread() {
    let mut event_loop = EventLoop::new(8).unwrap();
    let handle = event_loop.stop_handle();
    let join = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        handle.stop();
    });
    let mut handler = |_event: &mio::event::Event| -> Result<()> { Ok(()) };
    event_loop.run(&mut handler).unwrap();
    join.join().unwrap();
}

#[test]
fn handler_errors_are_returned() {
    let mut event_loop = EventLoop::new(8).unwrap();
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    event_loop
        .register(&mut reader, Token(9), Interest::READABLE)
        .unwrap();
    writer.write_all(&[1]).unwrap();
    let mut handler = |_event: &mio::event::Event| -> Result<()> {
        Err(aeg_maia::Error::Event("handler failed".into()))
    };

    let error = event_loop.run(&mut handler).expect_err("handler error");
    assert!(error.to_string().contains("handler failed"));
}

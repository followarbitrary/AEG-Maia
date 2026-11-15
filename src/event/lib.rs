use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use std::{collections::BTreeMap, io, net::SocketAddr, time::Duration};

use mio::{event::Source, net::TcpListener, Events, Interest, Poll, Registry, Token, Waker};

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventKey(usize);

impl EventKey {
    pub const fn from_raw(raw: usize) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventInterest {
    readable: bool,
    writable: bool,
}

impl EventInterest {
    pub const fn readable() -> Self {
        Self {
            readable: true,
            writable: false,
        }
    }

    pub const fn writable() -> Self {
        Self {
            readable: false,
            writable: true,
        }
    }

    pub const fn readable_writable() -> Self {
        Self {
            readable: true,
            writable: true,
        }
    }

    fn into_mio(self) -> Interest {
        match (self.readable, self.writable) {
            (true, true) => Interest::READABLE | Interest::WRITABLE,
            (true, false) => Interest::READABLE,
            (false, true) => Interest::WRITABLE,
            (false, false) => Interest::READABLE,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModuleEvent {
    key: EventKey,
    readable: bool,
    writable: bool,
    error: bool,
    hup: bool,
}

impl ModuleEvent {
    pub const fn new(
        key: EventKey,
        readable: bool,
        writable: bool,
        error: bool,
        hup: bool,
    ) -> Self {
        Self {
            key,
            readable,
            writable,
            error,
            hup,
        }
    }

    pub const fn key(self) -> EventKey {
        self.key
    }

    pub const fn is_readable(self) -> bool {
        self.readable
    }

    pub const fn is_writable(self) -> bool {
        self.writable
    }

    pub const fn is_error(self) -> bool {
        self.error
    }

    pub const fn is_hup(self) -> bool {
        self.hup
    }
}

pub trait EventRegistrar {
    fn register_source(
        &mut self,
        owner: &str,
        source: &mut dyn Source,
        interests: EventInterest,
    ) -> Result<EventKey>;

    fn reregister_source(
        &mut self,
        key: EventKey,
        source: &mut dyn Source,
        interests: EventInterest,
    ) -> Result<()>;

    fn deregister_source(&mut self, key: EventKey, source: &mut dyn Source) -> Result<()>;

    fn unregister_owner(&mut self, owner: &str);

    fn validate(&self) -> Result<()>;
}

#[derive(Clone, Debug)]
struct Registration {
    owner: String,
    interests: EventInterest,
    error_count: u64,
}

pub struct EventDispatcher {
    poll: Poll,
    events: Events,
    registrations: BTreeMap<EventKey, Registration>,
    next_key: usize,
    draining: bool,
}

pub struct EventContext<'a> {
    dispatcher: &'a mut EventDispatcher,
    key: EventKey,
    owner: String,
}

impl EventContext<'_> {
    pub const fn key(&self) -> EventKey {
        self.key
    }

    pub fn is_draining(&self) -> bool {
        self.dispatcher.is_draining()
    }

    pub fn error_count(&self) -> u64 {
        self.dispatcher.error_count(self.key).unwrap_or(0)
    }

    pub fn register_source(
        &mut self,
        owner: &str,
        source: &mut dyn Source,
        interests: EventInterest,
    ) -> Result<EventKey> {
        if owner != self.owner {
            return Err(crate::error::Error::Event(format!(
                "module {} cannot register a source for module {}",
                self.owner, owner
            )));
        }
        self.dispatcher.register_source(owner, source, interests)
    }

    pub fn reregister_source(
        &mut self,
        source: &mut dyn Source,
        interests: EventInterest,
    ) -> Result<()> {
        self.reregister_source_for(self.key, source, interests)
    }

    /// Reregister any source owned by this module. This is used by services
    /// that accept a new source while handling a different event key.
    pub fn reregister_source_for(
        &mut self,
        key: EventKey,
        source: &mut dyn Source,
        interests: EventInterest,
    ) -> Result<()> {
        self.ensure_owner(key)?;
        self.dispatcher.reregister_source(key, source, interests)
    }

    pub fn deregister_source(&mut self, source: &mut dyn Source) -> Result<()> {
        self.deregister_source_for(self.key, source)
    }

    /// Deregister any source owned by this module. This is used when a
    /// callback closes a connection accepted from another event.
    pub fn deregister_source_for(&mut self, key: EventKey, source: &mut dyn Source) -> Result<()> {
        self.ensure_owner(key)?;
        self.dispatcher.deregister_source(key, source)
    }

    fn ensure_owner(&self, key: EventKey) -> Result<()> {
        match self.dispatcher.owner(key) {
            Some(owner) if owner == self.owner => Ok(()),
            Some(owner) => Err(crate::error::Error::Event(format!(
                "module {} cannot modify source owned by {}",
                self.owner, owner
            ))),
            None => Err(crate::error::Error::Event(format!(
                "unknown event key: {}",
                key.raw()
            ))),
        }
    }

    pub fn unregister(&mut self) {
        self.dispatcher.remove_key(self.key);
    }
}

impl EventDispatcher {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::Event(
                "event capacity must be greater than zero".into(),
            ));
        }
        Ok(Self {
            poll: Poll::new().map_err(|error| Error::Event(error.to_string()))?,
            events: Events::with_capacity(capacity),
            registrations: BTreeMap::new(),
            next_key: 1,
            draining: false,
        })
    }

    pub fn set_draining(&mut self, draining: bool) {
        self.draining = draining;
    }

    pub fn context(&mut self, key: EventKey) -> Result<EventContext<'_>> {
        let owner = self
            .registrations
            .get(&key)
            .map(|registration| registration.owner.clone())
            .ok_or_else(|| Error::Event(format!("unknown event key: {}", key.raw())))?;
        Ok(EventContext {
            dispatcher: self,
            key,
            owner,
        })
    }

    pub fn is_draining(&self) -> bool {
        self.draining
    }

    pub fn poll(&mut self, timeout: Option<Duration>) -> Result<Vec<ModuleEvent>> {
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(|error| Error::Event(error.to_string()))?;
        let mut ready = Vec::new();
        for event in self.events.iter() {
            let key = EventKey(event.token().0);
            if !self.registrations.contains_key(&key) {
                continue;
            }
            ready.push(ModuleEvent::new(
                key,
                event.is_readable(),
                event.is_writable(),
                event.is_error(),
                event.is_read_closed() || event.is_write_closed(),
            ));
        }
        ready.sort_by_key(|event| event.key());
        Ok(ready)
    }

    pub fn record_handler_error(&mut self, key: EventKey) -> Result<u64> {
        let registration = self
            .registrations
            .get_mut(&key)
            .ok_or_else(|| Error::Event(format!("unknown event key: {}", key.raw())))?;
        registration.error_count += 1;
        Ok(registration.error_count)
    }

    pub fn error_count(&self, key: EventKey) -> Option<u64> {
        self.registrations
            .get(&key)
            .map(|registration| registration.error_count)
    }

    pub fn owner(&self, key: EventKey) -> Option<&str> {
        self.registrations
            .get(&key)
            .map(|registration| registration.owner.as_str())
    }

    pub fn remove_key(&mut self, key: EventKey) {
        self.registrations.remove(&key);
    }

    pub fn clear_registrations(&mut self) {
        self.registrations.clear();
    }

    pub fn interests(&self, key: EventKey) -> Option<EventInterest> {
        self.registrations
            .get(&key)
            .map(|registration| registration.interests)
    }
}

impl EventRegistrar for EventDispatcher {
    fn register_source(
        &mut self,
        owner: &str,
        source: &mut dyn Source,
        interests: EventInterest,
    ) -> Result<EventKey> {
        if owner.trim().is_empty() {
            return Err(Error::Event("event source owner must not be empty".into()));
        }
        let key = EventKey(self.next_key);
        self.next_key = self
            .next_key
            .checked_add(1)
            .ok_or_else(|| Error::Event("event key space exhausted".into()))?;
        self.poll
            .registry()
            .register(source, Token(key.raw()), interests.into_mio())
            .map_err(|error| Error::Event(error.to_string()))?;
        let previous = self.registrations.insert(
            key,
            Registration {
                owner: owner.to_owned(),
                interests,
                error_count: 0,
            },
        );
        debug_assert!(previous.is_none());
        Ok(key)
    }

    fn reregister_source(
        &mut self,
        key: EventKey,
        source: &mut dyn Source,
        interests: EventInterest,
    ) -> Result<()> {
        let registration = self
            .registrations
            .get_mut(&key)
            .ok_or_else(|| Error::Event(format!("unknown event key: {}", key.raw())))?;
        self.poll
            .registry()
            .reregister(source, Token(key.raw()), interests.into_mio())
            .map_err(|error| Error::Event(error.to_string()))?;
        registration.interests = interests;
        Ok(())
    }

    fn deregister_source(&mut self, key: EventKey, source: &mut dyn Source) -> Result<()> {
        if !self.registrations.contains_key(&key) {
            return Err(Error::Event(format!("unknown event key: {}", key.raw())));
        }
        self.poll
            .registry()
            .deregister(source)
            .map_err(|error| Error::Event(error.to_string()))?;
        self.registrations.remove(&key);
        Ok(())
    }

    fn unregister_owner(&mut self, owner: &str) {
        self.registrations
            .retain(|_, registration| registration.owner != owner);
    }

    fn validate(&self) -> Result<()> {
        if self
            .registrations
            .values()
            .any(|registration| registration.owner.trim().is_empty())
        {
            return Err(Error::Event("event source owner must not be empty".into()));
        }
        Ok(())
    }
}

pub fn accept_loop<F>(listener: &mut TcpListener, mut on_accept: F) -> Result<usize>
where
    F: FnMut(mio::net::TcpStream, SocketAddr) -> Result<()>,
{
    let mut accepted = 0;
    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                on_accept(stream, peer)?;
                accepted += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(accepted),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(Error::Event(error.to_string())),
        }
    }
}

const STOP_TOKEN: Token = Token(usize::MAX);

pub trait EventHandler {
    fn on_event(&mut self, event: &mio::event::Event) -> Result<()>;
}

impl<F> EventHandler for F
where
    F: FnMut(&mio::event::Event) -> Result<()>,
{
    fn on_event(&mut self, event: &mio::event::Event) -> Result<()> {
        self(event)
    }
}

#[derive(Clone)]
pub struct EventLoopHandle {
    stop_requested: Arc<AtomicBool>,
    waker: Arc<Waker>,
}

impl EventLoopHandle {
    pub fn stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
        let _ = self.waker.wake();
    }
}

pub struct EventLoop {
    poll: Poll,
    events: Events,
    handle: EventLoopHandle,
}

impl EventLoop {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::Event(
                "event capacity must be greater than zero".into(),
            ));
        }
        let poll = Poll::new().map_err(|error| Error::Event(error.to_string()))?;
        let waker = Arc::new(
            Waker::new(poll.registry(), STOP_TOKEN)
                .map_err(|error| Error::Event(error.to_string()))?,
        );
        Ok(Self {
            poll,
            events: Events::with_capacity(capacity),
            handle: EventLoopHandle {
                stop_requested: Arc::new(AtomicBool::new(false)),
                waker,
            },
        })
    }

    pub fn registry(&self) -> &Registry {
        self.poll.registry()
    }

    pub fn stop_handle(&self) -> EventLoopHandle {
        self.handle.clone()
    }

    pub fn stop(&self) {
        self.handle.stop();
    }

    pub fn register<S: Source + ?Sized>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
    ) -> Result<()> {
        if token == STOP_TOKEN {
            return Err(Error::Event("the stop token is reserved".into()));
        }
        self.registry()
            .register(source, token, interests)
            .map_err(|error| Error::Event(error.to_string()))
    }

    pub fn reregister<S: Source + ?Sized>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
    ) -> Result<()> {
        if token == STOP_TOKEN {
            return Err(Error::Event("the stop token is reserved".into()));
        }
        self.registry()
            .reregister(source, token, interests)
            .map_err(|error| Error::Event(error.to_string()))
    }

    pub fn deregister<S: Source + ?Sized>(&self, source: &mut S) -> Result<()> {
        self.registry()
            .deregister(source)
            .map_err(|error| Error::Event(error.to_string()))
    }

    pub fn run<H: EventHandler>(&mut self, handler: &mut H) -> Result<()> {
        loop {
            if self.handle.stop_requested.load(Ordering::Acquire) {
                return Ok(());
            }
            self.poll
                .poll(&mut self.events, None)
                .map_err(|error| Error::Event(error.to_string()))?;
            for event in self.events.iter() {
                if event.token() == STOP_TOKEN {
                    continue;
                }
                handler.on_event(event)?;
                if self.handle.stop_requested.load(Ordering::Acquire) {
                    return Ok(());
                }
            }
        }
    }
}

use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use std::{
    collections::{BTreeMap, VecDeque},
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

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
    timeout: bool,
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
            timeout: false,
        }
    }

    pub const fn timeout(key: EventKey) -> Self {
        Self {
            key,
            readable: false,
            writable: false,
            error: false,
            hup: false,
            timeout: true,
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

    pub const fn is_timeout(self) -> bool {
        self.timeout
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
    waker: Arc<Waker>,
    commands: Arc<Mutex<VecDeque<EventCommand>>>,
    timers: BTreeMap<Instant, Vec<EventKey>>,
    raw_sources: RefCell<BTreeMap<Token, usize>>,
}

type EventCommand = Box<dyn FnOnce(&mut EventDispatcher) + Send + 'static>;

#[derive(Clone)]
pub struct EventPostHandle {
    waker: Arc<Waker>,
    commands: Arc<Mutex<VecDeque<EventCommand>>>,
}

impl EventPostHandle {
    /// Wake the owning event loop without enqueueing a command.
    pub fn wake(&self) -> Result<()> {
        self.waker
            .wake()
            .map_err(|error| Error::Event(error.to_string()))
    }

    pub fn post<F>(&self, command: F) -> Result<()>
    where
        F: FnOnce(&mut EventDispatcher) + Send + 'static,
    {
        self.commands
            .lock()
            .map_err(|_| Error::Event("event command queue poisoned".into()))?
            .push_back(Box::new(command));
        self.wake()
    }
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

    pub fn post<F>(&self, command: F) -> Result<()>
    where
        F: FnOnce(&mut EventDispatcher) + Send + 'static,
    {
        self.dispatcher.post(command)
    }

    pub fn schedule_timeout(&mut self, after: Duration) -> Result<()> {
        self.dispatcher.schedule_timeout(self.key, after)
    }

    pub fn schedule_timeout_for(&mut self, key: EventKey, after: Duration) -> Result<()> {
        self.ensure_owner(key)?;
        self.dispatcher.schedule_timeout(key, after)
    }

    pub fn cancel_timeout(&mut self) {
        self.dispatcher.cancel_timeout(self.key);
    }

    pub fn cancel_timeout_for(&mut self, key: EventKey) -> Result<()> {
        self.ensure_owner(key)?;
        self.dispatcher.cancel_timeout(key);
        Ok(())
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
        let poll = Poll::new().map_err(|error| Error::Event(error.to_string()))?;
        let waker = Arc::new(
            Waker::new(poll.registry(), Token(usize::MAX - 1))
                .map_err(|error| Error::Event(error.to_string()))?,
        );
        let commands = Arc::new(Mutex::new(VecDeque::new()));
        Ok(Self {
            poll,
            events: Events::with_capacity(capacity),
            registrations: BTreeMap::new(),
            next_key: 1,
            draining: false,
            waker,
            commands,
            timers: BTreeMap::new(),
            raw_sources: RefCell::new(BTreeMap::new()),
        })
    }

    /// Expose the shared mio registry for adapters that need the low-level
    /// source API. All polling still happens through this dispatcher.
    pub fn registry(&self) -> &Registry {
        self.poll.registry()
    }

    pub fn post_handle(&self) -> EventPostHandle {
        EventPostHandle {
            waker: Arc::clone(&self.waker),
            commands: Arc::clone(&self.commands),
        }
    }

    pub fn post<F>(&self, command: F) -> Result<()>
    where
        F: FnOnce(&mut EventDispatcher) + Send + 'static,
    {
        self.post_handle().post(command)
    }

    fn raw_source_id<S: Source + ?Sized>(source: &mut S) -> usize {
        source as *mut S as *mut () as usize
    }

    /// Register a legacy/raw mio source on the same poller used by module
    /// events. This is used only by the compatibility `EventLoop` facade.
    fn register_raw_source<S: Source + ?Sized>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
    ) -> Result<()> {
        if token == STOP_TOKEN || token == Token(usize::MAX - 1) {
            return Err(Error::Event("the event-loop wake token is reserved".into()));
        }
        if self.raw_sources.borrow().contains_key(&token)
            || self
                .registrations
                .contains_key(&EventKey::from_raw(token.0))
        {
            return Err(Error::Event(format!(
                "event token already registered: {token:?}"
            )));
        }
        self.poll
            .registry()
            .register(source, token, interests)
            .map_err(|error| Error::Event(error.to_string()))?;
        self.raw_sources
            .borrow_mut()
            .insert(token, Self::raw_source_id(source));
        Ok(())
    }

    fn reregister_raw_source<S: Source + ?Sized>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
    ) -> Result<()> {
        if !self.raw_sources.borrow().contains_key(&token) {
            return Err(Error::Event(format!("unknown event token: {token:?}")));
        }
        self.poll
            .registry()
            .reregister(source, token, interests)
            .map_err(|error| Error::Event(error.to_string()))
    }

    fn deregister_raw_source<S: Source + ?Sized>(&self, source: &mut S) -> Result<()> {
        let source_id = Self::raw_source_id(source);
        let token = self
            .raw_sources
            .borrow()
            .iter()
            .find_map(|(token, id)| (*id == source_id).then_some(*token))
            .ok_or_else(|| Error::Event("unknown raw event source".into()))?;
        self.poll
            .registry()
            .deregister(source)
            .map_err(|error| Error::Event(error.to_string()))?;
        self.raw_sources.borrow_mut().remove(&token);
        Ok(())
    }

    fn poll_raw(&mut self, timeout: Option<Duration>) -> Result<Vec<mio::event::Event>> {
        self.apply_posted_commands();
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(|error| Error::Event(error.to_string()))?;
        Ok(self
            .events
            .iter()
            .filter(|event| event.token() != Token(usize::MAX - 1))
            .cloned()
            .collect())
    }

    pub fn schedule_timeout(&mut self, key: EventKey, after: Duration) -> Result<()> {
        if !self.registrations.contains_key(&key) {
            return Err(Error::Event(format!("unknown event key: {}", key.raw())));
        }
        self.cancel_timeout(key);
        self.timers
            .entry(
                Instant::now()
                    .checked_add(after)
                    .unwrap_or_else(Instant::now),
            )
            .or_default()
            .push(key);
        Ok(())
    }

    pub fn cancel_timeout(&mut self, key: EventKey) {
        self.timers.retain(|_, keys| {
            keys.retain(|candidate| *candidate != key);
            !keys.is_empty()
        });
    }

    fn apply_posted_commands(&mut self) {
        let commands = self
            .commands
            .lock()
            .ok()
            .map(|mut queue| queue.drain(..).collect::<Vec<_>>());
        if let Some(commands) = commands {
            for command in commands {
                command(self);
            }
        }
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
        self.apply_posted_commands();
        let timer_timeout = self
            .timers
            .keys()
            .next()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        let timeout = match (timeout, timer_timeout) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (None, Some(timer)) => Some(timer),
            (other, None) => other,
        };
        let mut ready = Vec::new();
        for event in self.poll_raw(timeout)? {
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
        self.apply_posted_commands();
        let now = Instant::now();
        let due = self
            .timers
            .keys()
            .take_while(|deadline| **deadline <= now)
            .copied()
            .collect::<Vec<_>>();
        for deadline in due {
            if let Some(keys) = self.timers.remove(&deadline) {
                for key in keys {
                    if self.registrations.contains_key(&key) {
                        ready.push(ModuleEvent::timeout(key));
                    }
                }
            }
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
        self.cancel_timeout(key);
    }

    pub fn clear_registrations(&mut self) {
        self.registrations.clear();
        self.timers.clear();
        self.raw_sources.get_mut().clear();
    }

    pub fn registration_count(&self) -> usize {
        self.registrations.len()
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
        let key = loop {
            let candidate = EventKey(self.next_key);
            self.next_key = self
                .next_key
                .checked_add(1)
                .ok_or_else(|| Error::Event("event key space exhausted".into()))?;
            if self
                .raw_sources
                .borrow()
                .contains_key(&Token(candidate.raw()))
                || candidate.raw() == usize::MAX - 1
                || candidate.raw() == usize::MAX
            {
                continue;
            }
            break candidate;
        };
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
    wake: EventPostHandle,
}

impl EventLoopHandle {
    pub fn stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
        let _ = self.wake.wake();
    }
}

/// Compatibility facade for the original raw-mio API.
///
/// The worker-facing `EventDispatcher` is now the single poller and owns the
/// waker, posted-command queue, timers, and registration lifecycle. This
/// facade only translates raw mio registration/callbacks for older embedders.
pub struct EventLoop {
    dispatcher: EventDispatcher,
    handle: EventLoopHandle,
}

impl EventLoop {
    pub fn new(capacity: usize) -> Result<Self> {
        let dispatcher = EventDispatcher::new(capacity)?;
        let wake = dispatcher.post_handle();
        Ok(Self {
            dispatcher,
            handle: EventLoopHandle {
                stop_requested: Arc::new(AtomicBool::new(false)),
                wake,
            },
        })
    }

    pub fn registry(&self) -> &Registry {
        self.dispatcher.registry()
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
        self.dispatcher
            .register_raw_source(source, token, interests)
    }

    pub fn reregister<S: Source + ?Sized>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
    ) -> Result<()> {
        self.dispatcher
            .reregister_raw_source(source, token, interests)
    }

    pub fn deregister<S: Source + ?Sized>(&self, source: &mut S) -> Result<()> {
        self.dispatcher.deregister_raw_source(source)
    }

    pub fn run<H: EventHandler>(&mut self, handler: &mut H) -> Result<()> {
        loop {
            if self.handle.stop_requested.load(Ordering::Acquire) {
                return Ok(());
            }
            for event in self.dispatcher.poll_raw(None)? {
                handler.on_event(&event)?;
                if self.handle.stop_requested.load(Ordering::Acquire) {
                    return Ok(());
                }
            }
        }
    }
}

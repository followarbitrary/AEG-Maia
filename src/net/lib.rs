//! Optional TCP connection service built on top of the core event API.
//!
//! The core only knows about event sources. This module owns accepted
//! connections, connection ids, read/write buffering, and protocol callbacks.

use std::{
    collections::{HashMap, VecDeque},
    io::{self, Read, Write},
    net::SocketAddr,
    time::Duration,
};

use mio::net::{TcpListener, TcpStream};

use crate::{
    config::WorkerNetConfig,
    error::{Error, NetError, Result},
    event::{EventContext, EventInterest, EventKey, EventRegistrar, ModuleEvent},
    module::{ListenSpec, Module, ModuleContext, WorkerContext, WorkerModule},
};

pub type ConnectionId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    PeerClosed,
    Reset,
    Timeout,
    LocalClose,
    Overflow,
    HandlerError,
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Open,
    ReadHalfClosed,
    Draining,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetConfig {
    pub max_write_queue: usize,
    pub high_watermark: usize,
    pub low_watermark: usize,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            max_write_queue: 1024 * 1024,
            high_watermark: 768 * 1024,
            low_watermark: 256 * 1024,
        }
    }
}

impl NetConfig {
    pub fn from_worker_config(config: &WorkerNetConfig) -> Self {
        Self {
            max_write_queue: config.max_write_queue,
            high_watermark: config.high_watermark,
            low_watermark: config.low_watermark,
        }
    }

    fn validate(self) -> Result<()> {
        if self.max_write_queue == 0
            || self.low_watermark > self.high_watermark
            || self.high_watermark > self.max_write_queue
        {
            return Err(NetError::InvalidWatermarks.into());
        }
        Ok(())
    }
}

/// The write side of a connection made available to protocol callbacks.
pub trait ConnectionWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<()>;
}

pub trait ConnHandler: Send {
    fn on_accept(&mut self, _id: ConnectionId, _peer: SocketAddr) -> Result<()> {
        Ok(())
    }

    fn on_accept_with_writer(
        &mut self,
        id: ConnectionId,
        peer: SocketAddr,
        _writer: &mut dyn ConnectionWriter,
    ) -> Result<()> {
        self.on_accept(id, peer)
    }

    fn on_read(&mut self, _id: ConnectionId, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    fn on_read_with_writer(
        &mut self,
        id: ConnectionId,
        data: &[u8],
        _writer: &mut dyn ConnectionWriter,
    ) -> Result<()> {
        self.on_read(id, data)
    }

    fn on_read_eof(&mut self, _id: ConnectionId) -> Result<()> {
        Ok(())
    }

    fn on_writable(&mut self, _id: ConnectionId) -> Result<()> {
        Ok(())
    }

    fn on_close(&mut self, _id: ConnectionId, _reason: CloseReason) -> Result<()> {
        Ok(())
    }
}

struct Listener {
    socket: TcpListener,
}

struct Connection {
    key: EventKey,
    socket: TcpStream,
    queue: VecDeque<Vec<u8>>,
    queued_bytes: usize,
    eof: bool,
    was_above_low: bool,
    state: ConnectionState,
}

struct ConnectionQueueWriter<'a> {
    connection: &'a mut Connection,
    total_queued_bytes: &'a mut usize,
    max_write_queue: usize,
    max_total_write_queue: usize,
}

impl ConnectionWriter for ConnectionQueueWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len()
            > self
                .max_write_queue
                .saturating_sub(self.connection.queued_bytes)
        {
            return Err(NetError::QueueFull.into());
        }
        if bytes.len()
            > self
                .max_total_write_queue
                .saturating_sub(*self.total_queued_bytes)
        {
            return Err(NetError::QueueFull.into());
        }
        if !bytes.is_empty() {
            self.connection.queue.push_back(bytes.to_vec());
            self.connection.queued_bytes += bytes.len();
            *self.total_queued_bytes += bytes.len();
        }
        Ok(())
    }
}

pub struct NetService<H: ConnHandler> {
    owner: String,
    draining: bool,
    handler: H,
    config: NetConfig,
    max_read_buffer: usize,
    max_total_write_queue: usize,
    total_queued_bytes: usize,
    idle_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    next_id: ConnectionId,
    listeners: HashMap<EventKey, Listener>,
    connections: HashMap<ConnectionId, Connection>,
    connection_keys: HashMap<EventKey, ConnectionId>,
}

impl<H: ConnHandler> NetService<H> {
    pub fn new(owner: impl Into<String>, handler: H, config: NetConfig) -> Self {
        Self {
            owner: owner.into(),
            draining: false,
            handler,
            config,
            max_read_buffer: 16 * 1024,
            max_total_write_queue: 8 * 1024 * 1024,
            total_queued_bytes: 0,
            idle_timeout: None,
            write_timeout: None,
            next_id: 1,
            listeners: HashMap::new(),
            connections: HashMap::new(),
            connection_keys: HashMap::new(),
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn set_draining(&mut self, draining: bool) {
        self.draining = draining;
    }

    pub fn set_max_read_buffer(&mut self, max_read_buffer: usize) -> Result<()> {
        if max_read_buffer == 0 {
            return Err(Error::Net(NetError::InvalidReadBuffer));
        }
        self.max_read_buffer = max_read_buffer;
        Ok(())
    }

    pub fn set_max_total_write_queue(&mut self, max_total_write_queue: usize) -> Result<()> {
        if max_total_write_queue == 0 || self.total_queued_bytes > max_total_write_queue {
            return Err(Error::Net(NetError::QueueFull));
        }
        self.max_total_write_queue = max_total_write_queue;
        Ok(())
    }

    pub fn set_timeouts(&mut self, idle_timeout_secs: u64, write_timeout_secs: u64) {
        self.idle_timeout = (idle_timeout_secs > 0).then(|| Duration::from_secs(idle_timeout_secs));
        self.write_timeout =
            (write_timeout_secs > 0).then(|| Duration::from_secs(write_timeout_secs));
    }

    pub fn attach_registered_listener(
        &mut self,
        key: EventKey,
        listener: TcpListener,
    ) -> Result<()> {
        if self.listeners.contains_key(&key) {
            return Err(Error::Event(format!(
                "duplicate listener event key: {}",
                key.raw()
            )));
        }
        self.listeners.insert(key, Listener { socket: listener });
        Ok(())
    }

    pub fn register_listener(
        &mut self,
        registrar: &mut dyn EventRegistrar,
        mut listener: TcpListener,
    ) -> Result<EventKey> {
        self.config.validate()?;
        let key =
            registrar.register_source(&self.owner, &mut listener, EventInterest::readable())?;
        self.listeners.insert(key, Listener { socket: listener });
        Ok(key)
    }

    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }

    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn connection_state(&self, id: ConnectionId) -> Result<ConnectionState> {
        self.connections
            .get(&id)
            .map(|connection| connection.state)
            .ok_or(NetError::UnknownConnection(id).into())
    }

    /// Dispatch one core event. Protocol callback errors close only the
    /// affected connection and do not escape to the worker loop.
    pub fn on_event(&mut self, event: ModuleEvent, context: &mut EventContext<'_>) -> Result<()> {
        if self.draining || context.is_draining() {
            for connection in self.connections.values_mut() {
                connection.state = ConnectionState::Draining;
            }
        }
        let key = event.key();
        if self.listeners.contains_key(&key) {
            return self.accept_ready(key, context);
        }
        let Some(id) = self.connection_keys.get(&key).copied() else {
            return Ok(());
        };
        self.connection_ready(id, event, context)
    }

    fn accept_ready(&mut self, key: EventKey, context: &mut EventContext<'_>) -> Result<()> {
        if self.draining || context.is_draining() {
            return Ok(());
        }
        loop {
            let accepted = {
                let listener = self
                    .listeners
                    .get_mut(&key)
                    .ok_or_else(|| Error::Event(format!("unknown listener key: {}", key.raw())))?;
                listener.socket.accept()
            };
            match accepted {
                Ok((stream, peer)) => self.add_connection(stream, peer, context)?,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                #[cfg(unix)]
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EMFILE) | Some(libc::ENFILE)
                    ) =>
                {
                    // Keep the listener registered and retry after the next
                    // readiness notification; a transient descriptor limit
                    // must not discard already accepted connections.
                    break;
                }
                // A process-wide descriptor exhaustion condition should be
                // surfaced so the event loop can apply its normal error
                // policy, while already accepted connections remain owned by
                // the service.
                Err(error) => return Err(Error::Event(error.to_string())),
            }
        }
        Ok(())
    }

    fn add_connection(
        &mut self,
        mut socket: TcpStream,
        peer: SocketAddr,
        context: &mut EventContext<'_>,
    ) -> Result<()> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| Error::Event("connection id space exhausted".into()))?;
        let key = context.register_source(&self.owner, &mut socket, EventInterest::readable())?;
        self.connection_keys.insert(key, id);
        self.connections.insert(
            id,
            Connection {
                key,
                socket,
                queue: VecDeque::new(),
                queued_bytes: 0,
                eof: false,
                was_above_low: false,
                state: if context.is_draining() {
                    ConnectionState::Draining
                } else {
                    ConnectionState::Open
                },
            },
        );
        let callback_result = {
            let (connections, handler) = (&mut self.connections, &mut self.handler);
            let connection = connections
                .get_mut(&id)
                .expect("connection inserted before accept callback");
            let mut writer = ConnectionQueueWriter {
                connection,
                total_queued_bytes: &mut self.total_queued_bytes,
                max_write_queue: self.config.max_write_queue,
                max_total_write_queue: self.max_total_write_queue,
            };
            handler.on_accept_with_writer(id, peer, &mut writer)
        };
        if let Err(error) = callback_result {
            let reason = matches!(error, Error::Net(NetError::QueueFull))
                .then_some(CloseReason::Overflow)
                .unwrap_or(CloseReason::HandlerError);
            self.close_internal(id, reason, context);
        } else {
            self.update_interest(id, context)?;
        }
        Ok(())
    }

    fn connection_ready(
        &mut self,
        id: ConnectionId,
        event: ModuleEvent,
        context: &mut EventContext<'_>,
    ) -> Result<()> {
        if event.is_timeout() {
            self.close_internal(id, CloseReason::Timeout, context);
            return Ok(());
        }
        if event.is_error() {
            self.close_internal(id, CloseReason::Reset, context);
            return Ok(());
        }

        let mut eof = false;
        if event.is_readable() || event.is_hup() {
            let mut buffer = vec![0_u8; self.max_read_buffer];
            loop {
                let read_result = {
                    let Some(connection) = self.connections.get_mut(&id) else {
                        return Ok(());
                    };
                    connection.socket.read(&mut buffer)
                };
                match read_result {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(length) => {
                        let callback_result = {
                            let (connections, handler) = (&mut self.connections, &mut self.handler);
                            let connection = connections
                                .get_mut(&id)
                                .expect("connection remains during read callback");
                            let mut writer = ConnectionQueueWriter {
                                connection,
                                total_queued_bytes: &mut self.total_queued_bytes,
                                max_write_queue: self.config.max_write_queue,
                                max_total_write_queue: self.max_total_write_queue,
                            };
                            handler.on_read_with_writer(id, &buffer[..length], &mut writer)
                        };
                        if let Err(error) = callback_result {
                            let reason = matches!(error, Error::Net(NetError::QueueFull))
                                .then_some(CloseReason::Overflow)
                                .unwrap_or(CloseReason::HandlerError);
                            self.close_internal(id, reason, context);
                            return Ok(());
                        }
                        self.update_interest(id, context)?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        self.close_internal(id, CloseReason::Reset, context);
                        return Ok(());
                    }
                }
            }
        }

        if eof {
            if self.handler.on_read_eof(id).is_err() {
                self.close_internal(id, CloseReason::HandlerError, context);
                return Ok(());
            }
            if let Some(connection) = self.connections.get_mut(&id) {
                connection.eof = true;
                connection.state = if connection.state == ConnectionState::Draining {
                    ConnectionState::Draining
                } else {
                    ConnectionState::ReadHalfClosed
                };
            }
        }

        if event.is_writable() {
            let notify = self
                .connections
                .get(&id)
                .map(|connection| connection.was_above_low)
                .unwrap_or(false);
            if self.flush(id).is_err() {
                self.close_internal(id, CloseReason::Reset, context);
                return Ok(());
            }
            let below_low = self
                .connections
                .get(&id)
                .map(|connection| connection.queued_bytes <= self.config.low_watermark)
                .unwrap_or(false);
            if notify && below_low && self.handler.on_writable(id).is_err() {
                self.close_internal(id, CloseReason::HandlerError, context);
                return Ok(());
            }
        }

        let should_close = self
            .connections
            .get(&id)
            .map(|connection| connection.eof && connection.queued_bytes == 0)
            .unwrap_or(false);
        if should_close {
            self.close_internal(id, CloseReason::PeerClosed, context);
            return Ok(());
        }
        self.update_interest(id, context)
    }

    fn flush(&mut self, id: ConnectionId) -> Result<()> {
        let Some(connection) = self.connections.get_mut(&id) else {
            return Err(NetError::UnknownConnection(id).into());
        };
        while let Some(front) = connection.queue.front_mut() {
            if front.is_empty() {
                connection.queue.pop_front();
                continue;
            }
            match connection.socket.write(front) {
                Ok(0) => break,
                Ok(length) => {
                    front.drain(..length);
                    connection.queued_bytes -= length;
                    self.total_queued_bytes = self.total_queued_bytes.saturating_sub(length);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(Error::Io(error)),
            }
        }
        while connection.queue.front().is_some_and(Vec::is_empty) {
            connection.queue.pop_front();
        }
        if connection.queued_bytes <= self.config.low_watermark {
            connection.was_above_low = false;
        }
        Ok(())
    }

    fn update_interest(&mut self, id: ConnectionId, context: &mut EventContext<'_>) -> Result<()> {
        let Some(connection) = self.connections.get_mut(&id) else {
            return Ok(());
        };
        let interest = if connection.queue.is_empty() {
            connection.was_above_low = false;
            EventInterest::readable()
        } else {
            if connection.queued_bytes >= self.config.high_watermark {
                connection.was_above_low = true;
            }
            EventInterest::readable_writable()
        };
        let key = connection.key;
        let has_pending_write = !connection.queue.is_empty();
        context.reregister_source_for(key, &mut connection.socket, interest)?;
        let timeout = if has_pending_write {
            self.write_timeout.or(self.idle_timeout)
        } else {
            self.idle_timeout
        };
        if let Some(timeout) = timeout {
            context.schedule_timeout_for(key, timeout)
        } else {
            context.cancel_timeout_for(key)?;
            Ok(())
        }
    }

    pub fn write(&mut self, id: ConnectionId, bytes: &[u8]) -> Result<()> {
        self.config.validate()?;
        let queued_bytes = self
            .connections
            .get(&id)
            .ok_or(NetError::UnknownConnection(id))?
            .queued_bytes;
        if bytes.len() > self.config.max_write_queue - queued_bytes {
            return Err(NetError::QueueFull.into());
        }
        if bytes.len()
            > self
                .max_total_write_queue
                .saturating_sub(self.total_queued_bytes)
        {
            return Err(NetError::QueueFull.into());
        }
        if !bytes.is_empty() {
            let connection = self
                .connections
                .get_mut(&id)
                .expect("connection checked before enqueue");
            connection.queue.push_back(bytes.to_vec());
            connection.queued_bytes += bytes.len();
            self.total_queued_bytes += bytes.len();
        }
        Ok(())
    }

    /// Queue bytes and update the mio interest for this connection.
    pub fn write_with_context(
        &mut self,
        id: ConnectionId,
        bytes: &[u8],
        context: &mut EventContext<'_>,
    ) -> Result<()> {
        match self.write(id, bytes) {
            Ok(()) => {}
            Err(error @ Error::Net(NetError::QueueFull)) => {
                self.close_internal(id, CloseReason::Overflow, context);
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        self.update_interest(id, context)
    }

    pub fn begin_drain(&mut self, context: &mut EventContext<'_>) -> Result<()> {
        let listeners = std::mem::take(&mut self.listeners);
        for (key, mut listener) in listeners {
            context.deregister_source_for(key, &mut listener.socket)?;
        }
        for connection in self.connections.values_mut() {
            connection.state = ConnectionState::Draining;
        }
        Ok(())
    }

    pub fn force_close(
        &mut self,
        context: &mut EventContext<'_>,
        reason: CloseReason,
    ) -> Result<()> {
        let ids: Vec<_> = self.connections.keys().copied().collect();
        for id in ids {
            self.close_internal(id, reason, context);
        }
        Ok(())
    }

    pub fn queued_bytes(&self, id: ConnectionId) -> Result<usize> {
        self.connections
            .get(&id)
            .map(|connection| connection.queued_bytes)
            .ok_or(NetError::UnknownConnection(id).into())
    }

    pub fn close(
        &mut self,
        id: ConnectionId,
        reason: CloseReason,
        context: &mut EventContext<'_>,
    ) -> Result<()> {
        self.close_internal(id, reason, context);
        Ok(())
    }

    fn close_internal(
        &mut self,
        id: ConnectionId,
        reason: CloseReason,
        context: &mut EventContext<'_>,
    ) {
        let Some(mut connection) = self.connections.remove(&id) else {
            return;
        };
        self.total_queued_bytes = self
            .total_queued_bytes
            .saturating_sub(connection.queued_bytes);
        self.connection_keys.remove(&connection.key);
        let _ = context.deregister_source_for(connection.key, &mut connection.socket);
        let _ = self.handler.on_close(id, reason);
    }

    /// Creates a local, unregistered connection for queue-focused unit tests
    /// and embedders that need to stage a connection before registration.
    pub fn insert_test_connection(&mut self) -> ConnectionId {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let _client = std::net::TcpStream::connect(address).expect("connect test listener");
        let (server, _) = listener.accept().expect("accept test connection");
        server
            .set_nonblocking(true)
            .expect("nonblocking test connection");
        let socket = TcpStream::from_std(server);
        let id = self.next_id;
        self.next_id += 1;
        let key = EventKey::from_raw(0);
        self.connections.insert(
            id,
            Connection {
                key,
                socket,
                queue: VecDeque::new(),
                queued_bytes: 0,
                eof: false,
                was_above_low: false,
                state: ConnectionState::Open,
            },
        );
        id
    }
}

pub struct NetModule {
    name: String,
    listener: ListenSpec,
}

impl NetModule {
    pub fn new(name: impl Into<String>, listener: ListenSpec) -> Self {
        Self {
            name: name.into(),
            listener,
        }
    }
}

impl Module for NetModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn init_module(&mut self, ctx: &mut ModuleContext) -> Result<()> {
        ctx.listen(self.listener.name.clone(), self.listener.clone())
    }

    fn exit_master(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }
}

pub struct NetWorker<H: ConnHandler> {
    listener_name: String,
    listener: Option<TcpListener>,
    service: NetService<H>,
}

impl<H: ConnHandler> NetWorker<H> {
    pub fn new(
        module_name: impl Into<String>,
        listener_name: impl Into<String>,
        listener: TcpListener,
        handler: H,
        config: NetConfig,
    ) -> Self {
        let module_name = module_name.into();
        Self {
            listener_name: listener_name.into(),
            service: NetService::new(module_name, handler, config),
            listener: Some(listener),
        }
    }

    /// Construct a worker whose listener is supplied by the core's claim
    /// handoff during `init_process`.
    pub fn new_unbound(
        module_name: impl Into<String>,
        listener_name: impl Into<String>,
        handler: H,
        config: NetConfig,
    ) -> Self {
        let module_name = module_name.into();
        Self {
            listener_name: listener_name.into(),
            service: NetService::new(module_name, handler, config),
            listener: None,
        }
    }

    /// Construct a worker whose listener is transferred by `claim_tcp` during
    /// worker initialization. This is the preferred path for inherited
    /// descriptors; the compatibility constructor above accepts a listener
    /// directly for embedders that already own one.
    pub fn from_claim(
        module_name: impl Into<String>,
        listener_name: impl Into<String>,
        handler: H,
        config: NetConfig,
    ) -> Self {
        let module_name = module_name.into();
        Self {
            listener_name: listener_name.into(),
            service: NetService::new(module_name, handler, config),
            listener: None,
        }
    }

    pub fn service(&self) -> &NetService<H> {
        &self.service
    }

    pub fn service_mut(&mut self) -> &mut NetService<H> {
        &mut self.service
    }
}

impl<H: ConnHandler> WorkerModule for NetWorker<H> {
    fn init_process(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.service
            .set_max_read_buffer(ctx.snapshot.worker.net.max_read_buffer)?;
        self.service
            .set_max_total_write_queue(ctx.snapshot.worker.net.max_total_write_queue)?;
        self.service.set_timeouts(
            ctx.snapshot.worker.net.idle_timeout_secs,
            ctx.snapshot.worker.net.write_timeout_secs,
        );
        if self.listener.is_some() {
            ctx.claim_listener(&self.listener_name)
        } else {
            self.listener = Some(ctx.claim_tcp(&self.listener_name)?);
            Ok(())
        }
    }

    fn register_events(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()> {
        let mut listener = self
            .listener
            .take()
            .ok_or_else(|| Error::Module("net listener already registered".into()))?;
        let key = ctx.register_source(&mut listener, EventInterest::readable())?;
        self.service.attach_registered_listener(key, listener)
    }

    fn start(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        Ok(())
    }

    fn on_event(&mut self, event: ModuleEvent, ctx: &mut EventContext<'_>) -> Result<()> {
        self.service.on_event(event, ctx)
    }

    fn stop(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.service.set_draining(true);
        Ok(())
    }

    fn exit_process(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        self.service.set_draining(true);
        Ok(())
    }
}

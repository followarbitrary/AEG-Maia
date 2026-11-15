//! Optional TCP connection service built on top of the core event API.
//!
//! The core only knows about event sources. This module owns accepted
//! connections, connection ids, read/write buffering, and protocol callbacks.

use std::{
    collections::{HashMap, VecDeque},
    io::{self, Read, Write},
    net::SocketAddr,
};

use mio::net::{TcpListener, TcpStream};

use crate::{
    accept_loop,
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

pub trait ConnHandler: Send {
    fn on_accept(&mut self, _id: ConnectionId, _peer: SocketAddr) -> Result<()> {
        Ok(())
    }

    fn on_read(&mut self, _id: ConnectionId, _data: &[u8]) -> Result<()> {
        Ok(())
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

pub struct NetService<H: ConnHandler> {
    owner: String,
    draining: bool,
    handler: H,
    config: NetConfig,
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
        let mut accepted = Vec::new();
        {
            let listener = self
                .listeners
                .get_mut(&key)
                .ok_or_else(|| Error::Event(format!("unknown listener key: {}", key.raw())))?;
            accept_loop(&mut listener.socket, |stream, peer| {
                accepted.push((stream, peer));
                Ok(())
            })?;
        }
        for (stream, peer) in accepted {
            self.add_connection(stream, peer, context)?;
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
        if self.handler.on_accept(id, peer).is_err() {
            self.close_internal(id, CloseReason::HandlerError, context);
        }
        Ok(())
    }

    fn connection_ready(
        &mut self,
        id: ConnectionId,
        event: ModuleEvent,
        context: &mut EventContext<'_>,
    ) -> Result<()> {
        if event.is_error() {
            self.close_internal(id, CloseReason::Reset, context);
            return Ok(());
        }

        let mut chunks = Vec::new();
        let mut eof = false;
        if event.is_readable() || event.is_hup() {
            let Some(connection) = self.connections.get_mut(&id) else {
                return Ok(());
            };
            let mut buffer = [0_u8; 16 * 1024];
            loop {
                match connection.socket.read(&mut buffer) {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(length) => chunks.push(buffer[..length].to_vec()),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        self.close_internal(id, CloseReason::Reset, context);
                        return Ok(());
                    }
                }
            }
        }

        for chunk in chunks {
            if self.handler.on_read(id, &chunk).is_err() {
                self.close_internal(id, CloseReason::HandlerError, context);
                return Ok(());
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
        context.reregister_source_for(connection.key, &mut connection.socket, interest)
    }

    pub fn write(&mut self, id: ConnectionId, bytes: &[u8]) -> Result<()> {
        self.config.validate()?;
        let connection = self
            .connections
            .get_mut(&id)
            .ok_or(NetError::UnknownConnection(id))?;
        if bytes.len() > self.config.max_write_queue - connection.queued_bytes {
            return Err(NetError::QueueFull.into());
        }
        if !bytes.is_empty() {
            connection.queue.push_back(bytes.to_vec());
            connection.queued_bytes += bytes.len();
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

    pub fn service(&self) -> &NetService<H> {
        &self.service
    }

    pub fn service_mut(&mut self) -> &mut NetService<H> {
        &mut self.service
    }
}

impl<H: ConnHandler> WorkerModule for NetWorker<H> {
    fn init_process(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()> {
        ctx.claim_listener(&self.listener_name)
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

//! A thin, replaceable core for generic Rust module systems.
//!
//! The crate provides explicit TOML snapshots, a single-thread mio loop,
//! worker-local RAII pools, deterministic module graphs, and homogeneous
//! worker-generation management. It does not provide an executor or business
//! routing layer.

#[path = "config/lib.rs"]
pub mod config;
#[path = "error/lib.rs"]
pub mod error;
#[path = "event/lib.rs"]
pub mod event;
#[path = "memory/lib.rs"]
pub mod memory;
#[path = "module/lib.rs"]
pub mod module;
#[path = "net/lib.rs"]
pub mod net;
#[path = "process/lib.rs"]
pub mod process;

pub use config::{
    ConfigManager, ConfigSnapshot, ConfigSource, GenerationConfig, ListenConfig, MasterConfig,
    OnAllFailed, PoolConfig, RestartConfig, RuntimeConfig, TomlConfigSource, WorkerConfig,
};
pub use error::{Error, NetError, Result};
pub use event::{
    accept_loop, EventContext, EventDispatcher, EventHandler, EventInterest, EventKey, EventLoop,
    EventLoopHandle, EventRegistrar, ModuleEvent,
};
pub use memory::{Pool, PoolFactory, PoolGuard};
pub use module::{
    ListenSpec, ListenerClaims, Module, ModuleContext, ModuleCycle, ModuleOrder, ModuleRegistry,
    ServiceRegistry, WorkerContext, WorkerEventRegistrar, WorkerEventRegistry, WorkerModule,
    WorkerModuleFactory,
};
pub use net::{
    CloseReason, ConnHandler, ConnectionId, ConnectionState, NetConfig, NetModule, NetService,
    NetWorker,
};
pub use process::{
    CommandWorkerFactory, ControlChannel, ControlFrameCodec, ExitStatus, FrameKind, GenerationId,
    GenerationManager, GenerationState, ListenerMetadata, ModuleRegistryFactory,
    PipeControlChannelFactory, ProcessEvent, WorkerCommand, WorkerEvent, WorkerFactory,
    WorkerHandle, WorkerId, WorkerMetadata, WorkerSpawnRequest, MAX_FRAME_BYTES,
};

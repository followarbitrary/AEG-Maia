use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use mio::event::Source;
#[cfg(feature = "net")]
use mio::net::TcpListener;

use crate::{
    config::{ConfigSnapshot, ListenConfig},
    error::{Error, Result},
    event::{EventContext, EventDispatcher, EventInterest, EventKey, EventRegistrar, ModuleEvent},
};

pub type WorkerId = u32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenSpec {
    pub name: String,
    pub address: String,
    pub backlog: u32,
    pub reuse_port: bool,
}

pub trait WorkerEventRegistrar: Send {
    fn claim_listener(&mut self, module: &str, name: &str) -> Result<()>;
    /// Claim and move an inherited listener into the worker module.
    ///
    /// The worker event registry owns listener handles until this method is
    /// called. This keeps raw descriptors out of configuration snapshots and
    /// prevents a module from taking a descriptor by index or by name alone.
    #[cfg(feature = "net")]
    fn claim_tcp(&mut self, module: &str, name: &str) -> Result<TcpListener> {
        let _ = (module, name);
        Err(Error::Module(
            "worker listener transfer is unavailable".into(),
        ))
    }
    fn unclaimed_listeners(&self) -> Vec<String>;
    fn claimed_listeners(&self) -> Vec<String>;

    fn register_source(
        &mut self,
        _module: &str,
        _source: &mut dyn Source,
        _interest: EventInterest,
    ) -> Result<EventKey> {
        Err(Error::Event(
            "worker event dispatcher is unavailable".into(),
        ))
    }

    fn reregister_source(
        &mut self,
        _module: &str,
        _key: EventKey,
        _source: &mut dyn Source,
        _interest: EventInterest,
    ) -> Result<()> {
        Err(Error::Event(
            "worker event dispatcher is unavailable".into(),
        ))
    }

    fn deregister_source(
        &mut self,
        _module: &str,
        _key: EventKey,
        _source: &mut dyn Source,
    ) -> Result<()> {
        Err(Error::Event(
            "worker event dispatcher is unavailable".into(),
        ))
    }

    fn validate_event_registrations(&self) -> Result<()> {
        Ok(())
    }

    fn unregister_events(&mut self) {}

    fn set_draining(&mut self, _draining: bool) {}

    fn poll_events(&mut self, _timeout: Option<Duration>) -> Result<Vec<ModuleEvent>> {
        Err(Error::Event(
            "worker event dispatcher is unavailable".into(),
        ))
    }

    fn event_context(&mut self, _key: EventKey) -> Result<EventContext<'_>> {
        Err(Error::Event(
            "worker event dispatcher is unavailable".into(),
        ))
    }

    fn event_owner(&self, _key: EventKey) -> Option<String> {
        None
    }

    fn record_event_error(&mut self, _key: EventKey) -> Result<u64> {
        Err(Error::Event(
            "worker event dispatcher is unavailable".into(),
        ))
    }

    fn remove_event(&mut self, _key: EventKey) {}

    fn active_event_count(&self) -> usize {
        0
    }
}

#[derive(Debug, Default)]
pub struct ListenerClaims {
    owners: BTreeMap<String, String>,
    required: BTreeSet<String>,
    claimed: BTreeSet<String>,
    #[cfg(feature = "net")]
    listeners: BTreeMap<String, TcpListener>,
}

impl ListenerClaims {
    pub fn new(owners: BTreeMap<String, String>) -> Self {
        Self::with_required(owners, std::iter::empty::<String>())
    }

    pub fn with_required(
        owners: BTreeMap<String, String>,
        required: impl IntoIterator<Item = String>,
    ) -> Self {
        let mut required_names = required.into_iter().collect::<BTreeSet<_>>();
        required_names.extend(owners.keys().cloned());
        Self {
            owners,
            required: required_names,
            claimed: BTreeSet::new(),
            #[cfg(feature = "net")]
            listeners: BTreeMap::new(),
        }
    }

    /// Create claims backed by listener handles inherited by a worker.
    #[cfg(feature = "net")]
    pub fn with_listeners(
        owners: BTreeMap<String, String>,
        listeners: impl IntoIterator<Item = (String, TcpListener)>,
    ) -> Self {
        let mut claims = Self::new(owners);
        claims.listeners.extend(listeners);
        claims.required.extend(claims.listeners.keys().cloned());
        claims
    }

    pub fn from_cycle(cycle: &ModuleCycle) -> Self {
        Self::new(cycle.listener_owners.clone())
    }
}

impl WorkerEventRegistrar for ListenerClaims {
    fn claim_listener(&mut self, module: &str, name: &str) -> Result<()> {
        self.claim_name(module, name)
    }

    #[cfg(feature = "net")]
    fn claim_tcp(&mut self, module: &str, name: &str) -> Result<TcpListener> {
        if !self.listeners.contains_key(name) {
            return Err(Error::Module(format!(
                "listener {name} has no inherited TCP handle"
            )));
        }
        self.claim_name(module, name)?;
        Ok(self
            .listeners
            .remove(name)
            .expect("listener handle checked before claim"))
    }

    fn unclaimed_listeners(&self) -> Vec<String> {
        self.required
            .iter()
            .filter(|name| !self.claimed.contains(*name))
            .cloned()
            .collect()
    }

    fn claimed_listeners(&self) -> Vec<String> {
        self.claimed.iter().cloned().collect()
    }
}

impl ListenerClaims {
    fn claim_name(&mut self, module: &str, name: &str) -> Result<()> {
        let owner = self.owners.get(name).ok_or_else(|| {
            Error::Module(format!(
                "module {module} attempted to claim unknown listener {name}"
            ))
        })?;
        if owner != module {
            return Err(Error::Module(format!(
                "module {module} is not the owner of listener {name}"
            )));
        }
        if !self.claimed.insert(name.to_owned()) {
            return Err(Error::Module(format!(
                "listener {name} was claimed more than once"
            )));
        }
        Ok(())
    }
}

pub struct WorkerEventRegistry {
    claims: ListenerClaims,
    dispatcher: EventDispatcher,
}

impl WorkerEventRegistry {
    pub fn new(claims: ListenerClaims, capacity: usize) -> Result<Self> {
        Ok(Self {
            claims,
            dispatcher: EventDispatcher::new(capacity)?,
        })
    }

    pub fn dispatcher(&self) -> &EventDispatcher {
        &self.dispatcher
    }
}

impl WorkerEventRegistrar for WorkerEventRegistry {
    fn claim_listener(&mut self, module: &str, name: &str) -> Result<()> {
        self.claims.claim_listener(module, name)
    }

    #[cfg(feature = "net")]
    fn claim_tcp(&mut self, module: &str, name: &str) -> Result<TcpListener> {
        self.claims.claim_tcp(module, name)
    }

    fn unclaimed_listeners(&self) -> Vec<String> {
        self.claims.unclaimed_listeners()
    }

    fn claimed_listeners(&self) -> Vec<String> {
        self.claims.claimed_listeners()
    }

    fn register_source(
        &mut self,
        module: &str,
        source: &mut dyn Source,
        interest: EventInterest,
    ) -> Result<EventKey> {
        self.dispatcher.register_source(module, source, interest)
    }

    fn reregister_source(
        &mut self,
        module: &str,
        key: EventKey,
        source: &mut dyn Source,
        interest: EventInterest,
    ) -> Result<()> {
        if self.dispatcher.owner(key) != Some(module) {
            return Err(Error::Event(format!(
                "module {module} cannot modify event key {}",
                key.raw()
            )));
        }
        self.dispatcher.reregister_source(key, source, interest)
    }

    fn deregister_source(
        &mut self,
        module: &str,
        key: EventKey,
        source: &mut dyn Source,
    ) -> Result<()> {
        if self.dispatcher.owner(key) != Some(module) {
            return Err(Error::Event(format!(
                "module {module} cannot modify event key {}",
                key.raw()
            )));
        }
        self.dispatcher.deregister_source(key, source)
    }

    fn validate_event_registrations(&self) -> Result<()> {
        self.dispatcher.validate()
    }

    fn unregister_events(&mut self) {
        self.dispatcher.clear_registrations();
    }

    fn set_draining(&mut self, draining: bool) {
        self.dispatcher.set_draining(draining);
    }

    fn poll_events(&mut self, timeout: Option<Duration>) -> Result<Vec<ModuleEvent>> {
        self.dispatcher.poll(timeout)
    }

    fn event_context(&mut self, key: EventKey) -> Result<EventContext<'_>> {
        self.dispatcher.context(key)
    }

    fn event_owner(&self, key: EventKey) -> Option<String> {
        self.dispatcher.owner(key).map(str::to_owned)
    }

    fn record_event_error(&mut self, key: EventKey) -> Result<u64> {
        self.dispatcher.record_handler_error(key)
    }

    fn remove_event(&mut self, key: EventKey) {
        self.dispatcher.remove_key(key);
    }

    fn active_event_count(&self) -> usize {
        // Listener registrations remain in place during a graceful drain so
        // they can reject new accepts.  Only registrations beyond claimed
        // listeners represent live connection handlers.
        self.dispatcher
            .registration_count()
            .saturating_sub(self.claims.claimed.len())
    }
}

#[derive(Default)]
pub struct ServiceRegistry {
    values: BTreeMap<TypeId, Arc<dyn Any + Send + Sync>>,
    providers: BTreeMap<TypeId, String>,
}

impl ServiceRegistry {
    pub fn register<T: Any + Send + Sync>(
        &mut self,
        provider: impl Into<String>,
        value: Arc<T>,
    ) -> Result<()> {
        let type_id = TypeId::of::<T>();
        if self.values.contains_key(&type_id) {
            return Err(Error::Module(format!(
                "service already registered: {}",
                std::any::type_name::<T>()
            )));
        }
        self.providers.insert(type_id, provider.into());
        self.values.insert(type_id, value);
        Ok(())
    }

    pub fn get<T: Any + Send + Sync>(&self) -> Result<Arc<T>> {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .ok_or_else(|| {
                Error::Module(format!("service not found: {}", std::any::type_name::<T>()))
            })?
            .downcast::<T>()
            .map_err(|_| {
                Error::Module(format!(
                    "service type mismatch: {}",
                    std::any::type_name::<T>()
                ))
            })
    }
}

#[derive(Default)]
pub struct ModuleContext {
    services: ServiceRegistry,
    current_module: Option<String>,
    dependencies: BTreeSet<String>,
    listeners: Vec<ListenSpec>,
    listener_owners: BTreeMap<String, String>,
}

impl ModuleContext {
    pub fn provide<T: Any + Send + Sync>(&mut self, value: Arc<T>) -> Result<()> {
        let provider = self
            .current_module
            .clone()
            .ok_or_else(|| Error::Module("module context has no current module".into()))?;
        self.services.register(provider, value)
    }

    pub fn require<T: Any + Send + Sync>(&self, provider: &str) -> Result<Arc<T>> {
        if !self.dependencies.contains(provider) {
            return Err(Error::Module(format!(
                "module {} must declare service provider {provider} in DEPS",
                self.current_module.as_deref().unwrap_or("<unknown>")
            )));
        }
        self.services.get()
    }

    pub fn services(&self) -> &ServiceRegistry {
        &self.services
    }

    pub fn listen(&mut self, name: impl Into<String>, mut spec: ListenSpec) -> Result<()> {
        let name = name.into();
        let owner = self
            .current_module
            .clone()
            .ok_or_else(|| Error::Module("module context has no current module".into()))?;
        if name != spec.name {
            return Err(Error::Module(format!(
                "listener name mismatch: argument {name}, spec {}",
                spec.name
            )));
        }
        if spec.name.trim().is_empty() || spec.address.trim().is_empty() {
            return Err(Error::Module(
                "listener name and address must not be empty".into(),
            ));
        }
        if self.listener_owners.contains_key(&name) {
            return Err(Error::Module(format!(
                "duplicate listener registration: {name}"
            )));
        }
        spec.name = name.clone();
        self.listener_owners.insert(name, owner);
        self.listeners.push(spec);
        Ok(())
    }

    fn enter(&mut self, name: &str, dependencies: &[&str]) {
        self.current_module = Some(name.to_owned());
        self.dependencies = dependencies
            .iter()
            .map(|dependency| (*dependency).to_owned())
            .collect();
    }
}

pub struct WorkerContext<'a> {
    pub worker_id: WorkerId,
    pub snapshot: &'a ConfigSnapshot,
    event_registrar: &'a mut dyn WorkerEventRegistrar,
    current_module: Option<String>,
    events_allowed: bool,
}

impl WorkerContext<'_> {
    pub fn claim_listener(&mut self, name: &str) -> Result<()> {
        let module = self
            .current_module
            .as_deref()
            .ok_or_else(|| Error::Module("worker context has no current module".into()))?;
        self.event_registrar.claim_listener(module, name)
    }

    /// Claim an inherited TCP listener and take ownership of its handle.
    #[cfg(feature = "net")]
    pub fn claim_tcp(&mut self, name: &str) -> Result<TcpListener> {
        let module = self
            .current_module
            .as_deref()
            .ok_or_else(|| Error::Module("worker context has no current module".into()))?;
        self.event_registrar.claim_tcp(module, name)
    }

    pub fn register_source<S: Source>(
        &mut self,
        source: &mut S,
        interest: EventInterest,
    ) -> Result<EventKey> {
        if !self.events_allowed {
            return Err(Error::Module(
                "event registration is only allowed during register_events or start".into(),
            ));
        }
        let module = self
            .current_module
            .as_deref()
            .ok_or_else(|| Error::Module("worker context has no current module".into()))?;
        self.event_registrar
            .register_source(module, source, interest)
    }

    pub fn reregister_source<S: Source>(
        &mut self,
        key: EventKey,
        source: &mut S,
        interest: EventInterest,
    ) -> Result<()> {
        if !self.events_allowed {
            return Err(Error::Module(
                "event registration is only allowed during register_events or start".into(),
            ));
        }
        let module = self
            .current_module
            .as_deref()
            .ok_or_else(|| Error::Module("worker context has no current module".into()))?;
        self.event_registrar
            .reregister_source(module, key, source, interest)
    }

    pub fn deregister_source<S: Source>(&mut self, key: EventKey, source: &mut S) -> Result<()> {
        let module = self
            .current_module
            .as_deref()
            .ok_or_else(|| Error::Module("worker context has no current module".into()))?;
        self.event_registrar.deregister_source(module, key, source)
    }
}

pub trait Module: Send {
    fn name(&self) -> &str;
    fn dependencies(&self) -> &[&str];
    fn init_module(&mut self, ctx: &mut ModuleContext) -> Result<()>;
    fn exit_master(&mut self, ctx: &mut ModuleContext) -> Result<()>;
}

pub trait WorkerModule: Send {
    fn init_process(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()>;
    fn register_events(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        Ok(())
    }
    fn start(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()>;
    fn on_event(&mut self, _event: ModuleEvent, _ctx: &mut EventContext<'_>) -> Result<()> {
        Ok(())
    }
    fn stop(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()>;
    fn exit_process(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()>;
}

pub trait WorkerModuleFactory: Send + Sync {
    fn build(
        &self,
        name: &str,
        worker_id: WorkerId,
        snapshot: &ConfigSnapshot,
    ) -> Result<Box<dyn WorkerModule>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleOrder {
    names: Vec<String>,
}

impl ModuleOrder {
    pub fn names(&self) -> Vec<&str> {
        self.names.iter().map(String::as_str).collect()
    }

    fn iter(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }
}

pub struct ModuleRegistry {
    modules: BTreeMap<String, Box<dyn Module>>,
}

impl Default for ModuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self {
            modules: BTreeMap::new(),
        }
    }

    pub fn register(&mut self, module: Box<dyn Module>) -> Result<()> {
        let name = module.name().to_owned();
        if name.trim().is_empty() {
            return Err(Error::Module("module name must not be empty".into()));
        }
        if self.modules.contains_key(&name) {
            return Err(Error::Module(format!("duplicate module name: {name}")));
        }
        self.modules.insert(name, module);
        Ok(())
    }

    pub fn validate(&self) -> Result<ModuleOrder> {
        for (name, module) in &self.modules {
            let mut seen_dependencies = BTreeSet::new();
            for dependency in module.dependencies() {
                if !self.modules.contains_key(*dependency) {
                    return Err(Error::Module(format!(
                        "module {name} depends on missing module {dependency}"
                    )));
                }
                if !seen_dependencies.insert(*dependency) {
                    return Err(Error::Module(format!(
                        "module {name} has duplicate dependency {dependency}"
                    )));
                }
            }
        }

        let mut indegree: BTreeMap<String, usize> =
            self.modules.keys().map(|name| (name.clone(), 0)).collect();
        let mut dependents: BTreeMap<String, BTreeSet<String>> = self
            .modules
            .keys()
            .map(|name| (name.clone(), BTreeSet::new()))
            .collect();
        for (name, module) in &self.modules {
            for dependency in module.dependencies() {
                *indegree.get_mut(name).expect("module exists") += 1;
                dependents
                    .get_mut(*dependency)
                    .expect("validated dependency")
                    .insert(name.clone());
            }
        }

        let mut ready: BTreeSet<String> = indegree
            .iter()
            .filter_map(|(name, degree)| (*degree == 0).then_some(name.clone()))
            .collect();
        let mut order = Vec::with_capacity(self.modules.len());
        while let Some(name) = ready.pop_first() {
            order.push(name.clone());
            for dependent in dependents.get(&name).expect("module exists") {
                let degree = indegree.get_mut(dependent).expect("module exists");
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(dependent.clone());
                }
            }
        }

        if order.len() != self.modules.len() {
            let cycle = self.find_cycle().unwrap_or_default();
            return Err(Error::Module(format!(
                "module dependency cycle: {}",
                cycle.join(" -> ")
            )));
        }
        Ok(ModuleOrder { names: order })
    }

    pub fn build_cycle(self) -> Result<ModuleCycle> {
        let order = self.validate()?;
        Ok(ModuleCycle {
            modules: self.modules,
            order,
            initialized: Vec::new(),
            listeners: Vec::new(),
            listener_owners: BTreeMap::new(),
        })
    }

    fn find_cycle(&self) -> Option<Vec<String>> {
        fn visit(
            name: &str,
            modules: &BTreeMap<String, Box<dyn Module>>,
            states: &mut BTreeMap<String, VisitState>,
            stack: &mut Vec<String>,
        ) -> Option<Vec<String>> {
            states.insert(name.to_owned(), VisitState::Visiting);
            stack.push(name.to_owned());
            let mut dependencies: Vec<&str> = modules
                .get(name)
                .expect("module exists")
                .dependencies()
                .to_vec();
            dependencies.sort_unstable();
            for dependency in dependencies {
                match states.get(dependency) {
                    Some(VisitState::Visiting) => {
                        let start = stack
                            .iter()
                            .position(|item| item == dependency)
                            .unwrap_or(0);
                        let mut cycle = stack[start..].to_vec();
                        cycle.push(dependency.to_owned());
                        return Some(cycle);
                    }
                    Some(VisitState::Done) => continue,
                    None => {
                        if let Some(cycle) = visit(dependency, modules, states, stack) {
                            return Some(cycle);
                        }
                    }
                }
            }
            stack.pop();
            states.insert(name.to_owned(), VisitState::Done);
            None
        }

        let mut states = BTreeMap::new();
        let mut stack = Vec::new();
        for name in self.modules.keys() {
            if !states.contains_key(name) {
                if let Some(cycle) = visit(name, &self.modules, &mut states, &mut stack) {
                    return Some(cycle);
                }
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
enum VisitState {
    Visiting,
    Done,
}

pub struct ModuleCycle {
    modules: BTreeMap<String, Box<dyn Module>>,
    order: ModuleOrder,
    initialized: Vec<String>,
    listeners: Vec<ListenSpec>,
    listener_owners: BTreeMap<String, String>,
}

impl ModuleCycle {
    pub fn order(&self) -> &ModuleOrder {
        &self.order
    }

    /// Return the master-derived listener ownership table for worker startup.
    ///
    /// Workers receive this immutable snapshot as metadata and never replay
    /// master `init_module` hooks just to reconstruct ownership.
    pub fn listener_owners(&self) -> &BTreeMap<String, String> {
        &self.listener_owners
    }

    pub fn listeners(&self) -> &[ListenSpec] {
        &self.listeners
    }

    pub fn validate_listener_config(&self, configured: &[ListenConfig]) -> Result<()> {
        if configured.len() != self.listeners.len() {
            return Err(Error::Module(format!(
                "configured listener count {} does not match module registrations {}",
                configured.len(),
                self.listeners.len()
            )));
        }
        for spec in &self.listeners {
            let Some(configured) = configured.iter().find(|item| item.name == spec.name) else {
                return Err(Error::Module(format!(
                    "listener {} is not present in configuration",
                    spec.name
                )));
            };
            if configured.address != spec.address {
                return Err(Error::Module(format!(
                    "listener {} address mismatch: configured {}, module {}",
                    spec.name, configured.address, spec.address
                )));
            }
        }
        Ok(())
    }

    pub fn init_module(&mut self) -> Result<()> {
        let mut ctx = ModuleContext::default();
        let names = self.order.names.to_vec();
        for name in names {
            let dependencies = self
                .modules
                .get(&name)
                .expect("ordered module exists")
                .dependencies()
                .to_vec();
            ctx.enter(&name, &dependencies);
            let result = self
                .modules
                .get_mut(&name)
                .expect("ordered module exists")
                .init_module(&mut ctx);
            if let Err(error) = result {
                let cleanup_errors = self.exit_master_internal(&mut ctx);
                return Err(combine_errors(error, cleanup_errors));
            }
            self.initialized.push(name);
        }
        self.listeners = ctx.listeners;
        self.listener_owners = ctx.listener_owners;
        Ok(())
    }

    pub fn exit_master(&mut self) -> Result<()> {
        let mut ctx = ModuleContext::default();
        let errors = self.exit_master_internal(&mut ctx);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Module(errors.join("; ")))
        }
    }

    fn exit_master_internal(&mut self, ctx: &mut ModuleContext) -> Vec<String> {
        let mut errors = Vec::new();
        while let Some(name) = self.initialized.pop() {
            let dependencies = self
                .modules
                .get(&name)
                .expect("initialized module exists")
                .dependencies()
                .to_vec();
            ctx.enter(&name, &dependencies);
            if let Err(error) = self
                .modules
                .get_mut(&name)
                .expect("initialized module exists")
                .exit_master(ctx)
            {
                errors.push(format!("{name}: {error}"));
            }
        }
        errors
    }

    pub fn build_worker(
        &self,
        factory: &dyn WorkerModuleFactory,
        worker_id: WorkerId,
        snapshot: &ConfigSnapshot,
    ) -> Result<WorkerCycle> {
        self.build_worker_with_registrar(
            factory,
            worker_id,
            snapshot,
            Box::new(ListenerClaims::from_cycle(self)),
        )
    }

    pub fn build_worker_with_registrar(
        &self,
        factory: &dyn WorkerModuleFactory,
        worker_id: WorkerId,
        snapshot: &ConfigSnapshot,
        event_registrar: Box<dyn WorkerEventRegistrar>,
    ) -> Result<WorkerCycle> {
        let mut modules = BTreeMap::new();
        for name in self.order.iter() {
            let module = factory.build(name, worker_id, snapshot)?;
            modules.insert(name.to_owned(), module);
        }
        Ok(WorkerCycle {
            modules,
            order: self.order.clone(),
            initialized: Vec::new(),
            started: Vec::new(),
            worker_id,
            snapshot: snapshot.clone(),
            event_registrar,
        })
    }
}

pub struct WorkerCycle {
    modules: BTreeMap<String, Box<dyn WorkerModule>>,
    order: ModuleOrder,
    initialized: Vec<String>,
    started: Vec<String>,
    worker_id: WorkerId,
    snapshot: ConfigSnapshot,
    event_registrar: Box<dyn WorkerEventRegistrar>,
}

impl WorkerCycle {
    pub fn init_process(&mut self) -> Result<()> {
        let snapshot = self.snapshot.clone();
        let names = self.order.names.to_vec();
        for name in names {
            let result = {
                let mut ctx = WorkerContext {
                    worker_id: self.worker_id,
                    snapshot: &snapshot,
                    event_registrar: self.event_registrar.as_mut(),
                    current_module: Some(name.clone()),
                    events_allowed: false,
                };
                self.modules
                    .get_mut(&name)
                    .expect("ordered worker module exists")
                    .init_process(&mut ctx)
            };
            if let Err(error) = result {
                let cleanup_errors = self.exit_process_internal();
                return Err(combine_errors(error, cleanup_errors));
            }
            self.initialized.push(name);
        }
        Ok(())
    }

    pub fn register_events(&mut self) -> Result<()> {
        let snapshot = self.snapshot.clone();
        let names = self.order.names.to_vec();
        for name in names {
            let result = {
                let mut ctx = WorkerContext {
                    worker_id: self.worker_id,
                    snapshot: &snapshot,
                    event_registrar: self.event_registrar.as_mut(),
                    current_module: Some(name.clone()),
                    events_allowed: true,
                };
                self.modules
                    .get_mut(&name)
                    .expect("ordered worker module exists")
                    .register_events(&mut ctx)
            };
            if let Err(error) = result {
                self.event_registrar.unregister_events();
                let cleanup_errors = self.exit_process_internal();
                return Err(combine_errors(error, cleanup_errors));
            }
        }
        Ok(())
    }

    pub fn validate_event_registrations(&self) -> Result<()> {
        self.event_registrar.validate_event_registrations()
    }

    pub fn poll_events(&mut self, timeout: Option<Duration>) -> Result<Vec<ModuleEvent>> {
        self.event_registrar.poll_events(timeout)
    }

    pub fn dispatch_event(&mut self, event: ModuleEvent) -> Result<()> {
        let Some(owner) = self.event_registrar.event_owner(event.key()) else {
            return Ok(());
        };
        let result = {
            let mut context = self.event_registrar.event_context(event.key())?;
            self.modules
                .get_mut(&owner)
                .ok_or_else(|| Error::Module(format!("event owner module not found: {owner}")))?
                .on_event(event, &mut context)
        };
        if result.is_err() {
            let _ = self.event_registrar.record_event_error(event.key());
            self.event_registrar.remove_event(event.key());
        }
        Ok(())
    }

    pub fn unregister_events(&mut self) {
        self.event_registrar.unregister_events();
    }

    pub fn set_draining(&mut self, draining: bool) {
        self.event_registrar.set_draining(draining);
    }

    pub fn has_active_handlers(&self) -> bool {
        self.event_registrar.active_event_count() > 0
    }

    pub fn start(&mut self) -> Result<()> {
        let snapshot = self.snapshot.clone();
        let names = self.order.names.to_vec();
        for name in names {
            let result = {
                let mut ctx = WorkerContext {
                    worker_id: self.worker_id,
                    snapshot: &snapshot,
                    event_registrar: self.event_registrar.as_mut(),
                    current_module: Some(name.clone()),
                    events_allowed: true,
                };
                self.modules
                    .get_mut(&name)
                    .expect("ordered worker module exists")
                    .start(&mut ctx)
            };
            if let Err(error) = result {
                let cleanup_errors = self.stop_internal();
                return Err(combine_errors(error, cleanup_errors));
            }
            self.started.push(name);
        }
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let errors = self.stop_internal();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Module(errors.join("; ")))
        }
    }

    fn stop_internal(&mut self) -> Vec<String> {
        let snapshot = self.snapshot.clone();
        let mut errors = Vec::new();
        while let Some(name) = self.started.pop() {
            let result = {
                let mut ctx = WorkerContext {
                    worker_id: self.worker_id,
                    snapshot: &snapshot,
                    event_registrar: self.event_registrar.as_mut(),
                    current_module: Some(name.clone()),
                    events_allowed: false,
                };
                self.modules
                    .get_mut(&name)
                    .expect("started worker module exists")
                    .stop(&mut ctx)
            };
            if let Err(error) = result {
                errors.push(format!("{name}: {error}"));
            }
        }
        errors
    }

    pub fn exit_process(&mut self) -> Result<()> {
        let errors = self.exit_process_internal();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Module(errors.join("; ")))
        }
    }

    fn exit_process_internal(&mut self) -> Vec<String> {
        let snapshot = self.snapshot.clone();
        let mut errors = Vec::new();
        while let Some(name) = self.initialized.pop() {
            let result = {
                let mut ctx = WorkerContext {
                    worker_id: self.worker_id,
                    snapshot: &snapshot,
                    event_registrar: self.event_registrar.as_mut(),
                    current_module: Some(name.clone()),
                    events_allowed: false,
                };
                self.modules
                    .get_mut(&name)
                    .expect("initialized worker module exists")
                    .exit_process(&mut ctx)
            };
            if let Err(error) = result {
                errors.push(format!("{name}: {error}"));
            }
        }
        errors
    }

    pub fn ensure_all_listeners_claimed(&self) -> Result<()> {
        let unclaimed = self.event_registrar.unclaimed_listeners();
        if unclaimed.is_empty() {
            Ok(())
        } else {
            Err(Error::Module(format!(
                "unclaimed listeners: {}",
                unclaimed.join(", ")
            )))
        }
    }

    pub fn claimed_listeners(&self) -> Vec<String> {
        self.event_registrar.claimed_listeners()
    }
}

fn combine_errors(error: Error, cleanup_errors: Vec<String>) -> Error {
    if cleanup_errors.is_empty() {
        error
    } else {
        Error::Module(format!("{error}; cleanup: {}", cleanup_errors.join("; ")))
    }
}

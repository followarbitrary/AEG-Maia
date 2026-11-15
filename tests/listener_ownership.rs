use aeg_maia::{
    ConfigSnapshot, ListenConfig, ListenSpec, ListenerClaims, Module, ModuleContext,
    ModuleRegistry, Result, WorkerContext, WorkerModule, WorkerModuleFactory,
};

struct ListenerModule {
    name: &'static str,
    duplicate_registration: bool,
}

impl Module for ListenerModule {
    fn name(&self) -> &str {
        self.name
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn init_module(&mut self, ctx: &mut ModuleContext) -> Result<()> {
        if self.name == "owner" {
            ctx.listen(
                "main",
                ListenSpec {
                    name: "main".into(),
                    address: "127.0.0.1:8080".into(),
                    backlog: 1024,
                    reuse_port: false,
                },
            )?;
            if self.duplicate_registration {
                ctx.listen(
                    "main",
                    ListenSpec {
                        name: "main".into(),
                        address: "127.0.0.1:8080".into(),
                        backlog: 1024,
                        reuse_port: false,
                    },
                )?;
            }
        }
        Ok(())
    }

    fn exit_master(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }
}

struct ClaimingWorker {
    claim: Option<&'static str>,
}

impl WorkerModule for ClaimingWorker {
    fn init_process(&mut self, ctx: &mut WorkerContext<'_>) -> Result<()> {
        if let Some(name) = self.claim {
            ctx.claim_listener(name)?;
            if name == "duplicate" {
                ctx.claim_listener(name)?;
            }
        }
        Ok(())
    }

    fn start(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        Ok(())
    }
    fn stop(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        Ok(())
    }
    fn exit_process(&mut self, _ctx: &mut WorkerContext<'_>) -> Result<()> {
        Ok(())
    }
}

struct WorkerFactoryForTest {
    claim: Option<&'static str>,
}

impl WorkerModuleFactory for WorkerFactoryForTest {
    fn build(
        &self,
        _name: &str,
        _worker_id: u32,
        _snapshot: &ConfigSnapshot,
    ) -> Result<Box<dyn WorkerModule>> {
        Ok(Box::new(ClaimingWorker { claim: self.claim }))
    }
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot::from_toml(
        "[worker]\nprogram='/bin/worker'\n[[listen]]\nname='main'\naddress='127.0.0.1:8080'\n",
    )
    .unwrap()
}

#[test]
fn rejects_duplicate_master_listener_registration() {
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(ListenerModule {
            name: "owner",
            duplicate_registration: true,
        }))
        .unwrap();
    let mut cycle = registry.build_cycle().unwrap();
    let error = cycle
        .init_module()
        .expect_err("duplicate listener must fail");
    assert!(error.to_string().contains("listener"));
}

#[test]
fn validates_module_listener_declarations_against_config() {
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(ListenerModule {
            name: "owner",
            duplicate_registration: false,
        }))
        .unwrap();
    let mut cycle = registry.build_cycle().unwrap();
    cycle.init_module().unwrap();
    assert!(cycle
        .validate_listener_config(&[ListenConfig {
            name: "other".into(),
            address: "127.0.0.1:8080".into(),
        }])
        .is_err());
}

#[test]
fn rejects_non_owner_duplicate_and_unclaimed_worker_claims() {
    let mut registry = ModuleRegistry::new();
    registry
        .register(Box::new(ListenerModule {
            name: "owner",
            duplicate_registration: false,
        }))
        .unwrap();
    let mut cycle = registry.build_cycle().unwrap();
    cycle.init_module().unwrap();
    assert_eq!(
        cycle.listener_owners().get("main").map(String::as_str),
        Some("owner")
    );

    let snapshot = config();
    let mut non_owner = cycle
        .build_worker_with_registrar(
            &WorkerFactoryForTest {
                claim: Some("missing"),
            },
            0,
            &snapshot,
            Box::new(ListenerClaims::from_cycle(&cycle)),
        )
        .unwrap();
    assert!(non_owner.init_process().is_err());

    let mut duplicate = cycle
        .build_worker_with_registrar(
            &WorkerFactoryForTest {
                claim: Some("duplicate"),
            },
            0,
            &snapshot,
            Box::new(ListenerClaims::from_cycle(&cycle)),
        )
        .unwrap();
    assert!(duplicate.init_process().is_err());

    let mut unclaimed = cycle
        .build_worker_with_registrar(
            &WorkerFactoryForTest { claim: None },
            0,
            &snapshot,
            Box::new(ListenerClaims::from_cycle(&cycle)),
        )
        .unwrap();
    unclaimed.init_process().unwrap();
    assert!(unclaimed.ensure_all_listeners_claimed().is_err());
}

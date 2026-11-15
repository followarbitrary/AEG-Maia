use std::fs;
use std::time::Duration;

use aeg_maia::{
    ConfigManager, ConfigSnapshot, ConfigSource, EventLoop, Module, ModuleContext, ModuleRegistry,
    Pool, PoolConfig, PoolFactory, Result, TomlConfigSource,
};
use mio::event::Event;
use tempfile::tempdir;

struct ItemFactory;

impl PoolFactory<String> for ItemFactory {
    fn create(&mut self) -> Result<String> {
        Ok(String::from("item"))
    }
    fn reset(&mut self, value: &mut String) -> Result<()> {
        value.clear();
        value.push_str("item");
        Ok(())
    }
}

struct ExampleModule;

impl Module for ExampleModule {
    fn name(&self) -> &str {
        "example"
    }
    fn dependencies(&self) -> &[&str] {
        &[]
    }
    fn init_module(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }
    fn exit_master(&mut self, _ctx: &mut ModuleContext) -> Result<()> {
        Ok(())
    }
}

struct StopHandler;

impl aeg_maia::EventHandler for StopHandler {
    fn on_event(&mut self, _event: &Event) -> Result<()> {
        Ok(())
    }
}

#[test]
fn public_api_supports_config_pool_event_and_registry_flow() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[worker]\nprogram='/bin/worker'\ncount=1\n").unwrap();
    let source = TomlConfigSource::new(&path);
    let initial = source.load().unwrap();
    let mut manager = ConfigManager::new(source, initial.clone()).unwrap();
    fs::write(&path, "[worker]\nprogram='/bin/worker'\ncount=2\n").unwrap();
    assert_eq!(manager.reload().unwrap().worker.count, 2);
    fs::write(&path, "[worker\ninvalid").unwrap();
    assert!(manager.reload().is_err());
    assert_eq!(manager.current().worker.count, 2);

    let pool = Pool::new(
        PoolConfig {
            initial: 1,
            soft_max: 1,
            hard_max: 1,
            idle_timeout_secs: 1,
        },
        ItemFactory,
    )
    .unwrap();
    let item = pool.checkout().unwrap();
    assert_eq!(&*item, "item");
    drop(item);
    pool.maintain(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();

    let mut event_loop = EventLoop::new(4).unwrap();
    event_loop.stop();
    event_loop.run(&mut StopHandler).unwrap();

    let mut registry = ModuleRegistry::new();
    registry.register(Box::new(ExampleModule)).unwrap();
    assert_eq!(registry.validate().unwrap().names(), vec!["example"]);

    let _: ConfigSnapshot = manager.current().clone();
}

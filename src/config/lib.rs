use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConfigSnapshot {
    pub master: MasterConfig,
    #[serde(rename = "listen")]
    pub listeners: Vec<ListenConfig>,
    pub worker: WorkerConfig,
    pub memory: PoolConfig,
    pub generation: GenerationConfig,
    pub restart: RestartConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MasterConfig {
    #[serde(default)]
    pub pid_file: Option<PathBuf>,
    #[serde(default)]
    pub log_file: Option<PathBuf>,
    #[serde(default)]
    pub daemon: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenConfig {
    pub name: String,
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "default_worker_count")]
    pub count: u32,
    #[serde(default, skip_serializing)]
    pub listener_fds: Vec<i32>,
    #[serde(default)]
    pub net: WorkerNetConfig,
}

/// Configuration passed to network worker modules. Keeping this nested under
/// `worker` makes the module's limits explicit in the snapshot instead of
/// silently relying on runtime defaults.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerNetConfig {
    #[serde(default = "default_net_max_write_queue")]
    pub max_write_queue: usize,
    #[serde(default = "default_net_max_read_buffer")]
    pub max_read_buffer: usize,
    #[serde(default = "default_net_max_total_write_queue")]
    pub max_total_write_queue: usize,
    #[serde(default = "default_net_high_watermark")]
    pub high_watermark: usize,
    #[serde(default = "default_net_low_watermark")]
    pub low_watermark: usize,
    #[serde(default)]
    pub idle_timeout_secs: u64,
    #[serde(default)]
    pub write_timeout_secs: u64,
}

impl Default for WorkerNetConfig {
    fn default() -> Self {
        Self {
            max_write_queue: default_net_max_write_queue(),
            max_read_buffer: default_net_max_read_buffer(),
            max_total_write_queue: default_net_max_total_write_queue(),
            high_watermark: default_net_high_watermark(),
            low_watermark: default_net_low_watermark(),
            idle_timeout_secs: 0,
            write_timeout_secs: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GenerationConfig {
    pub min_ready: u32,
    pub ready_timeout_secs: u64,
    pub drain_timeout_secs: u64,
    pub kill_timeout_secs: u64,
    pub handoff_timeout_secs: u64,
    pub max_draining: usize,
    pub on_all_failed: OnAllFailed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RestartConfig {
    pub max_restarts: u32,
    pub window_secs: u64,
    pub backoff_initial_ms: u64,
    pub backoff_max_secs: u64,
    pub jitter_ratio_percent: u8,
    pub stable_after_secs: u64,
    pub startup_failures: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    #[serde(default)]
    pub initial: usize,
    #[serde(default = "default_soft_max")]
    pub soft_max: usize,
    #[serde(default = "default_hard_max")]
    pub hard_max: usize,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnAllFailed {
    Exit,
    Keep,
}

pub type RuntimeConfig = ConfigSnapshot;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    master: MasterConfig,
    worker: WorkerConfig,
    #[serde(default)]
    listen: Vec<ListenConfig>,
    #[serde(default)]
    memory: RawPoolConfig,
    #[serde(default)]
    generation: RawGenerationConfig,
    #[serde(default)]
    restart: RawRestartConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPoolConfig {
    #[serde(default)]
    initial: usize,
    #[serde(default = "default_soft_max")]
    soft_max: usize,
    #[serde(default = "default_hard_max")]
    hard_max: usize,
    #[serde(default = "default_idle_timeout_secs")]
    idle_timeout_secs: u64,
}

impl Default for RawPoolConfig {
    fn default() -> Self {
        Self {
            initial: 0,
            soft_max: default_soft_max(),
            hard_max: default_hard_max(),
            idle_timeout_secs: default_idle_timeout_secs(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGenerationConfig {
    min_ready: Option<u32>,
    #[serde(default = "default_ready_timeout_secs")]
    ready_timeout_secs: u64,
    #[serde(default = "default_drain_timeout_secs")]
    drain_timeout_secs: u64,
    #[serde(default = "default_kill_timeout_secs")]
    kill_timeout_secs: u64,
    #[serde(default = "default_handoff_timeout_secs", alias = "handoff_timeout")]
    handoff_timeout_secs: u64,
    #[serde(default = "default_max_draining")]
    max_draining: usize,
    #[serde(default = "default_on_all_failed")]
    on_all_failed: OnAllFailed,
}

impl Default for RawGenerationConfig {
    fn default() -> Self {
        Self {
            min_ready: None,
            ready_timeout_secs: default_ready_timeout_secs(),
            drain_timeout_secs: default_drain_timeout_secs(),
            kill_timeout_secs: default_kill_timeout_secs(),
            handoff_timeout_secs: default_handoff_timeout_secs(),
            max_draining: default_max_draining(),
            on_all_failed: default_on_all_failed(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRestartConfig {
    #[serde(default = "default_max_restarts")]
    max_restarts: u32,
    #[serde(default = "default_window_secs")]
    window_secs: u64,
    #[serde(default = "default_backoff_initial_ms")]
    backoff_initial_ms: u64,
    #[serde(default = "default_backoff_max_secs")]
    backoff_max_secs: u64,
    #[serde(default = "default_jitter_ratio_percent")]
    jitter_ratio_percent: u8,
    #[serde(default = "default_stable_after_secs")]
    stable_after_secs: u64,
    #[serde(default = "default_startup_failures")]
    startup_failures: u32,
}

impl Default for RawRestartConfig {
    fn default() -> Self {
        Self {
            max_restarts: default_max_restarts(),
            window_secs: default_window_secs(),
            backoff_initial_ms: default_backoff_initial_ms(),
            backoff_max_secs: default_backoff_max_secs(),
            jitter_ratio_percent: default_jitter_ratio_percent(),
            stable_after_secs: default_stable_after_secs(),
            startup_failures: default_startup_failures(),
        }
    }
}

pub trait ConfigSource {
    fn load(&self) -> Result<ConfigSnapshot>;
}

#[derive(Clone, Debug)]
pub struct TomlConfigSource {
    path: PathBuf,
}

impl TomlConfigSource {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl ConfigSource for TomlConfigSource {
    fn load(&self) -> Result<ConfigSnapshot> {
        let contents = fs::read_to_string(&self.path)?;
        ConfigSnapshot::from_toml(&contents)
    }
}

pub struct ConfigManager<S> {
    source: S,
    current: ConfigSnapshot,
}

impl<S: ConfigSource> ConfigManager<S> {
    pub fn new(source: S, initial: ConfigSnapshot) -> Result<Self> {
        initial.validate()?;
        Ok(Self {
            source,
            current: initial,
        })
    }

    pub fn current(&self) -> &ConfigSnapshot {
        &self.current
    }

    pub fn reload(&mut self) -> Result<ConfigSnapshot> {
        let next = self.source.load()?;
        self.current = next.clone();
        Ok(next)
    }

    pub fn prepare_reload(&self) -> Result<ConfigSnapshot> {
        self.source.load()
    }

    pub fn commit(&mut self, snapshot: ConfigSnapshot) -> Result<()> {
        snapshot.validate()?;
        self.current = snapshot;
        Ok(())
    }

    pub fn reload_with<E>(&mut self, apply: E) -> Result<ConfigSnapshot>
    where
        E: FnOnce(&ConfigSnapshot) -> Result<()>,
    {
        let next = self.prepare_reload()?;
        apply(&next)?;
        self.commit(next.clone())?;
        Ok(next)
    }
}

impl ConfigSnapshot {
    pub fn from_toml(contents: &str) -> Result<Self> {
        let raw: RawConfig =
            toml::from_str(contents).map_err(|error| Error::Config(error.to_string()))?;
        let snapshot = Self {
            master: raw.master,
            listeners: raw.listen,
            generation: GenerationConfig {
                min_ready: raw.generation.min_ready.unwrap_or(raw.worker.count),
                ready_timeout_secs: raw.generation.ready_timeout_secs,
                drain_timeout_secs: raw.generation.drain_timeout_secs,
                kill_timeout_secs: raw.generation.kill_timeout_secs,
                handoff_timeout_secs: raw.generation.handoff_timeout_secs,
                max_draining: raw.generation.max_draining,
                on_all_failed: raw.generation.on_all_failed,
            },
            restart: RestartConfig {
                max_restarts: raw.restart.max_restarts,
                window_secs: raw.restart.window_secs,
                backoff_initial_ms: raw.restart.backoff_initial_ms,
                backoff_max_secs: raw.restart.backoff_max_secs,
                jitter_ratio_percent: raw.restart.jitter_ratio_percent,
                stable_after_secs: raw.restart.stable_after_secs,
                startup_failures: raw.restart.startup_failures,
            },
            memory: PoolConfig {
                initial: raw.memory.initial,
                soft_max: raw.memory.soft_max,
                hard_max: raw.memory.hard_max,
                idle_timeout_secs: raw.memory.idle_timeout_secs,
            },
            worker: raw.worker,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<()> {
        let mut listener_names = std::collections::BTreeSet::new();
        for listener in &self.listeners {
            if listener.name.trim().is_empty() {
                return Err(Error::Config("listener.name must not be empty".into()));
            }
            if listener.address.trim().is_empty() {
                return Err(Error::Config(format!(
                    "listener {} address must not be empty",
                    listener.name
                )));
            }
            if !listener_names.insert(&listener.name) {
                return Err(Error::Config(format!(
                    "duplicate listener name: {}",
                    listener.name
                )));
            }
        }
        if self.worker.program.trim().is_empty() {
            return Err(Error::Config("worker.program must not be empty".into()));
        }
        if self.worker.count == 0 {
            return Err(Error::Config(
                "worker.count must be greater than zero".into(),
            ));
        }
        if self.generation.min_ready == 0 || self.generation.min_ready > self.worker.count {
            return Err(Error::Config(format!(
                "generation.min_ready must be between 1 and worker.count ({})",
                self.worker.count
            )));
        }
        if self.memory.initial > self.memory.soft_max {
            return Err(Error::Config(
                "memory.initial must not exceed memory.soft_max".into(),
            ));
        }
        if self.memory.soft_max > self.memory.hard_max {
            return Err(Error::Config(
                "memory.soft_max must not exceed memory.hard_max".into(),
            ));
        }
        if self.memory.hard_max == 0 {
            return Err(Error::Config(
                "memory.hard_max must be greater than zero".into(),
            ));
        }
        if self.worker.net.max_write_queue == 0
            || self.worker.net.max_read_buffer == 0
            || self.worker.net.max_total_write_queue == 0
            || self.worker.net.low_watermark > self.worker.net.high_watermark
            || self.worker.net.high_watermark > self.worker.net.max_write_queue
        {
            return Err(Error::Config(
                "worker.net has invalid read buffer or write queue limits".into(),
            ));
        }
        if self.restart.max_restarts == 0 || self.restart.window_secs == 0 {
            return Err(Error::Config(
                "restart budget and window must be greater than zero".into(),
            ));
        }
        if self.restart.backoff_initial_ms == 0
            || self.restart.backoff_max_secs == 0
            || self.restart.stable_after_secs == 0
            || self.restart.startup_failures == 0
        {
            return Err(Error::Config(
                "restart timing and startup budget must be greater than zero".into(),
            ));
        }
        if self.restart.jitter_ratio_percent > 100 {
            return Err(Error::Config(
                "restart.jitter_ratio_percent must be at most 100".into(),
            ));
        }
        if self.generation.max_draining == 0 {
            return Err(Error::Config(
                "generation.max_draining must be greater than zero".into(),
            ));
        }
        if self.generation.ready_timeout_secs == 0
            || self.generation.kill_timeout_secs == 0
            || self.generation.handoff_timeout_secs == 0
        {
            return Err(Error::Config(
                "ready_timeout_secs, kill_timeout_secs, and handoff_timeout_secs must be greater than zero".into(),
            ));
        }
        Ok(())
    }

    pub fn listener_identity(&self) -> Vec<(String, String)> {
        self.listeners
            .iter()
            .map(|listener| (listener.name.clone(), listener.address.clone()))
            .collect()
    }
}

const fn default_worker_count() -> u32 {
    1
}
const fn default_soft_max() -> usize {
    8
}
const fn default_hard_max() -> usize {
    16
}
const fn default_idle_timeout_secs() -> u64 {
    60
}
const fn default_ready_timeout_secs() -> u64 {
    10
}
const fn default_drain_timeout_secs() -> u64 {
    30
}
const fn default_kill_timeout_secs() -> u64 {
    5
}
const fn default_handoff_timeout_secs() -> u64 {
    10
}
const fn default_max_draining() -> usize {
    2
}
const fn default_on_all_failed() -> OnAllFailed {
    OnAllFailed::Exit
}
const fn default_max_restarts() -> u32 {
    5
}
const fn default_window_secs() -> u64 {
    60
}
const fn default_backoff_initial_ms() -> u64 {
    100
}
const fn default_backoff_max_secs() -> u64 {
    10
}
const fn default_jitter_ratio_percent() -> u8 {
    20
}
const fn default_stable_after_secs() -> u64 {
    60
}
const fn default_startup_failures() -> u32 {
    3
}
const fn default_net_max_write_queue() -> usize {
    1024 * 1024
}
const fn default_net_max_read_buffer() -> usize {
    16 * 1024
}
const fn default_net_max_total_write_queue() -> usize {
    8 * 1024 * 1024
}
const fn default_net_high_watermark() -> usize {
    768 * 1024
}
const fn default_net_low_watermark() -> usize {
    256 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn parses_minimal_toml_and_applies_defaults() {
        let snapshot = ConfigSnapshot::from_toml(
            r#"
                [worker]
                program = "/bin/worker"
                count = 2
                [memory]
                initial = 1
                soft_max = 4
                hard_max = 8
            "#,
        )
        .expect("valid config");

        assert_eq!(snapshot.worker.count, 2);
        assert_eq!(snapshot.generation.min_ready, 2);
        assert_eq!(snapshot.generation.ready_timeout_secs, 10);
        assert_eq!(snapshot.restart.max_restarts, 5);
        assert_eq!(snapshot.restart.window_secs, 60);
        assert_eq!(snapshot.memory.hard_max, 8);
    }

    #[test]
    fn rejects_unknown_fields() {
        let error = ConfigSnapshot::from_toml(
            r#"
                [worker]
                program = "/bin/worker"
                count = 1
                surprise = true
            "#,
        )
        .expect_err("unknown field must fail");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn rejects_invalid_pool_capacity() {
        let error = ConfigSnapshot::from_toml(
            r#"
                [worker]
                program = "/bin/worker"
                count = 1
                [memory]
                initial = 4
                soft_max = 2
                hard_max = 1
            "#,
        )
        .expect_err("invalid capacities must fail");

        assert!(error.to_string().contains("hard_max") || error.to_string().contains("soft_max"));
    }

    #[test]
    fn rejects_min_ready_above_worker_count() {
        let error = ConfigSnapshot::from_toml(
            r#"
                [worker]
                program = "/bin/worker"
                count = 2
                [generation]
                min_ready = 3
            "#,
        )
        .expect_err("min_ready must not exceed count");

        assert!(error.to_string().contains("min_ready"));
    }

    #[test]
    fn rejects_zero_restart_budget_settings() {
        let error = ConfigSnapshot::from_toml(
            r#"
                [worker]
                program = "/bin/worker"
                count = 1
                [restart]
                startup_failures = 0
            "#,
        )
        .expect_err("zero startup budget must fail");
        assert!(error.to_string().contains("startup budget"));
    }

    #[test]
    fn parses_listener_fds_and_all_failed_policy() {
        let snapshot = ConfigSnapshot::from_toml(
            r#"
                [worker]
                program = "/bin/worker"
                count = 1
                listener_fds = [3, 4]
                [generation]
                on_all_failed = "keep"
            "#,
        )
        .expect("valid extended config");
        assert_eq!(snapshot.worker.listener_fds, vec![3, 4]);
        assert_eq!(snapshot.generation.on_all_failed, OnAllFailed::Keep);
    }

    #[test]
    fn parses_module_scoped_network_limits() {
        let snapshot = ConfigSnapshot::from_toml(
            r#"
                [worker]
                program = "/bin/worker"
                [worker.net]
                max_write_queue = 8192
                max_read_buffer = 4096
                high_watermark = 6144
                low_watermark = 2048
            "#,
        )
        .expect("network limits are valid");
        assert_eq!(snapshot.worker.net.max_write_queue, 8192);
        assert_eq!(snapshot.worker.net.max_read_buffer, 4096);
    }

    #[test]
    fn reload_keeps_current_snapshot_when_source_is_invalid() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("config.toml");
        fs::write(&path, "[worker]\nprogram = '/bin/worker'\ncount = 1\n")
            .expect("write initial config");
        let source = TomlConfigSource::new(&path);
        let initial = source.load().expect("initial snapshot");
        let mut manager = ConfigManager::new(source, initial.clone()).expect("manager");

        fs::write(&path, "[worker\ninvalid").expect("write invalid config");
        assert!(manager.reload().is_err());
        assert_eq!(manager.current(), &initial);
    }

    #[test]
    fn reload_with_commits_only_after_downstream_apply_succeeds() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("config.toml");
        fs::write(&path, "[worker]\nprogram = '/bin/worker'\ncount = 1\n")
            .expect("write initial config");
        let source = TomlConfigSource::new(&path);
        let initial = source.load().expect("initial snapshot");
        let mut manager = ConfigManager::new(source, initial.clone()).expect("manager");

        fs::write(&path, "[worker]\nprogram = '/bin/worker'\ncount = 2\n")
            .expect("write next config");
        assert!(manager
            .reload_with(|_| Err(Error::Process("generation failed".into())))
            .is_err());
        assert_eq!(manager.current(), &initial);
    }
}

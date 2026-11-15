use std::cell::Cell;
use std::time::{Duration, Instant};

use aeg_maia::{
    config::PoolConfig,
    memory::{Pool, PoolFactory},
    Error, Result,
};

struct Factory {
    next: usize,
    reset_calls: Cell<usize>,
    fail_reset: bool,
}

impl PoolFactory<usize> for Factory {
    fn create(&mut self) -> Result<usize> {
        let value = self.next;
        self.next += 1;
        Ok(value)
    }

    fn reset(&mut self, value: &mut usize) -> Result<()> {
        self.reset_calls.set(self.reset_calls.get() + 1);
        if self.fail_reset {
            Err(Error::Memory("reset failed".into()))
        } else {
            *value = 0;
            Ok(())
        }
    }
}

fn config(initial: usize, soft_max: usize, hard_max: usize) -> PoolConfig {
    PoolConfig {
        initial,
        soft_max,
        hard_max,
        idle_timeout_secs: 10,
    }
}

#[test]
fn prewarms_and_grows_without_blocking_past_hard_max() {
    let pool = Pool::new(
        config(1, 2, 3),
        Factory {
            next: 1,
            reset_calls: Cell::new(0),
            fail_reset: false,
        },
    )
    .unwrap();
    assert_eq!(pool.len(), 1);
    let first = pool.checkout().unwrap();
    let second = pool.checkout().unwrap();
    let third = pool.checkout().unwrap();
    assert_eq!(pool.len(), 3);
    assert!(
        matches!(pool.checkout(), Err(Error::Memory(message)) if message.contains("exhausted"))
    );
    drop((first, second, third));
}

#[test]
fn guard_drop_resets_and_returns_object() {
    let pool = Pool::new(
        config(0, 1, 1),
        Factory {
            next: 10,
            reset_calls: Cell::new(0),
            fail_reset: false,
        },
    )
    .unwrap();
    {
        let mut guard = pool.checkout().unwrap();
        *guard = 99;
    }
    assert_eq!(pool.idle_len(), 1);
    let guard = pool.checkout().unwrap();
    assert_eq!(*guard, 0);
}

#[test]
fn reset_failure_discards_the_object_instead_of_panicking() {
    let pool = Pool::new(
        config(0, 1, 1),
        Factory {
            next: 1,
            reset_calls: Cell::new(0),
            fail_reset: true,
        },
    )
    .unwrap();
    let guard = pool.checkout().unwrap();
    drop(guard);
    assert_eq!(pool.len(), 0);
    assert_eq!(*pool.checkout().unwrap(), 2);
}

#[test]
fn maintenance_shrinks_idle_objects_but_keeps_initial_reserve() {
    let pool = Pool::new(
        config(1, 3, 3),
        Factory {
            next: 1,
            reset_calls: Cell::new(0),
            fail_reset: false,
        },
    )
    .unwrap();
    let first = pool.checkout().unwrap();
    let second = pool.checkout().unwrap();
    let third = pool.checkout().unwrap();
    drop((first, second, third));
    let removed = pool
        .maintain(Instant::now() + Duration::from_secs(11))
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(pool.len(), 1);
}

#[test]
fn soft_max_discards_excess_idle_objects_after_a_burst() {
    let pool = Pool::new(
        config(0, 1, 3),
        Factory {
            next: 1,
            reset_calls: Cell::new(0),
            fail_reset: false,
        },
    )
    .unwrap();
    let first = pool.checkout().unwrap();
    let second = pool.checkout().unwrap();
    let third = pool.checkout().unwrap();
    drop((first, second, third));
    assert_eq!(pool.len(), 1);
    assert_eq!(pool.idle_len(), 1);
}

#[test]
fn stats_report_checkout_exhaustion_and_destruction() {
    let pool = Pool::new(
        config(0, 0, 1),
        Factory {
            next: 1,
            reset_calls: Cell::new(0),
            fail_reset: false,
        },
    )
    .unwrap();
    let guard = pool.checkout().unwrap();
    assert_eq!(pool.stats().in_use, 1);
    assert_eq!(pool.stats().checkouts, 1);
    assert!(pool.checkout().is_err());
    assert_eq!(pool.stats().exhausted, 1);
    drop(guard);
    let stats = pool.stats();
    assert_eq!(stats.total, 0);
    assert_eq!(stats.idle, 0);
    assert_eq!(stats.destroyed, 1);
}

use std::{
    cell::RefCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    rc::Rc,
    time::{Duration, Instant},
};

use crate::{
    config::PoolConfig,
    error::{Error, Result},
};

pub trait PoolFactory<T> {
    fn create(&mut self) -> Result<T>;
    fn reset(&mut self, value: &mut T) -> Result<()>;
}

/// Policy used when a pool has no idle object and reached its hard limit.
///
/// Pools are deliberately non-blocking. `Error` is the default and returns a
/// memory error immediately; `Drop` is provided for callers that want to make
/// that policy explicit at the call site (it has the same non-blocking
/// behavior because a checkout has no value to drop).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Exhausted {
    #[default]
    Error,
    Drop,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub total: usize,
    pub idle: usize,
    pub in_use: usize,
    pub created: u64,
    pub destroyed: u64,
    pub checkouts: u64,
    pub exhausted: u64,
}

struct Idle<T> {
    value: T,
    available_at: Instant,
}

struct PoolState<T, F> {
    config: PoolConfig,
    factory: F,
    idle: VecDeque<Idle<T>>,
    total: usize,
    created: u64,
    destroyed: u64,
    checkouts: u64,
    exhausted: u64,
}

pub struct Pool<T, F> {
    state: Rc<RefCell<PoolState<T, F>>>,
}

impl<T, F: PoolFactory<T>> Pool<T, F> {
    pub fn new(config: PoolConfig, mut factory: F) -> Result<Self> {
        if config.hard_max == 0 {
            return Err(Error::Memory("hard_max must be greater than zero".into()));
        }
        if config.initial > config.soft_max || config.soft_max > config.hard_max {
            return Err(Error::Memory(
                "pool capacities must satisfy initial <= soft_max <= hard_max".into(),
            ));
        }
        let mut idle = VecDeque::with_capacity(config.initial);
        for _ in 0..config.initial {
            idle.push_back(Idle {
                value: factory.create()?,
                available_at: Instant::now(),
            });
        }
        let total = idle.len();
        Ok(Self {
            state: Rc::new(RefCell::new(PoolState {
                config,
                factory,
                idle,
                total,
                created: total as u64,
                destroyed: 0,
                checkouts: 0,
                exhausted: 0,
            })),
        })
    }

    pub fn checkout(&self) -> Result<PoolGuard<T, F>> {
        let mut state = self.state.borrow_mut();
        let value = if let Some(idle) = state.idle.pop_front() {
            idle.value
        } else {
            if state.total >= state.config.hard_max {
                state.exhausted += 1;
                return Err(Error::Memory("pool exhausted at hard_max".into()));
            }
            let value = state.factory.create()?;
            state.total += 1;
            state.created += 1;
            value
        };
        state.checkouts += 1;
        drop(state);
        Ok(PoolGuard {
            state: Rc::clone(&self.state),
            value: Some(value),
        })
    }

    pub fn maintain(&self, now: Instant) -> Result<usize> {
        let mut state = self.state.borrow_mut();
        let timeout = Duration::from_secs(state.config.idle_timeout_secs);
        let mut removed = 0;
        while state.total - removed > state.config.initial {
            let expired = state
                .idle
                .front()
                .map(|idle| now.saturating_duration_since(idle.available_at) >= timeout)
                .unwrap_or(false);
            if !expired {
                break;
            }
            state.idle.pop_front();
            removed += 1;
        }
        state.total -= removed;
        state.destroyed += removed as u64;
        Ok(removed)
    }

    pub fn len(&self) -> usize {
        self.state.borrow().total
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn idle_len(&self) -> usize {
        self.state.borrow().idle.len()
    }

    pub fn stats(&self) -> PoolStats {
        let state = self.state.borrow();
        PoolStats {
            total: state.total,
            idle: state.idle.len(),
            in_use: state.total.saturating_sub(state.idle.len()),
            created: state.created,
            destroyed: state.destroyed,
            checkouts: state.checkouts,
            exhausted: state.exhausted,
        }
    }
}

pub struct PoolGuard<T, F: PoolFactory<T>> {
    state: Rc<RefCell<PoolState<T, F>>>,
    value: Option<T>,
}

impl<T, F: PoolFactory<T>> Deref for PoolGuard<T, F> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value.as_ref().expect("pool guard value exists")
    }
}

impl<T, F: PoolFactory<T>> DerefMut for PoolGuard<T, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.as_mut().expect("pool guard value exists")
    }
}

impl<T, F: PoolFactory<T>> Drop for PoolGuard<T, F> {
    fn drop(&mut self) {
        let Some(mut value) = self.value.take() else {
            return;
        };
        let mut state = self.state.borrow_mut();
        if state.factory.reset(&mut value).is_ok() {
            if state.total > state.config.soft_max {
                state.total = state.total.saturating_sub(1);
                state.destroyed += 1;
            } else {
                state.idle.push_back(Idle {
                    value,
                    available_at: Instant::now(),
                });
            }
        } else {
            state.total = state.total.saturating_sub(1);
            state.destroyed += 1;
        }
    }
}

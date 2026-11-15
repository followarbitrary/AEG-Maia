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

struct Idle<T> {
    value: T,
    available_at: Instant,
}

struct PoolState<T, F> {
    config: PoolConfig,
    factory: F,
    idle: VecDeque<Idle<T>>,
    total: usize,
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
            })),
        })
    }

    pub fn checkout(&self) -> Result<PoolGuard<T, F>> {
        let mut state = self.state.borrow_mut();
        let value = if let Some(idle) = state.idle.pop_front() {
            idle.value
        } else {
            if state.total >= state.config.hard_max {
                return Err(Error::Memory("pool exhausted at hard_max".into()));
            }
            let value = state.factory.create()?;
            state.total += 1;
            value
        };
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
            } else {
                state.idle.push_back(Idle {
                    value,
                    available_at: Instant::now(),
                });
            }
        } else {
            state.total = state.total.saturating_sub(1);
        }
    }
}

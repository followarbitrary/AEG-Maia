use std::io;

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum NetError {
    #[error("connection does not exist: {0}")]
    UnknownConnection(u64),
    #[error("write queue is full")]
    QueueFull,
    #[error("invalid write queue watermarks")]
    InvalidWatermarks,
    #[error("invalid read buffer limit")]
    InvalidReadBuffer,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("event error: {0}")]
    Event(String),
    #[error("memory error: {0}")]
    Memory(String),
    #[error("module error: {0}")]
    Module(String),
    #[error("process error: {0}")]
    Process(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("network error: {0}")]
    Net(#[from] NetError),
}

pub type Result<T> = std::result::Result<T, Error>;

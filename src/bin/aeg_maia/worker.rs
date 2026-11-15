use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    sync::mpsc,
    thread,
    time::Duration,
};

#[cfg(unix)]
use std::os::fd::FromRawFd;

use aeg_maia::{
    ConfigSnapshot, ControlFrameCodec, Error, FrameKind, ListenerClaims, ModuleRegistryFactory,
    WorkerEventRegistry, WorkerMetadata,
};

use super::modules::{ReferenceRegistryFactory, ReferenceWorkerFactory};

pub fn run_worker() -> aeg_maia::Result<()> {
    #[cfg(unix)]
    if let Ok(raw_fd) = std::env::var("AEG_CONTROL_FD") {
        let fd = raw_fd
            .parse::<std::os::fd::RawFd>()
            .map_err(|error| Error::Process(format!("invalid AEG_CONTROL_FD: {error}")))?;
        let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
        let input = stream
            .try_clone()
            .map_err(|error| Error::Process(error.to_string()))?;
        return run_worker_io(input, stream);
    }
    run_worker_io(io::stdin(), io::stdout())
}

fn run_worker_io<R, W>(mut input: R, mut output: W) -> aeg_maia::Result<()>
where
    R: Read + Send + 'static,
    W: Write,
{
    let startup = match read_startup_frames(&mut input) {
        Ok(startup) => startup,
        Err(error) => {
            write_startup_failed(&mut output, &error)?;
            return Err(error);
        }
    };
    if let Err(error) = validate_metadata(&startup.0, &startup.1) {
        let _ = write_startup_failed(&mut output, &error);
        return Err(error);
    }
    if let Err(error) = validate_listener_fds(&startup.1) {
        let _ = write_startup_failed(&mut output, &error);
        return Err(error);
    }

    let registry = ReferenceRegistryFactory.build(&startup.0)?;
    let worker_cycle = registry.build_cycle()?;
    // Worker startup must not replay master-side init_module hooks.  Listener
    // ownership comes from the immutable metadata snapshot, so a listener
    // name is not required to equal its module name.
    let owners = if startup.1.listener_owners.is_empty() {
        // Metadata produced by older masters did not carry the owner table.
        // Keep that wire format readable for the reference one-module-per-
        // listener binary while all new masters send the explicit map.
        startup
            .1
            .listeners
            .iter()
            .map(|listener| (listener.name.clone(), listener.name.clone()))
            .collect::<BTreeMap<_, _>>()
    } else {
        startup.1.listener_owners.clone()
    };
    let registrar = ListenerClaims::with_listeners(owners, inherited_listeners(&startup.1)?);
    let mut worker = worker_cycle.build_worker_with_registrar(
        &ReferenceWorkerFactory,
        startup.1.worker_id,
        &startup.0,
        Box::new(WorkerEventRegistry::new(
            registrar,
            startup.1.listeners.len().max(16),
        )?),
    )?;
    if let Err(error) = worker.init_process() {
        let _ = write_startup_failed(&mut output, &error);
        return Err(error);
    }
    if let Err(error) = worker.ensure_all_listeners_claimed() {
        let _ = worker.exit_process();
        let _ = write_startup_failed(&mut output, &error);
        return Err(error);
    }
    if let Err(error) = worker.register_events() {
        let _ = worker.exit_process();
        let _ = write_startup_failed(&mut output, &error);
        return Err(error);
    }
    if let Err(error) = worker.validate_event_registrations() {
        worker.unregister_events();
        let _ = worker.exit_process();
        let _ = write_startup_failed(&mut output, &error);
        return Err(error);
    }
    if let Err(error) = worker.start() {
        worker.unregister_events();
        let _ = worker.exit_process();
        let _ = write_startup_failed(&mut output, &error);
        return Err(error);
    }
    write_frame(&mut output, FrameKind::Ready, &[])?;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || read_commands(input, sender));
    let mut draining = false;
    loop {
        for event in worker.poll_events(Some(Duration::from_millis(50)))? {
            worker.dispatch_event(event)?;
        }
        for command in receiver.try_iter() {
            let command = match command {
                Ok(command) => command,
                Err(_error) if draining => {
                    worker.unregister_events();
                    let _ = worker.exit_process();
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            match command {
                FrameKind::Quit if !draining => {
                    worker.set_draining(true);
                    worker.stop()?;
                    draining = true;
                }
                FrameKind::Terminate => {
                    worker.set_draining(false);
                    worker.unregister_events();
                    let _ = worker.stop();
                    let _ = worker.exit_process();
                    return Ok(());
                }
                FrameKind::ReopenLog => {}
                other => {
                    let error =
                        Error::Process(format!("unexpected worker command frame: {other:?}"));
                    let _ = write_startup_failed(&mut output, &error);
                    return Err(error);
                }
            }
        }
        if draining && !worker.has_active_handlers() {
            worker.unregister_events();
            worker.exit_process()?;
            return Ok(());
        }
    }
}

#[cfg(unix)]
fn inherited_listeners(
    metadata: &WorkerMetadata,
) -> aeg_maia::Result<Vec<(String, mio::net::TcpListener)>> {
    use std::os::fd::FromRawFd;
    metadata
        .listeners
        .iter()
        .map(|listener| {
            let std_listener = unsafe { std::net::TcpListener::from_raw_fd(listener.fd) };
            std_listener
                .set_nonblocking(true)
                .map_err(|error| Error::Process(error.to_string()))?;
            Ok((
                listener.name.clone(),
                mio::net::TcpListener::from_std(std_listener),
            ))
        })
        .collect()
}

#[cfg(not(unix))]
fn inherited_listeners(
    _metadata: &WorkerMetadata,
) -> aeg_maia::Result<Vec<(String, mio::net::TcpListener)>> {
    Err(Error::Process("inherited listeners require Unix".into()))
}

fn validate_listener_fds(metadata: &WorkerMetadata) -> aeg_maia::Result<()> {
    for listener in &metadata.listeners {
        if listener.fd < 0 {
            return Err(Error::Process(format!(
                "listener {} has invalid inherited fd {}",
                listener.name, listener.fd
            )));
        }
    }
    Ok(())
}

fn read_startup_frames<R: Read>(
    reader: &mut R,
) -> aeg_maia::Result<(ConfigSnapshot, WorkerMetadata)> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    let mut config = None;
    let mut metadata = None;
    loop {
        let read = reader
            .read(&mut chunk)
            .map_err(|error| Error::Process(error.to_string()))?;
        if read == 0 {
            return Err(Error::Process(
                "worker control channel closed before startup frames".into(),
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        while let Some((kind, payload)) = ControlFrameCodec::decode(&mut buffer)? {
            match kind {
                FrameKind::Config if config.is_none() => {
                    let text = std::str::from_utf8(&payload)
                        .map_err(|error| Error::Config(error.to_string()))?;
                    config = Some(ConfigSnapshot::from_toml(text)?);
                }
                FrameKind::Metadata if metadata.is_none() => {
                    metadata = Some(
                        toml::from_str(
                            std::str::from_utf8(&payload)
                                .map_err(|error| Error::Process(error.to_string()))?,
                        )
                        .map_err(|error| Error::Process(error.to_string()))?,
                    );
                }
                FrameKind::Config | FrameKind::Metadata => {
                    return Err(Error::Process("duplicate worker startup frame".into()));
                }
                _ => {
                    return Err(Error::Process(
                        "worker command arrived before startup".into(),
                    ))
                }
            }
            if config.is_some() && metadata.is_some() {
                if !buffer.is_empty() {
                    return Err(Error::Process(
                        "unexpected trailing data after worker startup frames".into(),
                    ));
                }
                return Ok((
                    config.take().expect("config is present"),
                    metadata.take().expect("metadata is present"),
                ));
            }
        }
    }
}

fn validate_metadata(config: &ConfigSnapshot, metadata: &WorkerMetadata) -> aeg_maia::Result<()> {
    if metadata.generation_id == 0 {
        return Err(Error::Process(
            "worker generation id must be non-zero".into(),
        ));
    }
    if config.listeners.len() != metadata.listeners.len() {
        return Err(Error::Process(format!(
            "worker listener metadata count {} does not match config {}",
            metadata.listeners.len(),
            config.listeners.len()
        )));
    }
    for (configured, received) in config.listeners.iter().zip(&metadata.listeners) {
        if configured.name != received.name {
            return Err(Error::Process(format!(
                "worker listener metadata mismatch: expected {}, got {}",
                configured.name, received.name
            )));
        }
    }
    if !metadata.listener_owners.is_empty() {
        if metadata.listener_owners.len() != metadata.listeners.len() {
            return Err(Error::Process(
                "worker listener owner metadata count does not match listeners".into(),
            ));
        }
        for listener in &metadata.listeners {
            if !metadata.listener_owners.contains_key(&listener.name) {
                return Err(Error::Process(format!(
                    "worker listener metadata has no owner for {}",
                    listener.name
                )));
            }
        }
    }
    Ok(())
}

fn read_commands<R: Read>(mut reader: R, sender: mpsc::Sender<aeg_maia::Result<FrameKind>>) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let result = reader.read(&mut chunk);
        let Ok(read) = result else {
            let _ = sender.send(Err(Error::Process("worker command read failed".into())));
            return;
        };
        if read == 0 {
            let _ = sender.send(Err(Error::Process("worker control channel closed".into())));
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        loop {
            match ControlFrameCodec::decode(&mut buffer) {
                Ok(Some((kind, _))) => {
                    if sender.send(Ok(kind)).is_err() {
                        return;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = sender.send(Err(error));
                    return;
                }
            }
        }
    }
}

fn write_startup_failed<W: Write>(writer: &mut W, error: &Error) -> aeg_maia::Result<()> {
    write_frame(
        writer,
        FrameKind::StartupFailed,
        error.to_string().as_bytes(),
    )
}

fn write_frame<W: Write>(writer: &mut W, kind: FrameKind, payload: &[u8]) -> aeg_maia::Result<()> {
    let frame = ControlFrameCodec::encode(kind, payload)?;
    writer
        .write_all(&frame)
        .and_then(|_| writer.flush())
        .map_err(|error| Error::Process(error.to_string()))
}

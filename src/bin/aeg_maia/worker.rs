use std::{
    io::{self, Read, Write},
    sync::mpsc,
    thread,
    time::Duration,
};

use aeg_maia::{
    ConfigSnapshot, ControlFrameCodec, Error, FrameKind, ListenerClaims, ModuleRegistryFactory,
    WorkerEventRegistry, WorkerMetadata,
};

use super::modules::{ReferenceRegistryFactory, ReferenceWorkerFactory};

pub fn run_worker() -> aeg_maia::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
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
    let mut master_cycle = registry.build_cycle()?;
    master_cycle.init_module()?;
    let registrar = ListenerClaims::from_cycle(&master_cycle);
    let mut worker = master_cycle.build_worker_with_registrar(
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
    drop(output);
    drop(input);

    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || read_commands(stdin, sender));
    loop {
        for event in worker.poll_events(Some(Duration::from_millis(50)))? {
            worker.dispatch_event(event)?;
        }
        let Some(command) = receiver.try_iter().next() else {
            continue;
        };
        match command? {
            FrameKind::Quit => {
                worker.set_draining(true);
                worker.stop()?;
                worker.unregister_events();
                worker.exit_process()?;
                return Ok(());
            }
            FrameKind::Terminate => {
                worker.set_draining(false);
                worker.unregister_events();
                let _ = worker.stop();
                let _ = worker.exit_process();
                return Ok(());
            }
            other => {
                let error = Error::Process(format!("unexpected worker command frame: {other:?}"));
                let _ = write_startup_failed(&mut io::stdout(), &error);
                return Err(error);
            }
        }
    }
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

#[path = "aeg_maia/master.rs"]
mod master;
#[path = "aeg_maia/modules.rs"]
mod modules;
#[path = "aeg_maia/platform.rs"]
mod platform;
#[path = "aeg_maia/worker.rs"]
mod worker;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cli {
    pub config: std::path::PathBuf,
    pub pid: Option<std::path::PathBuf>,
    pub log: Option<std::path::PathBuf>,
    pub daemon: bool,
    pub worker: bool,
}

impl Cli {
    pub fn parse<I>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut cli = Self {
            config: "aeg-maia.toml".into(),
            pid: None,
            log: None,
            daemon: false,
            worker: false,
        };
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--worker" => cli.worker = true,
                "--daemon" => cli.daemon = true,
                "-c" | "--config" => {
                    cli.config = args
                        .next()
                        .ok_or_else(|| format!("{arg} requires a path"))?
                        .into();
                }
                "-p" | "--pid" => {
                    cli.pid = Some(
                        args.next()
                            .ok_or_else(|| format!("{arg} requires a path"))?
                            .into(),
                    );
                }
                "-l" | "--log" => {
                    cli.log = Some(
                        args.next()
                            .ok_or_else(|| format!("{arg} requires a path"))?
                            .into(),
                    );
                }
                value if value.starts_with("--config=") => cli.config = value[9..].into(),
                value if value.starts_with("--pid=") => cli.pid = Some(value[6..].into()),
                value if value.starts_with("--log=") => cli.log = Some(value[6..].into()),
                "-h" | "--help" => {
                    return Err("usage: aeg-maia [-c PATH] [-p PATH] [-l PATH] [--daemon]".into())
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(cli)
    }
}

fn main() {
    let cli = match Cli::parse(std::env::args().skip(1)) {
        Ok(cli) => cli,
        Err(error) => {
            eprintln!("aeg-maia: {error}");
            std::process::exit(2);
        }
    };
    let result = if cli.worker {
        worker::run_worker()
    } else {
        master::run_master(cli)
    };
    if let Err(error) = result {
        eprintln!("aeg-maia: {error}");
        std::process::exit(1);
    }
}

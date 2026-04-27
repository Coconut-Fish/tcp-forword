use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use serde::Deserialize;
use tracing::Level;

const DEFAULT_LISTEN: &str = "0.0.0.0:7000";
const DEFAULT_UPSTREAM: &str = "127.0.0.1:9000";
const DEFAULT_RECONNECT_INITIAL_MS: u64 = 500;
const DEFAULT_RECONNECT_MAX_MS: u64 = 10_000;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_QUEUE_CAPACITY: usize = 256;
const DEFAULT_RECONNECT_MULTIPLIER: u32 = 2;

#[derive(Debug, Clone, Parser)]
#[command(
    author,
    version,
    about = "High-availability multi-client TCP forwarder"
)]
pub struct CliArgs {
    #[arg(long)]
    pub listen: Option<String>,

    #[arg(long)]
    pub upstream: Option<String>,

    #[arg(long)]
    pub config: Option<PathBuf>,

    #[arg(long)]
    pub reconnect_initial_ms: Option<u64>,

    #[arg(long)]
    pub reconnect_max_ms: Option<u64>,

    #[arg(long)]
    pub connect_timeout_ms: Option<u64>,

    #[arg(long)]
    pub queue_capacity: Option<usize>,

    #[arg(long, value_enum)]
    pub log_level: Option<LogLevel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Info
    }
}

impl LogLevel {
    pub fn as_tracing_level(self) -> Level {
        match self {
            Self::Trace => Level::TRACE,
            Self::Debug => Level::DEBUG,
            Self::Info => Level::INFO,
            Self::Warn => Level::WARN,
            Self::Error => Level::ERROR,
        }
    }
}

impl FromStr for LogLevel {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => bail!("unsupported log level: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub listen: String,
    pub upstream: String,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub reconnect_multiplier: u32,
    pub connect_timeout: Duration,
    pub queue_capacity: usize,
    pub log_level: LogLevel,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN.to_string(),
            upstream: DEFAULT_UPSTREAM.to_string(),
            reconnect_initial: Duration::from_millis(DEFAULT_RECONNECT_INITIAL_MS),
            reconnect_max: Duration::from_millis(DEFAULT_RECONNECT_MAX_MS),
            reconnect_multiplier: DEFAULT_RECONNECT_MULTIPLIER,
            connect_timeout: Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            log_level: LogLevel::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FileConfig {
    listen: Option<String>,
    upstream: Option<String>,
    reconnect_initial_ms: Option<u64>,
    reconnect_max_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    queue_capacity: Option<usize>,
    log_level: Option<LogLevel>,
}

impl Config {
    pub fn from_cli(args: CliArgs) -> Result<Self> {
        Self::build(args)
    }

    pub fn parse_cli_from<I, T>(input: I) -> Result<CliArgs>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        CliArgs::try_parse_from(input).map_err(Into::into)
    }

    fn build(args: CliArgs) -> Result<Self> {
        let mut config = Self::default();
        let file_config = match args.config.as_deref() {
            Some(path) => Some(read_file_config(path)?),
            None => None,
        };

        if let Some(file) = file_config {
            apply_file_config(&mut config, file);
        }

        if let Some(listen) = args.listen {
            config.listen = listen;
        }
        if let Some(upstream) = args.upstream {
            config.upstream = upstream;
        }
        if let Some(value) = args.reconnect_initial_ms {
            config.reconnect_initial = Duration::from_millis(value);
        }
        if let Some(value) = args.reconnect_max_ms {
            config.reconnect_max = Duration::from_millis(value);
        }
        if let Some(value) = args.connect_timeout_ms {
            config.connect_timeout = Duration::from_millis(value);
        }
        if let Some(value) = args.queue_capacity {
            config.queue_capacity = value;
        }
        if let Some(level) = args.log_level {
            config.log_level = level;
        }

        validate(&config)?;
        Ok(config)
    }
}

fn read_file_config(path: &Path) -> Result<FileConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("failed to parse TOML config: {}", path.display()))
}

fn apply_file_config(config: &mut Config, file: FileConfig) {
    if let Some(listen) = file.listen {
        config.listen = listen;
    }
    if let Some(upstream) = file.upstream {
        config.upstream = upstream;
    }
    if let Some(value) = file.reconnect_initial_ms {
        config.reconnect_initial = Duration::from_millis(value);
    }
    if let Some(value) = file.reconnect_max_ms {
        config.reconnect_max = Duration::from_millis(value);
    }
    if let Some(value) = file.connect_timeout_ms {
        config.connect_timeout = Duration::from_millis(value);
    }
    if let Some(value) = file.queue_capacity {
        config.queue_capacity = value;
    }
    if let Some(level) = file.log_level {
        config.log_level = level;
    }
}

fn validate(config: &Config) -> Result<()> {
    if config.listen.trim().is_empty() {
        bail!("listen address cannot be empty");
    }
    if config.upstream.trim().is_empty() {
        bail!("upstream address cannot be empty");
    }
    if config.reconnect_initial.is_zero() {
        bail!("reconnect_initial_ms must be greater than 0");
    }
    if config.reconnect_max < config.reconnect_initial {
        bail!("reconnect_max_ms must be greater than or equal to reconnect_initial_ms");
    }
    if config.connect_timeout.is_zero() {
        bail!("connect_timeout_ms must be greater than 0");
    }
    if config.queue_capacity == 0 {
        bail!("queue_capacity must be greater than 0");
    }
    if config.reconnect_multiplier < 2 {
        return Err(anyhow!("reconnect multiplier must be at least 2"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_config_path(name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("valid system time")
            .as_nanos();

        std::env::temp_dir().join(format!("tcp-forward-{name}-{timestamp}.toml"))
    }

    #[test]
    fn applies_defaults_without_external_config() {
        let args = Config::parse_cli_from(["tcp-forward"]).expect("cli parses");
        let config = Config::from_cli(args).expect("config builds");

        assert_eq!(config.listen, DEFAULT_LISTEN);
        assert_eq!(config.upstream, DEFAULT_UPSTREAM);
        assert_eq!(
            config.reconnect_initial,
            Duration::from_millis(DEFAULT_RECONNECT_INITIAL_MS)
        );
        assert_eq!(
            config.reconnect_max,
            Duration::from_millis(DEFAULT_RECONNECT_MAX_MS)
        );
        assert_eq!(
            config.connect_timeout,
            Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS)
        );
        assert_eq!(config.queue_capacity, DEFAULT_QUEUE_CAPACITY);
        assert_eq!(config.log_level, LogLevel::Info);
    }

    #[test]
    fn cli_values_override_file_values() {
        let path = temp_config_path("merge");
        let file = r#"
listen = "127.0.0.1:7100"
upstream = "127.0.0.1:9100"
reconnect_initial_ms = 200
reconnect_max_ms = 5000
connect_timeout_ms = 1500
queue_capacity = 64
log_level = "warn"
"#;
        fs::write(&path, file).expect("writes temp config");

        let args = Config::parse_cli_from([
            "tcp-forward",
            "--config",
            path.to_str().expect("path"),
            "--listen",
            "0.0.0.0:7200",
            "--queue-capacity",
            "128",
            "--log-level",
            "debug",
        ])
        .expect("cli parses");
        let config = Config::from_cli(args).expect("config builds");

        assert_eq!(config.listen, "0.0.0.0:7200");
        assert_eq!(config.upstream, "127.0.0.1:9100");
        assert_eq!(config.reconnect_initial, Duration::from_millis(200));
        assert_eq!(config.reconnect_max, Duration::from_millis(5000));
        assert_eq!(config.connect_timeout, Duration::from_millis(1500));
        assert_eq!(config.queue_capacity, 128);
        assert_eq!(config.log_level, LogLevel::Debug);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_invalid_queue_capacity() {
        let args =
            Config::parse_cli_from(["tcp-forward", "--queue-capacity", "0"]).expect("cli parses");
        let error = Config::from_cli(args).expect_err("config should fail");

        assert!(
            error.to_string().contains("queue_capacity"),
            "unexpected error: {error}"
        );
    }
}

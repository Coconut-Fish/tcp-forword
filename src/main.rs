use clap::Parser;
use tcp_forward::config::{CliArgs, Config};

fn init_tracing(level: tcp_forward::config::LogLevel) {
    tracing_subscriber::fmt()
        .with_max_level(level.as_tracing_level())
        .with_target(false)
        .compact()
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_cli(CliArgs::parse())?;
    init_tracing(config.log_level);

    tcp_forward::run(config).await
}

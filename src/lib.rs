pub mod config;
pub mod proxy;
pub mod upstream;

use anyhow::Result;
use config::Config;

pub async fn run(config: Config) -> Result<()> {
    proxy::run(config).await
}

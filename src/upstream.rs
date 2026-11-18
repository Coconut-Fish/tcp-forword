use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, watch},
    time,
};
use tracing::{debug, info, warn};

use crate::{config::Config, proxy::ClientRegistry};

#[derive(Debug)]
pub struct UpstreamMessage {
    pub client_id: u64,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct UpstreamState {
    inner: Arc<UpstreamStateInner>,
}

struct UpstreamStateInner {
    sender: Mutex<Option<mpsc::Sender<UpstreamMessage>>>,
    connected: AtomicBool,
    connected_tx: watch::Sender<bool>,
}

impl UpstreamState {
    pub fn new() -> Self {
        let (connected_tx, _) = watch::channel(false);

        Self {
            inner: Arc::new(UpstreamStateInner {
                sender: Mutex::new(None),
                connected: AtomicBool::new(false),
                connected_tx,
            }),
        }
    }

    pub fn mark_connected(&self, sender: mpsc::Sender<UpstreamMessage>) {
        *self.inner.sender.lock().expect("poisoned upstream sender") = Some(sender);
        self.inner.connected.store(true, Ordering::Release);
        let _ = self.inner.connected_tx.send(true);
    }

    pub fn mark_disconnected(&self) {
        *self.inner.sender.lock().expect("poisoned upstream sender") = None;
        self.inner.connected.store(false, Ordering::Release);
        let _ = self.inner.connected_tx.send(false);
    }

    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Acquire)
    }

    pub fn current_sender(&self) -> Option<mpsc::Sender<UpstreamMessage>> {
        self.inner
            .sender
            .lock()
            .expect("poisoned upstream sender")
            .clone()
    }

    pub async fn wait_until_connected(&self) {
        if self.is_connected() {
            return;
        }

        let mut receiver = self.inner.connected_tx.subscribe();
        loop {
            if *receiver.borrow() {
                return;
            }

            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    multiplier: u32,
    current: Duration,
    attempts: u32,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration, multiplier: u32) -> Self {
        Self {
            initial,
            max,
            multiplier,
            current: initial,
            attempts: 0,
        }
    }

    pub fn reset(&mut self) {
        self.current = self.initial;
        self.attempts = 0;
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = jitter(self.current, self.attempts);
        let next_ms = self
            .current
            .as_millis()
            .saturating_mul(u128::from(self.multiplier));
        let capped_ms = next_ms.min(self.max.as_millis());
        let capped_ms = capped_ms.max(self.initial.as_millis());

        self.current = duration_from_millis(capped_ms);
        self.attempts = self.attempts.saturating_add(1);

        delay.min(self.max)
    }
}

fn jitter(base: Duration, attempt: u32) -> Duration {
    let base_ms = base.as_millis();
    let spread = u128::from(80 + pseudo_random_percent(attempt));
    let jittered = base_ms.saturating_mul(spread) / 100;
    duration_from_millis(jittered.max(1))
}

fn pseudo_random_percent(attempt: u32) -> u32 {
    let mixed = attempt.wrapping_mul(1_103_515_245).wrapping_add(12_345) % 41;

    mixed
}

fn duration_from_millis(value: u128) -> Duration {
    let bounded = value.min(u128::from(u64::MAX));
    Duration::from_millis(bounded as u64)
}

pub async fn run_connection_manager(
    config: Config,
    state: UpstreamState,
    clients: ClientRegistry,
) -> Result<()> {
    let mut backoff = Backoff::new(
        config.reconnect_initial,
        config.reconnect_max,
        config.reconnect_multiplier,
    );

    state.mark_disconnected();

    loop {
        match connect_upstream(&config).await {
            Ok(stream) => {
                info!(upstream = %config.upstream, "connected to upstream");
                backoff.reset();

                if let Err(error) =
                    run_upstream_session(stream, &config, state.clone(), clients.clone()).await
                {
                    warn!(upstream = %config.upstream, error = %error, "upstream session ended");
                }
            }
            Err(error) => {
                warn!(upstream = %config.upstream, error = %error, "failed to connect to upstream");
            }
        }

        state.mark_disconnected();

        let delay = backoff.next_delay();
        info!(
            upstream = %config.upstream,
            delay_ms = delay.as_millis() as u64,
            "retrying upstream connection"
        );
        time::sleep(delay).await;
    }
}

async fn connect_upstream(config: &Config) -> Result<TcpStream> {
    match time::timeout(config.connect_timeout, TcpStream::connect(&config.upstream)).await {
        Ok(Ok(stream)) => {
            stream.set_nodelay(true)?;
            Ok(stream)
        }
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(anyhow!(
            "timed out after {} ms while connecting",
            config.connect_timeout.as_millis()
        )),
    }
}

async fn run_upstream_session(
    stream: TcpStream,
    config: &Config,
    state: UpstreamState,
    clients: ClientRegistry,
) -> Result<()> {
    let (mut reader, writer) = stream.into_split();
    let (sender, receiver) = mpsc::channel(config.queue_capacity);

    state.mark_connected(sender);

    let reader_handle = tokio::spawn(async move {
        let mut buffer = vec![0_u8; 4096];
        let mut total_bytes = 0_u64;

        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                return Err(anyhow!("upstream closed the connection"));
            }

            total_bytes = total_bytes.saturating_add(read as u64);
            let delivered = clients.broadcast(&buffer[..read]);
            debug!(
                bytes = read,
                delivered_clients = delivered.delivered,
                dropped_clients = delivered.dropped,
                total_bytes,
                "broadcast upstream payload to clients"
            );
        }
    });

    let writer_handle = tokio::spawn(write_upstream_loop(
        receiver,
        writer,
        config.forward_interval,
    ));

    let outcome = tokio::select! {
        result = reader_handle => join_result("upstream reader", result),
        result = writer_handle => join_result("upstream writer", result),
    };

    state.mark_disconnected();

    outcome
}

async fn write_upstream_loop(
    mut receiver: mpsc::Receiver<UpstreamMessage>,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    interval: Duration,
) -> Result<()> {
    let mut total_bytes = 0_u64;
    let mut next_send = time::Instant::now();

    while let Some(message) = receiver.recv().await {
        time::sleep_until(next_send).await;

        writer.write_all(&message.data).await?;
        next_send = time::Instant::now() + interval;

        total_bytes = total_bytes.saturating_add(message.data.len() as u64);
        debug!(
            client_id = message.client_id,
            bytes = message.data.len(),
            total_bytes,
            interval_ms = interval.as_millis() as u64,
            "forwarded client payload to upstream"
        );
    }

    Err(anyhow!("upstream write loop ended unexpectedly"))
}

fn join_result(
    task_name: &'static str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(inner) => inner,
        Err(error) => Err(anyhow!("{task_name} task failed: {error}")),
    }
}

impl fmt::Debug for UpstreamStateInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamStateInner")
            .field("connected", &self.connected.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_resets_within_bounds() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_millis(800), 2);

        let first = backoff.next_delay();
        let second = backoff.next_delay();
        let third = backoff.next_delay();
        let fourth = backoff.next_delay();

        assert!(first >= Duration::from_millis(80));
        assert!(first <= Duration::from_millis(120));
        assert!(second >= Duration::from_millis(160));
        assert!(second <= Duration::from_millis(240));
        assert!(third >= Duration::from_millis(320));
        assert!(third <= Duration::from_millis(480));
        assert!(fourth <= Duration::from_millis(800));

        backoff.reset();
        let reset = backoff.next_delay();
        assert!(reset >= Duration::from_millis(80));
        assert!(reset <= Duration::from_millis(120));
    }
}

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, anyhow};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::mpsc,
};
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    upstream::{UpstreamMessage, UpstreamState, run_connection_manager},
};

const CLIENT_BROADCAST_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct ClientRegistry {
    inner: Arc<Mutex<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BroadcastReport {
    pub delivered: usize,
    pub dropped: usize,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn insert(&self, client_id: u64, sender: mpsc::Sender<Vec<u8>>) {
        self.inner
            .lock()
            .expect("poisoned client registry")
            .insert(client_id, sender);
    }

    pub fn remove(&self, client_id: u64) -> bool {
        self.inner
            .lock()
            .expect("poisoned client registry")
            .remove(&client_id)
            .is_some()
    }

    pub fn broadcast(&self, data: &[u8]) -> BroadcastReport {
        let snapshot: Vec<(u64, mpsc::Sender<Vec<u8>>)> = self
            .inner
            .lock()
            .expect("poisoned client registry")
            .iter()
            .map(|(client_id, sender)| (*client_id, sender.clone()))
            .collect();

        let mut delivered = 0_usize;
        let mut stale = Vec::new();

        for (client_id, sender) in snapshot {
            match sender.try_send(data.to_vec()) {
                Ok(()) => delivered += 1,
                Err(error) => {
                    stale.push(client_id);
                    debug!(
                        client_id,
                        reason = ?error,
                        "dropping client from broadcast registry after send failure"
                    );
                }
            }
        }

        if !stale.is_empty() {
            let mut registry = self.inner.lock().expect("poisoned client registry");
            for client_id in &stale {
                registry.remove(client_id);
            }
        }

        BroadcastReport {
            delivered,
            dropped: stale.len(),
        }
    }
}

pub async fn run(config: Config) -> Result<()> {
    let listener = TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("failed to bind listen address {}", config.listen))?;
    let clients = ClientRegistry::new();
    let upstream = UpstreamState::new();
    let next_client_id = Arc::new(AtomicU64::new(1));

    info!(
        listen = %config.listen,
        upstream = %config.upstream,
        reconnect_initial_ms = config.reconnect_initial.as_millis() as u64,
        reconnect_max_ms = config.reconnect_max.as_millis() as u64,
        queue_capacity = config.queue_capacity,
        "tcp forwarder started"
    );

    let upstream_handle = tokio::spawn(run_connection_manager(
        config.clone(),
        upstream.clone(),
        clients.clone(),
    ));

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let client_id = next_client_id.fetch_add(1, Ordering::Relaxed);
        let clients = clients.clone();
        let upstream = upstream.clone();

        info!(client_id, peer_addr = %peer_addr, "client connected");

        tokio::spawn(async move {
            if let Err(error) = handle_client(client_id, peer_addr, stream, upstream, clients).await
            {
                warn!(client_id, peer_addr = %peer_addr, error = %error, "client task ended with error");
            }
        });

        if upstream_handle.is_finished() {
            match upstream_handle.await {
                Ok(Err(error)) => return Err(error).context("upstream manager exited"),
                Ok(Ok(())) => {
                    return Err(anyhow!("upstream manager stopped unexpectedly"));
                }
                Err(error) => {
                    return Err(anyhow!("upstream manager join error: {error}"));
                }
            }
        }
    }
}

async fn handle_client(
    client_id: u64,
    peer_addr: SocketAddr,
    stream: TcpStream,
    upstream: UpstreamState,
    clients: ClientRegistry,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let (sender, receiver) = mpsc::channel(CLIENT_BROADCAST_QUEUE_CAPACITY);
    clients.insert(client_id, sender);

    let mut writer_handle = tokio::spawn(client_write_loop(client_id, writer, receiver));

    let outcome = tokio::select! {
        result = client_read_loop(client_id, reader, upstream) => {
            clients.remove(client_id);
            writer_handle.abort();
            result
        }
        result = &mut writer_handle => {
            clients.remove(client_id);
            match result {
                Ok(inner) => inner,
                Err(error) => Err(anyhow!("client writer task failed: {error}")),
            }
        }
    };

    info!(client_id, peer_addr = %peer_addr, "client disconnected");

    outcome
}

async fn client_read_loop(
    client_id: u64,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    upstream: UpstreamState,
) -> Result<()> {
    let mut buffer = vec![0_u8; 4096];
    let mut pending: Option<Vec<u8>> = None;
    let mut total_bytes = 0_u64;

    loop {
        if let Some(message) = pending.take() {
            pending = forward_pending(client_id, message, &upstream).await;
            continue;
        }

        upstream.wait_until_connected().await;
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        total_bytes = total_bytes.saturating_add(bytes_read as u64);
        debug!(
            client_id,
            bytes_read, total_bytes, "received client payload"
        );

        pending = Some(buffer[..bytes_read].to_vec());
    }
}

async fn forward_pending(
    client_id: u64,
    mut payload: Vec<u8>,
    upstream: &UpstreamState,
) -> Option<Vec<u8>> {
    loop {
        if let Some(sender) = upstream.current_sender() {
            match sender
                .send(UpstreamMessage {
                    client_id,
                    data: payload,
                })
                .await
            {
                Ok(()) => return None,
                Err(error) => {
                    payload = error.0.data;
                }
            }
        }

        upstream.wait_until_connected().await;
    }
}

async fn client_write_loop(
    client_id: u64,
    mut writer: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    let mut total_bytes = 0_u64;

    while let Some(payload) = receiver.recv().await {
        writer.write_all(&payload).await?;
        total_bytes = total_bytes.saturating_add(payload.len() as u64);
        debug!(
            client_id,
            bytes = payload.len(),
            total_bytes,
            "sent upstream payload to client"
        );
    }

    Ok(())
}

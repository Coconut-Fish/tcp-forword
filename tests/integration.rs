use std::{future::Future, net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use tcp_forward::config::{Config, LogLevel};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{sleep, timeout},
};

const TEST_FORWARD_INTERVAL: Duration = Duration::from_millis(50);

fn test_config(listen: String, upstream: String) -> Config {
    Config {
        listen,
        upstream,
        reconnect_initial: Duration::from_millis(25),
        reconnect_max: Duration::from_millis(75),
        reconnect_multiplier: 2,
        connect_timeout: Duration::from_millis(200),
        queue_capacity: 32,
        forward_interval: TEST_FORWARD_INTERVAL,
        log_level: LogLevel::Error,
    }
}

fn free_local_addr() -> Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    Ok(address)
}

async fn spawn_proxy(listen: SocketAddr, upstream: SocketAddr) -> Result<JoinHandle<Result<()>>> {
    spawn_proxy_with_interval(listen, upstream, TEST_FORWARD_INTERVAL).await
}

async fn spawn_proxy_with_interval(
    listen: SocketAddr,
    upstream: SocketAddr,
    forward_interval: Duration,
) -> Result<JoinHandle<Result<()>>> {
    let mut config = test_config(listen.to_string(), upstream.to_string());
    config.forward_interval = forward_interval;
    let handle = tokio::spawn(async move { tcp_forward::run(config).await });
    wait_for_connect(listen).await?;
    Ok(handle)
}

async fn wait_for_connect(addr: SocketAddr) -> Result<TcpStream> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn read_exact_len(stream: &mut TcpStream, size: usize) -> Result<Vec<u8>> {
    let mut buffer = vec![0_u8; size];
    timeout(Duration::from_secs(2), stream.read_exact(&mut buffer))
        .await
        .context("timed out while reading exact payload")??;
    Ok(buffer)
}

async fn accept_upstream(listener: &TcpListener) -> Result<(TcpStream, SocketAddr)> {
    timeout(Duration::from_secs(2), listener.accept())
        .await
        .context("timed out waiting for upstream connection")?
        .map_err(Into::into)
}

async fn with_proxy<F, Fut>(test: F) -> Result<()>
where
    F: FnOnce(SocketAddr, SocketAddr) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let listen = free_local_addr()?;
    let upstream = free_local_addr()?;
    test(listen, upstream).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwards_multiple_clients_into_single_upstream() -> Result<()> {
    with_proxy(|listen, upstream| async move {
        let upstream_listener = TcpListener::bind(upstream).await?;
        let proxy_handle = spawn_proxy(listen, upstream).await?;
        let (mut upstream_stream, _) = accept_upstream(&upstream_listener).await?;

        let mut client_one = wait_for_connect(listen).await?;
        let mut client_two = wait_for_connect(listen).await?;

        client_one.write_all(b"one").await?;
        client_two.write_all(b"two").await?;

        let payload = read_exact_len(&mut upstream_stream, 6).await?;
        assert!(
            payload == b"onetwo" || payload == b"twoone",
            "unexpected upstream payload: {:?}",
            payload
        );

        proxy_handle.abort();
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcasts_upstream_payload_to_all_clients() -> Result<()> {
    with_proxy(|listen, upstream| async move {
        let upstream_listener = TcpListener::bind(upstream).await?;
        let proxy_handle = spawn_proxy(listen, upstream).await?;
        let (mut upstream_stream, _) = accept_upstream(&upstream_listener).await?;

        let mut client_one = wait_for_connect(listen).await?;
        let mut client_two = wait_for_connect(listen).await?;

        client_one.write_all(b"1").await?;
        client_two.write_all(b"2").await?;
        let ready = read_exact_len(&mut upstream_stream, 2).await?;
        assert!(
            ready == b"12" || ready == b"21",
            "unexpected ready payload: {:?}",
            ready
        );

        upstream_stream.write_all(b"status").await?;

        let received_one = read_exact_len(&mut client_one, 6).await?;
        let received_two = read_exact_len(&mut client_two, 6).await?;

        assert_eq!(received_one, b"status");
        assert_eq!(received_two, b"status");

        proxy_handle.abort();
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paces_queued_client_messages_to_upstream() -> Result<()> {
    with_proxy(|listen, upstream| async move {
        let upstream_listener = TcpListener::bind(upstream).await?;
        let forward_interval = Duration::from_millis(200);
        let proxy_handle = spawn_proxy_with_interval(listen, upstream, forward_interval).await?;
        let (mut upstream_stream, _) = accept_upstream(&upstream_listener).await?;

        let mut client_one = wait_for_connect(listen).await?;
        let mut client_two = wait_for_connect(listen).await?;
        client_one.set_nodelay(true)?;
        client_two.set_nodelay(true)?;

        let start = tokio::time::Instant::now();
        tokio::try_join!(client_one.write_all(b"aaa"), client_two.write_all(b"bbb"))?;

        let first = read_exact_len(&mut upstream_stream, 3).await?;
        let first_at = start.elapsed();
        let second = read_exact_len(&mut upstream_stream, 3).await?;
        let second_at = start.elapsed();

        let forwarded = [first, second];
        assert!(
            (forwarded[0].as_slice() == b"aaa" && forwarded[1].as_slice() == b"bbb")
                || (forwarded[0].as_slice() == b"bbb" && forwarded[1].as_slice() == b"aaa"),
            "unexpected upstream payloads: {:?}",
            forwarded
        );
        assert!(
            first_at < forward_interval,
            "first message should be forwarded immediately, took {first_at:?}"
        );
        assert!(
            second_at - first_at >= forward_interval.mul_f64(0.7),
            "queued message was not paced: {first_at:?} -> {second_at:?}"
        );

        proxy_handle.abort();
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnects_upstream_without_dropping_existing_clients() -> Result<()> {
    with_proxy(|listen, upstream| async move {
        let upstream_listener = TcpListener::bind(upstream).await?;
        let proxy_handle = spawn_proxy(listen, upstream).await?;
        let (mut upstream_stream, _) = accept_upstream(&upstream_listener).await?;

        let mut client = wait_for_connect(listen).await?;

        client.write_all(b"first").await?;
        let initial = read_exact_len(&mut upstream_stream, 5).await?;
        assert_eq!(initial, b"first");

        drop(upstream_stream);
        drop(upstream_listener);
        sleep(Duration::from_millis(80)).await;

        let upstream_listener = TcpListener::bind(upstream).await?;
        let (mut reconnected_stream, _) = accept_upstream(&upstream_listener).await?;

        client.write_all(b"second").await?;
        let resent = read_exact_len(&mut reconnected_stream, 6).await?;
        assert_eq!(resent, b"second");

        reconnected_stream.write_all(b"ok").await?;
        let response = read_exact_len(&mut client, 2).await?;
        assert_eq!(response, b"ok");

        proxy_handle.abort();
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removes_broken_clients_without_affecting_others() -> Result<()> {
    with_proxy(|listen, upstream| async move {
        let upstream_listener = TcpListener::bind(upstream).await?;
        let proxy_handle = spawn_proxy(listen, upstream).await?;
        let (mut upstream_stream, _) = accept_upstream(&upstream_listener).await?;

        let client_one = wait_for_connect(listen).await?;
        let mut client_two = wait_for_connect(listen).await?;

        drop(client_one);
        sleep(Duration::from_millis(30)).await;

        upstream_stream.write_all(b"cleanup").await?;
        upstream_stream.write_all(b"steady").await?;

        let received = read_exact_len(&mut client_two, 13).await?;
        assert_eq!(received, b"cleanupsteady");

        proxy_handle.abort();
        Ok(())
    })
    .await
}

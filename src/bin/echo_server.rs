use std::env;
use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = env::var("ECHO_ADDR").unwrap_or_else(|_| "127.0.0.1:9999".into());
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    eprintln!("echo listening on {addr}");

    loop {
        let (mut socket, peer) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        eprintln!(
                            "got from {peer}: {:?}",
                            String::from_utf8_lossy(&buf[..n])
                        );
                        if socket.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

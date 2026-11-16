use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use futures_util::{SinkExt, StreamExt};
use pocketbi_agent::{ClientMsg, ServerMsg};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let token = env::var("POCKETBI_TOKEN").context("POCKETBI_TOKEN is required")?;
    let url = env::var("POCKETBI_URL").unwrap_or_else(|_| "ws://127.0.0.1:8700/agent".into());

    let mut backoff = Duration::from_secs(1);
    loop {
        match run_session(&url, &token).await {
            Ok(()) => {
                info!("session ended cleanly; reconnecting");
                backoff = Duration::from_secs(1);
            }
            Err(err) => {
                error!(?err, "session failed; reconnecting");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn run_session(url: &str, token: &str) -> Result<()> {
    info!(%url, "connecting to hub");
    let (ws, _) = connect_async(url)
        .await
        .with_context(|| format!("connect to {url}"))?;
    let (mut sink, mut stream) = ws.split();

    sink.send(Message::Text(ClientMsg::Hello {
        token: token.to_string(),
    }
    .to_json()?
    .into()))
    .await?;

    let streams: Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let (out_tx, mut out_rx) = mpsc::channel::<ClientMsg>(256);

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            match msg.to_json() {
                Ok(json) => {
                    if sink.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    error!(?err, "encode outbound message");
                    break;
                }
            }
        }
        let _: Result<(), _> = SinkExt::<Message>::close(&mut sink).await;
    });

    let mut authed = false;
    while let Some(frame) = stream.next().await {
        let frame = frame.context("read websocket")?;
        match frame {
            Message::Text(text) => {
                let msg = ServerMsg::from_json(&text).context("parse server message")?;
                match msg {
                    ServerMsg::HelloOk { connector_id } => {
                        authed = true;
                        info!(%connector_id, "agent online");
                    }
                    ServerMsg::HelloErr { message } => {
                        return Err(anyhow!("auth rejected: {message}"));
                    }
                    ServerMsg::Pong => {}
                    ServerMsg::Dial {
                        stream_id,
                        host,
                        port,
                    } => {
                        if !authed {
                            warn!("dial before hello_ok; ignoring");
                            continue;
                        }
                        spawn_dial(
                            stream_id,
                            host,
                            port,
                            out_tx.clone(),
                            Arc::clone(&streams),
                        );
                    }
                    ServerMsg::Data { stream_id, payload } => {
                        let bytes = B64.decode(payload).unwrap_or_default();
                        if let Some(tx) = streams.lock().await.get(&stream_id) {
                            let _ = tx.send(bytes).await;
                        }
                    }
                    ServerMsg::Close { stream_id } => {
                        streams.lock().await.remove(&stream_id);
                    }
                }
            }
            Message::Ping(data) => {
                // tungstenite may auto-pong; also keep app-level ping path warm
                let _ = data;
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    writer.abort();
    Ok(())
}

fn spawn_dial(
    stream_id: String,
    host: String,
    port: u16,
    out_tx: mpsc::Sender<ClientMsg>,
    streams: Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>>,
) {
    tokio::spawn(async move {
        let addr = format!("{host}:{port}");
        info!(%stream_id, %addr, "dial requested");
        let tcp = match TcpStream::connect(&addr).await {
            Ok(tcp) => tcp,
            Err(err) => {
                let _ = out_tx
                    .send(ClientMsg::DialErr {
                        stream_id: stream_id.clone(),
                        message: err.to_string(),
                    })
                    .await;
                return;
            }
        };

        let _ = out_tx
            .send(ClientMsg::DialOk {
                stream_id: stream_id.clone(),
            })
            .await;

        let (mut reader, mut writer) = tcp.into_split();
        let (down_tx, mut down_rx) = mpsc::channel::<Vec<u8>>(64);
        streams.lock().await.insert(stream_id.clone(), down_tx);

        let sid = stream_id.clone();
        let out_up = out_tx.clone();
        let upload = tokio::spawn(async move {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if out_up
                            .send(ClientMsg::Data {
                                stream_id: sid.clone(),
                                payload: B64.encode(&buf[..n]),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let download = tokio::spawn(async move {
            while let Some(chunk) = down_rx.recv().await {
                if writer.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });

        tokio::select! {
            _ = upload => {}
            _ = download => {}
        }

        streams.lock().await.remove(&stream_id);
        let _ = out_tx
            .send(ClientMsg::Close {
                stream_id: stream_id.clone(),
            })
            .await;
        info!(%stream_id, "stream closed");
    });
}

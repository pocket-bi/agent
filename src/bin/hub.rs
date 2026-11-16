use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use futures_util::{SinkExt, StreamExt};
use pocketbi_agent::{ClientMsg, ServerMsg};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};
use uuid::Uuid;

struct AgentSession {
    connector_id: String,
    /// Send framed messages to the agent websocket writer.
    tx: mpsc::Sender<ServerMsg>,
    /// Pending dials waiting for dial_ok / dial_err.
    pending: Mutex<HashMap<String, oneshot::Sender<Result<(), String>>>>,
    /// Live streams: hub → TCP (data coming from agent).
    down: Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>,
}

type SharedAgent = Arc<Mutex<Option<Arc<AgentSession>>>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let token = env::var("HUB_TOKEN").unwrap_or_else(|_| "dev-token".into());
    let wss_addr: SocketAddr = env::var("HUB_WSS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8700".into())
        .parse()
        .context("HUB_WSS_ADDR")?;
    let forward_addr: SocketAddr = env::var("HUB_FORWARD_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:15432".into())
        .parse()
        .context("HUB_FORWARD_ADDR")?;
    let dial_host = env::var("HUB_DIAL_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let dial_port: u16 = env::var("HUB_DIAL_PORT")
        .unwrap_or_else(|_| "9999".into())
        .parse()
        .context("HUB_DIAL_PORT")?;

    let agent: SharedAgent = Arc::new(Mutex::new(None));

    let agent_wss = Arc::clone(&agent);
    let expected_token = token.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_agents(wss_addr, expected_token, agent_wss).await {
            error!(?err, "agent websocket server exited");
        }
    });

    info!(%forward_addr, %dial_host, dial_port, "TCP forward listening (Sauron stand-in)");
    let listener = TcpListener::bind(forward_addr).await?;
    loop {
        let (socket, peer) = listener.accept().await?;
        let agent = Arc::clone(&agent);
        let dial_host = dial_host.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_forward(socket, peer, agent, dial_host, dial_port).await {
                warn!(?err, %peer, "forward connection ended");
            }
        });
    }
}

async fn serve_agents(addr: SocketAddr, expected_token: String, agent: SharedAgent) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "agent websocket listening on /agent (any path accepted for local demo)");
    loop {
        let (stream, peer) = listener.accept().await?;
        let expected_token = expected_token.clone();
        let agent = Arc::clone(&agent);
        tokio::spawn(async move {
            if let Err(err) = handle_agent(stream, peer, expected_token, agent).await {
                warn!(?err, %peer, "agent session ended");
            }
        });
    }
}

async fn handle_agent(
    stream: TcpStream,
    peer: SocketAddr,
    expected_token: String,
    agent_slot: SharedAgent,
) -> Result<()> {
    let ws = accept_async(stream).await.context("websocket handshake")?;
    let (mut sink, mut stream) = ws.split();
    info!(%peer, "websocket connected; waiting for hello");

    let (tx, mut rx) = mpsc::channel::<ServerMsg>(256);
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg.to_json() {
                Ok(json) => {
                    if sink.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut live: Option<Arc<AgentSession>> = None;

    while let Some(frame) = stream.next().await {
        let frame = frame?;
        let Message::Text(text) = frame else {
            continue;
        };
        let msg = ClientMsg::from_json(&text)?;
        match msg {
            ClientMsg::Hello { token } => {
                if token != expected_token {
                    let _ = tx
                        .send(ServerMsg::HelloErr {
                            message: "invalid token".into(),
                        })
                        .await;
                    break;
                }
                let connector_id = Uuid::new_v4().to_string();
                let sess = Arc::new(AgentSession {
                    connector_id: connector_id.clone(),
                    tx: tx.clone(),
                    pending: Mutex::new(HashMap::new()),
                    down: Mutex::new(HashMap::new()),
                });
                *agent_slot.lock().await = Some(Arc::clone(&sess));
                live = Some(Arc::clone(&sess));
                let _ = tx
                    .send(ServerMsg::HelloOk {
                        connector_id: connector_id.clone(),
                    })
                    .await;
                info!(%connector_id, %peer, "agent authenticated");
            }
            ClientMsg::Ping => {
                let _ = tx.send(ServerMsg::Pong).await;
            }
            ClientMsg::DialOk { stream_id } => {
                if let Some(sess) = &live {
                    if let Some(waiter) = sess.pending.lock().await.remove(&stream_id) {
                        let _ = waiter.send(Ok(()));
                    }
                }
            }
            ClientMsg::DialErr { stream_id, message } => {
                if let Some(sess) = &live {
                    if let Some(waiter) = sess.pending.lock().await.remove(&stream_id) {
                        let _ = waiter.send(Err(message));
                    }
                }
            }
            ClientMsg::Data { stream_id, payload } => {
                if let Some(sess) = &live {
                    let bytes = B64.decode(payload).unwrap_or_default();
                    if let Some(down) = sess.down.lock().await.get(&stream_id) {
                        let _ = down.send(bytes).await;
                    }
                }
            }
            ClientMsg::Close { stream_id } => {
                if let Some(sess) = &live {
                    sess.down.lock().await.remove(&stream_id);
                    if let Some(waiter) = sess.pending.lock().await.remove(&stream_id) {
                        let _ = waiter.send(Err("stream closed".into()));
                    }
                }
            }
        }
    }

    if let Some(sess) = live {
        let mut slot = agent_slot.lock().await;
        if slot
            .as_ref()
            .map(|s| s.connector_id == sess.connector_id)
            .unwrap_or(false)
        {
            *slot = None;
            info!(connector_id = %sess.connector_id, "agent offline");
        }
    }

    writer.abort();
    Ok(())
}

async fn handle_forward(
    socket: TcpStream,
    peer: SocketAddr,
    agent_slot: SharedAgent,
    dial_host: String,
    dial_port: u16,
) -> Result<()> {
    let sess = agent_slot
        .lock()
        .await
        .clone()
        .ok_or_else(|| anyhow!("no agent connected"))?;

    let stream_id = Uuid::new_v4().to_string();
    info!(%peer, %stream_id, host = %dial_host, port = dial_port, "opening tunnel");

    let (wait_tx, wait_rx) = oneshot::channel();
    sess.pending.lock().await.insert(stream_id.clone(), wait_tx);

    sess.tx
        .send(ServerMsg::Dial {
            stream_id: stream_id.clone(),
            host: dial_host,
            port: dial_port,
        })
        .await
        .map_err(|_| anyhow!("agent gone"))?;

    wait_rx
        .await
        .map_err(|_| anyhow!("dial wait dropped"))?
        .map_err(|message| anyhow!("agent dial failed: {message}"))?;

    let (down_tx, mut down_rx) = mpsc::channel::<Vec<u8>>(64);
    sess.down.lock().await.insert(stream_id.clone(), down_tx);

    let (mut reader, mut writer) = socket.into_split();
    let sess_up = Arc::clone(&sess);
    let sid_up = stream_id.clone();
    let upload = tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if sess_up
                        .tx
                        .send(ServerMsg::Data {
                            stream_id: sid_up.clone(),
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

    sess.down.lock().await.remove(&stream_id);
    let _ = sess
        .tx
        .send(ServerMsg::Close {
            stream_id: stream_id.clone(),
        })
        .await;
    info!(%stream_id, "forward closed");
    Ok(())
}

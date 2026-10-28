//! Minimal WebSocket application for trying sipws with a softphone.
//!
//!   cargo run --example echo_app -- 127.0.0.1:8080
//!   cargo run -- --ws-url ws://127.0.0.1:8080/{call_id}
//!
//! Echoes caller audio back. DTMF: `*` clears the playback buffer, `#` hangs up,
//! `1` sends DTMF "123" back to the caller.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:8080".into());
    let listener = TcpListener::bind(&addr).await?;
    println!("echo app listening on ws://{addr}");
    loop {
        let (tcp, peer) = listener.accept().await?;
        tokio::spawn(async move {
            let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else { return };
            println!("[{peer}] connected");
            let (mut tx, mut rx) = ws.split();
            let mut next_id = 1;
            let mut frames = 0u64;
            while let Some(Ok(msg)) = rx.next().await {
                match msg {
                    Message::Binary(b) => {
                        frames += 1;
                        if tx.send(Message::Binary(b)).await.is_err() {
                            break;
                        }
                    }
                    Message::Text(t) => {
                        println!("[{peer}] <- {t}");
                        let v: Value = serde_json::from_str(&t).unwrap_or_default();
                        let req = match (v["method"].as_str(), v["params"]["digit"].as_str()) {
                            (Some("call.dtmf"), Some("*")) => Some(("call.clear", json!({}))),
                            (Some("call.dtmf"), Some("#")) => Some(("call.hangup", json!({"reason": "caller pressed #"}))),
                            (Some("call.dtmf"), Some("1")) => Some(("call.dtmf", json!({"digits": "123"}))),
                            _ => None,
                        };
                        if let Some((method, params)) = req {
                            let r = json!({"jsonrpc": "2.0", "id": next_id, "method": method, "params": params});
                            next_id += 1;
                            println!("[{peer}] -> {r}");
                            let _ = tx.send(Message::Text(r.to_string().into())).await;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            println!("[{peer}] closed after {frames} audio frames");
        });
    }
}

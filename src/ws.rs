//! WebSocket connector: one outbound WebSocket per call, JSON-RPC 2.0 on text
//! frames and raw L16 audio on binary frames.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::config::Endian;

pub type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub struct WsLink {
    pub tx: mpsc::Sender<Message>,
    pub rx: WsStream,
    pub writer: JoinHandle<()>,
}

/// Percent-encodes everything outside RFC 3986 unreserved characters.
fn pct(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

pub fn expand_url(template: &str, vars: &[(&str, &str)]) -> String {
    let mut out = template.to_string();
    for (k, v) in vars {
        out = out.replace(&format!("{{{k}}}"), &pct(v));
    }
    out
}

pub async fn connect(url: &str, headers: &[String], timeout: Duration) -> Result<WsLink> {
    let mut req = url.into_client_request().context("invalid WebSocket URL")?;
    for h in headers {
        let Some((name, value)) = h.split_once(':') else { continue };
        req.headers_mut().insert(
            HeaderName::from_bytes(name.trim().as_bytes())?,
            HeaderValue::from_str(value.trim())?,
        );
    }
    let (ws, _resp) = tokio::time::timeout(timeout, tokio_tungstenite::connect_async(req))
        .await
        .map_err(|_| anyhow!("WebSocket connect timed out"))?
        .context("WebSocket connect failed")?;
    let (mut sink, rx) = ws.split();
    let (tx, mut out_rx) = mpsc::channel::<Message>(256);
    let writer = tokio::spawn(async move {
        while let Some(m) = out_rx.recv().await {
            let is_close = matches!(m, Message::Close(_));
            if sink.send(m).await.is_err() || is_close {
                break;
            }
        }
        let _ = sink.close().await;
    });
    Ok(WsLink { tx, rx, writer })
}

pub fn notification(method: &str, params: Value) -> Message {
    Message::Text(json!({"jsonrpc": "2.0", "method": method, "params": params}).to_string().into())
}

pub fn result(id: Value, result: Value) -> Message {
    Message::Text(json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string().into())
}

pub fn error(id: Value, code: i64, message: &str) -> Message {
    error_data(id, code, message, Value::Null)
}

pub fn error_data(id: Value, code: i64, message: &str, data: Value) -> Message {
    let mut e = json!({"code": code, "message": message});
    if !data.is_null() {
        e["data"] = data;
    }
    Message::Text(json!({"jsonrpc": "2.0", "id": id, "error": e}).to_string().into())
}

pub fn request(id: &str, method: &str, params: Value) -> Message {
    Message::Text(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string().into())
}

pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const SERVER_ERROR: i64 = -32000;

/// A parsed incoming JSON-RPC message.
pub enum Incoming {
    /// Request (id is Some) or notification (id is None).
    Call { id: Option<Value>, method: String, params: Value },
    /// A response to a request we sent (stt.begin, stt.finalize).
    Response { id: Value, result: Result<Value, Value> },
    Invalid { id: Value, code: i64, message: String },
}

pub fn parse_rpc(text: &str) -> Incoming {
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => return Incoming::Invalid { id: Value::Null, code: PARSE_ERROR, message: e.to_string() },
    };
    let id = v.get("id").cloned();
    match v.get("method").and_then(Value::as_str) {
        Some(m) => Incoming::Call {
            id: id.filter(|i| !i.is_null()),
            method: m.to_string(),
            params: v.get("params").cloned().unwrap_or(Value::Null),
        },
        None if v.get("result").is_some() || v.get("error").is_some() => Incoming::Response {
            id: id.unwrap_or(Value::Null),
            result: match v.get("error") {
                Some(e) if !e.is_null() => Err(e.clone()),
                _ => Ok(v.get("result").cloned().unwrap_or(Value::Null)),
            },
        },
        None => Incoming::Invalid {
            id: id.unwrap_or(Value::Null),
            code: INVALID_REQUEST,
            message: "missing method".into(),
        },
    }
}

pub fn pcm_to_bytes(pcm: &[i16], endian: Endian) -> Vec<u8> {
    let mut out = Vec::with_capacity(pcm.len() * 2);
    for &s in pcm {
        out.extend_from_slice(&match endian {
            Endian::Le => s.to_le_bytes(),
            Endian::Be => s.to_be_bytes(),
        });
    }
    out
}

/// Converts bytes to samples, carrying an odd trailing byte over to the next frame.
pub fn bytes_to_pcm(data: &[u8], carry: &mut Option<u8>, endian: Endian, out: &mut Vec<i16>) {
    let mut it = data.iter().copied();
    let mut first = carry.take().or_else(|| it.next());
    while let Some(a) = first {
        match it.next() {
            Some(b) => out.push(match endian {
                Endian::Le => i16::from_le_bytes([a, b]),
                Endian::Be => i16::from_be_bytes([a, b]),
            }),
            None => {
                *carry = Some(a);
                break;
            }
        }
        first = it.next();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_template() {
        let u = expand_url("ws://h/{call_id}?to={to_user}", &[("call_id", "a b@c"), ("to_user", "+40")]);
        assert_eq!(u, "ws://h/a%20b%40c?to=%2B40");
    }

    #[test]
    fn pcm_bytes_with_odd_split() {
        let pcm = [1i16, -2, 300, i16::MIN];
        for e in [Endian::Le, Endian::Be] {
            let b = pcm_to_bytes(&pcm, e);
            let mut carry = None;
            let mut out = Vec::new();
            bytes_to_pcm(&b[..3], &mut carry, e, &mut out);
            bytes_to_pcm(&b[3..], &mut carry, e, &mut out);
            assert_eq!(out, pcm);
            assert!(carry.is_none());
        }
    }

    #[test]
    fn rpc_parsing() {
        assert!(matches!(parse_rpc("{"), Incoming::Invalid { code: PARSE_ERROR, .. }));
        assert!(matches!(parse_rpc(r#"{"jsonrpc":"2.0","id":1,"result":true}"#), Incoming::Response { result: Ok(_), .. }));
        match parse_rpc(r#"{"jsonrpc":"2.0","id":"stt-begin","error":{"code":-32601,"message":"x"}}"#) {
            Incoming::Response { id, result: Err(e) } => assert!(id == "stt-begin" && e["code"] == -32601),
            _ => panic!(),
        }
        match parse_rpc(r#"{"jsonrpc":"2.0","method":"call.clear"}"#) {
            Incoming::Call { id, method, .. } => assert!(id.is_none() && method == "call.clear"),
            _ => panic!(),
        }
    }
}

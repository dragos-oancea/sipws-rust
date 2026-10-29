//! Mock speech-to-text engine speaking `sipws.stt/1` (docs/stt-protocol.md).
//!
//!   cargo run --example stt_mock -- 127.0.0.1:8090
//!   cargo run -- --ws-url ws://127.0.0.1:8090/{call_id} --barge-in speech --transcript-dir ./transcripts
//!
//! It runs an energy VAD on the caller's L16 audio and reports each stretch of speech as an
//! utterance: speech_started, a partial every 200 ms, then a final after 400 ms of silence.
//! The "text" is a description of the audio (no real recognition). After each final it plays
//! a 1.5 s tone back to the caller as a stand-in for a bot reply. Talk over the tone to see
//! barge-in. Requires --ws-out-format l16 (the default).

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

const SPEECH_RMS: f64 = 700.0;
const HANGOVER_MS: u64 = 400;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:8090".into());
    let listener = TcpListener::bind(&addr).await?;
    println!("mock STT engine on ws://{addr}");
    loop {
        let (tcp, peer) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = session(tcp).await {
                println!("[{peer}] {e:#}");
            }
        });
    }
}

struct Utt {
    id: String,
    start_ms: u64,
    last_voice_ms: u64,
    seq: u64,
    peak: f64,
}

fn text_for(u: &Utt, now: u64) -> String {
    format!("speech {:.1} seconds peak {:.0}", (now - u.start_ms) as f64 / 1000.0, u.peak)
}

async fn session(tcp: tokio::net::TcpStream) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(tcp).await?;
    let (mut tx, mut rx) = ws.split();
    let send = |v: Value| Message::Text(v.to_string().into());

    let mut rate = 16000u64;
    let mut big_endian = false;
    let mut samples: u64 = 0;
    let mut cur: Option<Utt> = None;
    let mut n = 0u32;
    let mut next_id = 1u64;

    while let Some(msg) = rx.next().await {
        match msg? {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t)?;
                match v["method"].as_str() {
                    Some("stt.begin") => {
                        let a = &v["params"]["audio"];
                        if a["encoding"] != "L16" {
                            tx.send(send(json!({"jsonrpc":"2.0","id":v["id"],"error":{"code":-32602,"message":"mock needs L16"}}))).await?;
                            continue;
                        }
                        rate = a["sample_rate"].as_u64().unwrap_or(16000);
                        big_endian = a["endianness"] == "big";
                        println!("call {} languages {}", v["params"]["call_id"], v["params"]["languages"]);
                        tx.send(send(json!({"jsonrpc":"2.0","id":v["id"],"result":{
                            "protocol":"sipws.stt/1","engine":"mock-vad",
                            "features":{"interim_results":true,"word_timestamps":false,"speech_events":true,"end_of_turn":true}}}))).await?;
                    }
                    Some("stt.finalize") => {
                        let now = samples * 1000 / rate;
                        if let Some(u) = cur.take() {
                            tx.send(send(json!({"jsonrpc":"2.0","method":"stt.final","params":{
                                "utterance_id":u.id,"text":text_for(&u, now),"start_ms":u.start_ms,"end_ms":now,
                                "confidence":0.5,"end_of_turn":true}}))).await?;
                        }
                        tx.send(send(json!({"jsonrpc":"2.0","id":v["id"],"result":{"utterances":n}}))).await?;
                    }
                    Some(m) => println!("<- {m} {}", v["params"]),
                    None if v.get("error").is_some() => println!("<- error {}", v["error"]),
                    None => {}
                }
            }
            Message::Binary(b) => {
                let pcm: Vec<i16> = b
                    .chunks_exact(2)
                    .map(|c| if big_endian { i16::from_be_bytes([c[0], c[1]]) } else { i16::from_le_bytes([c[0], c[1]]) })
                    .collect();
                let start_ms = samples * 1000 / rate;
                samples += pcm.len() as u64;
                let now = samples * 1000 / rate;
                let rms = (pcm.iter().map(|&s| (s as f64).powi(2)).sum::<f64>() / pcm.len().max(1) as f64).sqrt();
                let voiced = rms > SPEECH_RMS;

                if voiced && cur.is_none() {
                    n += 1;
                    let u = Utt { id: format!("u{n}"), start_ms, last_voice_ms: now, seq: 0, peak: rms };
                    tx.send(send(json!({"jsonrpc":"2.0","method":"stt.speech_started",
                        "params":{"utterance_id":u.id,"start_ms":start_ms}}))).await?;
                    cur = Some(u);
                }
                let Some(u) = cur.as_mut() else { continue };
                if voiced {
                    u.last_voice_ms = now;
                    u.peak = u.peak.max(rms);
                }
                if now - u.last_voice_ms >= HANGOVER_MS {
                    let u = cur.take().unwrap();
                    let text = text_for(&u, u.last_voice_ms);
                    println!("final {}: {text}", u.id);
                    let id = next_id;
                    next_id += 1;
                    tx.send(send(json!({"jsonrpc":"2.0","id":id,"method":"stt.final","params":{
                        "utterance_id":u.id,"text":text,"start_ms":u.start_ms,"end_ms":u.last_voice_ms,
                        "confidence":0.5,"end_of_turn":true}}))).await?;
                    tx.send(send(json!({"jsonrpc":"2.0","method":"stt.speech_ended",
                        "params":{"utterance_id":u.id,"end_ms":u.last_voice_ms}}))).await?;
                    // "Bot reply": 1.5 s of 600 Hz, then a mark.
                    let reply: Vec<u8> = (0..rate * 3 / 2)
                        .flat_map(|i| {
                            let s = ((i as f64 * 2.0 * std::f64::consts::PI * 600.0 / rate as f64).sin() * 6000.0) as i16;
                            if big_endian { s.to_be_bytes() } else { s.to_le_bytes() }
                        })
                        .collect();
                    tx.send(Message::Binary(reply.into())).await?;
                    tx.send(send(json!({"jsonrpc":"2.0","method":"call.mark","params":{"name":format!("reply-{}", u.id)}}))).await?;
                } else if now / 200 != start_ms / 200 {
                    u.seq += 1;
                    tx.send(send(json!({"jsonrpc":"2.0","method":"stt.partial","params":{
                        "utterance_id":u.id,"seq":u.seq,"text":text_for(u, now),"start_ms":u.start_ms,"end_ms":now}}))).await?;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

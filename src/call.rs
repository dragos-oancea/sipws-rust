//! Per-call media bridge: RTP <-> transcoder <-> L16 frames on the WebSocket,
//! plus the JSON-RPC control surface.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::codec::{CodecKind, Transcoder};
use crate::config::Config;
use crate::rtp::{dtmf_char, dtmf_event, DtmfEvent, RtpPacket, RtpSocket};
use crate::sip::message::{addr_uri, uri_user};
use crate::sip::sdp::Negotiated;
use crate::sip::server::{Dialog, EndReason, SipServer};
use crate::ws::{self, Incoming};

pub enum MediaCtl {
    /// Remote RTP address changed via re-INVITE/UPDATE.
    RemoteAddr(SocketAddr),
    /// DTMF received via SIP INFO.
    Dtmf { digit: char, duration_ms: u32 },
}

pub async fn run_call(
    server: Arc<SipServer>,
    d: Arc<Dialog>,
    neg: Negotiated,
    rtp: RtpSocket,
    answer_sdp: String,
    media_rx: mpsc::UnboundedReceiver<MediaCtl>,
) {
    if let Err(e) = bridge(&server, &d, neg, rtp, answer_sdp, media_rx).await {
        warn!(call_id = d.call_id, "call failed: {e:#}");
        d.terminate(EndReason::Error(e.to_string()));
    }
}

/// Outbound DTMF (RFC 4733) generator state.
struct DtmfTx {
    queue: VecDeque<(u8, u32)>, // (event, duration in RTP clock units)
    current: Option<DtmfCur>,
    gap_ticks: u32,
}

struct DtmfCur {
    event: u8,
    start_ts: u32,
    elapsed: u32,
    total: u32,
    first: bool,
}

struct Playout {
    buf: VecDeque<i16>,
    max: usize,
    enqueued: u64,
    consumed: u64,
    marks: VecDeque<(u64, String)>,
    carry: Option<u8>,
}

impl Playout {
    fn push(&mut self, pcm: &[i16]) {
        self.buf.extend(pcm);
        self.enqueued += pcm.len() as u64;
        if self.buf.len() > self.max {
            let drop = self.buf.len() - self.max;
            self.buf.drain(..drop);
            self.consumed += drop as u64;
        }
    }

    fn take(&mut self, n: usize, out: &mut Vec<i16>) {
        out.clear();
        let k = n.min(self.buf.len());
        out.extend(self.buf.drain(..k));
        self.consumed += k as u64;
        out.resize(n, 0);
    }

    fn clear(&mut self) -> usize {
        let n = self.buf.len();
        self.buf.clear();
        self.consumed = self.enqueued;
        self.carry = None;
        n
    }

    /// Marks whose audio has been fully played (or cleared).
    fn due_marks(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        while self.marks.front().is_some_and(|(pos, _)| *pos <= self.consumed) {
            out.push(self.marks.pop_front().unwrap().1);
        }
        out
    }
}

async fn send_ctl(tx: &mpsc::Sender<Message>, m: Message) {
    let _ = tokio::time::timeout(Duration::from_secs(1), tx.send(m)).await;
}

async fn bridge(
    server: &Arc<SipServer>,
    d: &Arc<Dialog>,
    neg: Negotiated,
    rtp: RtpSocket,
    answer_sdp: String,
    mut media_rx: mpsc::UnboundedReceiver<MediaCtl>,
) -> anyhow::Result<()> {
    let cfg: &Config = &server.cfg;
    let inv = &d.invite;
    let from_uri = addr_uri(inv.header("From").unwrap_or_default()).to_string();
    let to_uri = addr_uri(inv.header("To").unwrap_or_default()).to_string();
    let req_uri = inv.uri().unwrap_or_default().to_string();

    // 1. Dial the application.
    let url = ws::expand_url(
        &cfg.ws_url,
        &[
            ("call_id", &d.call_id),
            ("from_user", uri_user(&from_uri)),
            ("to_user", uri_user(&req_uri)),
        ],
    );
    let link = tokio::select! {
        r = ws::connect(&url, &cfg.ws_headers, Duration::from_millis(cfg.ws_connect_timeout_ms)) => r,
        _ = d.cancel.cancelled() => return Ok(()),
    };
    let ws::WsLink { tx: ws_tx, rx: mut ws_rx, writer } = match link {
        Ok(l) => l,
        Err(e) => {
            warn!(call_id = d.call_id, url, "{e:#}");
            server.reject(d, 503, "Service Unavailable").await;
            d.terminate(EndReason::Error("ws_connect_failed".into()));
            return Ok(());
        }
    };
    debug!(call_id = d.call_id, url, "WebSocket connected");

    let mut sip_headers = serde_json::Map::new();
    for (name, value) in &inv.headers {
        let lname = name.to_ascii_lowercase();
        if cfg.forward_headers.iter().any(|p| lname.starts_with(&p.to_ascii_lowercase())) {
            sip_headers.insert(name.clone(), Value::String(value.clone()));
        }
    }
    let ws_rate = cfg.ws_sample_rate;
    send_ctl(
        &ws_tx,
        ws::notification(
            "call.start",
            json!({
                "call_id": d.call_id,
                "from": from_uri,
                "to": to_uri,
                "request_uri": req_uri,
                "sip_headers": sip_headers,
                "media": {
                    "encoding": "L16",
                    "sample_rate": ws_rate,
                    "channels": 1,
                    "endianness": match cfg.ws_endian { crate::config::Endian::Le => "little", crate::config::Endian::Be => "big" },
                    "frame_ms": cfg.ws_frame_ms,
                    "rtp_codec": neg.codec.name(),
                    "dtmf": neg.dtmf_pt.is_some(),
                }
            }),
        ),
    )
    .await;

    // 2. Answer the SIP leg.
    if !server.answer(d, answer_sdp).await {
        finish_ws(&ws_tx, writer, d).await;
        return Ok(());
    }
    info!(call_id = d.call_id, "call answered");

    // 3. Media loop.
    let mut tc = Transcoder::new(neg.codec, ws_rate, cfg.ptime)?;
    let frame_samples = tc.frame_samples();
    let ws_frame_samples = (ws_rate * cfg.ws_frame_ms / 1000) as usize;
    let clock = neg.codec.clock_rate();
    let ts_step = clock * cfg.ptime / 1000;

    let mut remote = SocketAddr::new(neg.remote_ip, neg.remote_port);
    let mut latched = false;
    let ssrc: u32 = rand::random();
    let mut seq: u16 = rand::random();
    let mut ts: u32 = rand::random();
    let mut first_packet = true;

    let mut rx_buf = vec![0u8; 2048];
    let mut tx_pkt = Vec::with_capacity(1500);
    let mut enc_buf = Vec::with_capacity(1500);
    let mut frame = Vec::with_capacity(frame_samples);
    let mut to_ws: Vec<i16> = Vec::with_capacity(ws_frame_samples * 4);
    let mut in_ssrc: Option<u32> = None;
    let mut expected_seq: u16 = 0;
    let mut last_dtmf_ts: Option<u32> = None;
    let mut last_rtp = Instant::now();
    let mut dropped_frames: u64 = 0;

    let mut play = Playout {
        buf: VecDeque::new(),
        max: (ws_rate as u64 * cfg.max_playout_ms as u64 / 1000) as usize,
        enqueued: 0,
        consumed: 0,
        marks: VecDeque::new(),
        carry: None,
    };
    let mut dtmf = DtmfTx { queue: VecDeque::new(), current: None, gap_ticks: 0 };

    let mut tick = tokio::time::interval(Duration::from_millis(cfg.ptime as u64));
    let rtp_timeout = (cfg.rtp_timeout_secs > 0).then(|| Duration::from_secs(cfg.rtp_timeout_secs));

    loop {
        tokio::select! {
            _ = d.cancel.cancelled() => break,

            msg = ws_rx.next() => match msg {
                Some(Ok(Message::Binary(data))) => {
                    let mut pcm = Vec::with_capacity(data.len() / 2 + 1);
                    ws::bytes_to_pcm(&data, &mut play.carry, cfg.ws_endian, &mut pcm);
                    play.push(&pcm);
                }
                Some(Ok(Message::Text(text))) => {
                    for reply in handle_rpc(&text, d, &mut play, &mut dtmf, &neg, clock) {
                        send_ctl(&ws_tx, reply).await;
                    }
                }
                Some(Ok(Message::Close(_))) | None => {
                    d.terminate(EndReason::WsClosed);
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    warn!(call_id = d.call_id, "WebSocket error: {e}");
                    d.terminate(EndReason::WsClosed);
                    break;
                }
            },

            r = rtp.socket.recv_from(&mut rx_buf) => {
                let Ok((n, src)) = r else { continue };
                let Some(pkt) = RtpPacket::parse(&rx_buf[..n]) else { continue };
                if cfg.symmetric_rtp && !latched {
                    if src != remote {
                        debug!(call_id = d.call_id, %src, "latching RTP to source address");
                    }
                    remote = src;
                    latched = true;
                }
                last_rtp = Instant::now();

                if pkt.pt == neg.pt {
                    if in_ssrc != Some(pkt.ssrc) {
                        in_ssrc = Some(pkt.ssrc);
                        expected_seq = pkt.seq;
                    }
                    let gap = pkt.seq.wrapping_sub(expected_seq) as i16;
                    if gap < 0 {
                        continue; // late or duplicate
                    }
                    if (1..=5).contains(&gap) {
                        for _ in 0..gap { tc.conceal(&mut to_ws); }
                    }
                    expected_seq = pkt.seq.wrapping_add(1);
                    if let Err(e) = tc.decode(pkt.payload, &mut to_ws) {
                        debug!(call_id = d.call_id, "decode error: {e}");
                    }
                    while to_ws.len() >= ws_frame_samples {
                        let bytes = ws::pcm_to_bytes(&to_ws[..ws_frame_samples], cfg.ws_endian);
                        to_ws.drain(..ws_frame_samples);
                        if ws_tx.try_send(Message::Binary(bytes.into())).is_err() {
                            dropped_frames += 1;
                            if dropped_frames.is_power_of_two() {
                                warn!(call_id = d.call_id, dropped_frames, "WebSocket backpressure, dropping audio");
                            }
                        }
                    }
                } else if Some(pkt.pt) == neg.dtmf_pt
                    && let Some(ev) = DtmfEvent::parse(pkt.payload)
                        && ev.end && last_dtmf_ts != Some(pkt.ts) {
                            last_dtmf_ts = Some(pkt.ts);
                            if let Some(digit) = dtmf_char(ev.event) {
                                let ms = ev.duration as u32 * 1000 / clock;
                                send_ctl(&ws_tx, ws::notification("call.dtmf",
                                    json!({"digit": digit.to_string(), "duration_ms": ms, "source": "rfc4733"}))).await;
                            }
                        }
            },

            _ = tick.tick() => {
                if let Some(t) = rtp_timeout
                    && last_rtp.elapsed() > t {
                        info!(call_id = d.call_id, "RTP timeout");
                        d.terminate(EndReason::RtpTimeout);
                        break;
                    }
                let mut marker = first_packet;
                first_packet = false;
                let mut payload_pt = neg.pt;

                if dtmf.current.is_none() && dtmf.gap_ticks == 0
                    && let Some((event, total)) = dtmf.queue.pop_front() {
                        dtmf.current = Some(DtmfCur { event, start_ts: ts, elapsed: 0, total, first: true });
                    }
                if let Some(cur) = dtmf.current.as_mut() {
                    // Audio playout is paused while an event is being sent.
                    cur.elapsed = (cur.elapsed + ts_step).min(cur.total);
                    let end = cur.elapsed >= cur.total;
                    let ev = DtmfEvent { event: cur.event, end, volume: 10, duration: cur.elapsed.min(0xFFFF) as u16 };
                    marker = cur.first;
                    cur.first = false;
                    payload_pt = neg.dtmf_pt.unwrap_or(neg.pt);
                    let bytes = ev.to_bytes();
                    let p = RtpPacket { marker, pt: payload_pt, seq, ts: cur.start_ts, ssrc, payload: &bytes };
                    p.write(&mut tx_pkt);
                    let reps = if end { 3 } else { 1 };
                    for _ in 0..reps {
                        let _ = rtp.socket.send_to(&tx_pkt, remote).await;
                        seq = seq.wrapping_add(1);
                        let p = RtpPacket { marker: false, pt: payload_pt, seq, ts: cur.start_ts, ssrc, payload: &bytes };
                        p.write(&mut tx_pkt);
                    }
                    if end {
                        dtmf.current = None;
                        dtmf.gap_ticks = (60 / cfg.ptime).max(1);
                    }
                } else {
                    dtmf.gap_ticks = dtmf.gap_ticks.saturating_sub(1);
                    play.take(frame_samples, &mut frame);
                    if let Err(e) = tc.encode(&frame, &mut enc_buf) {
                        debug!(call_id = d.call_id, "encode error: {e}");
                    } else {
                        let p = RtpPacket { marker, pt: payload_pt, seq, ts, ssrc, payload: &enc_buf };
                        p.write(&mut tx_pkt);
                        let _ = rtp.socket.send_to(&tx_pkt, remote).await;
                        seq = seq.wrapping_add(1);
                    }
                }
                ts = ts.wrapping_add(ts_step);

                for name in play.due_marks() {
                    send_ctl(&ws_tx, ws::notification("call.mark", json!({"name": name, "cleared": false}))).await;
                }
            },

            Some(ctl) = media_rx.recv() => match ctl {
                MediaCtl::RemoteAddr(a) => {
                    if a != remote {
                        info!(call_id = d.call_id, %a, "remote RTP address updated");
                    }
                    remote = a;
                    latched = false;
                }
                MediaCtl::Dtmf { digit, duration_ms } => {
                    send_ctl(&ws_tx, ws::notification("call.dtmf",
                        json!({"digit": digit.to_string(), "duration_ms": duration_ms, "source": "sip_info"}))).await;
                }
            },
        }
    }

    finish_ws(&ws_tx, writer, d).await;
    Ok(())
}

async fn finish_ws(ws_tx: &mpsc::Sender<Message>, writer: tokio::task::JoinHandle<()>, d: &Dialog) {
    let reason = d.end_reason().map(|r| r.as_str()).unwrap_or_else(|| "unknown".into());
    if d.end_reason() != Some(EndReason::WsClosed) {
        send_ctl(ws_tx, ws::notification("call.end", json!({"call_id": d.call_id, "reason": reason}))).await;
        send_ctl(ws_tx, Message::Close(None)).await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
}

fn handle_rpc(
    text: &str,
    d: &Dialog,
    play: &mut Playout,
    dtmf: &mut DtmfTx,
    neg: &Negotiated,
    clock: u32,
) -> Vec<Message> {
    let (id, method, params) = match ws::parse_rpc(text) {
        Incoming::Call { id, method, params } => (id, method, params),
        Incoming::Response => return vec![],
        Incoming::Invalid { id, code, message } => return vec![ws::error(id, code, &message)],
    };
    let mut out = Vec::new();
    let outcome: Result<Value, (i64, String)> = match method.as_str() {
        "call.hangup" => {
            let reason = params.get("reason").and_then(Value::as_str).unwrap_or("").to_string();
            d.terminate(EndReason::AppHangup(reason));
            Ok(json!(true))
        }
        "call.clear" => {
            let samples = play.clear();
            // Pending marks are reported back as cleared, so the app can track what was cut.
            let cleared: Vec<String> = play.marks.drain(..).map(|(_, n)| n).collect();
            for name in &cleared {
                out.push(ws::notification("call.mark", json!({"name": name, "cleared": true})));
            }
            Ok(json!({"cleared_samples": samples, "cleared_marks": cleared}))
        }
        "call.mark" => match params.get("name").and_then(Value::as_str) {
            Some(name) => {
                play.marks.push_back((play.enqueued, name.to_string()));
                Ok(json!(true))
            }
            None => Err((ws::INVALID_PARAMS, "params.name (string) required".into())),
        },
        "call.dtmf" => {
            let digits = params.get("digits").and_then(Value::as_str).unwrap_or("");
            let ms = params.get("duration_ms").and_then(Value::as_u64).unwrap_or(100).clamp(40, 2000) as u32;
            if neg.dtmf_pt.is_none() {
                Err((ws::SERVER_ERROR, "telephone-event not negotiated on this call".into()))
            } else if digits.is_empty() || !digits.chars().all(|c| dtmf_event(c).is_some()) {
                Err((ws::INVALID_PARAMS, "params.digits must be non-empty and contain 0-9*#A-D".into()))
            } else {
                for c in digits.chars() {
                    dtmf.queue.push_back((dtmf_event(c).unwrap(), ms * clock / 1000));
                }
                Ok(json!({"queued": digits.len()}))
            }
        }
        "call.status" => Ok(json!({
            "call_id": d.call_id,
            "codec": match neg.codec { CodecKind::Opus => "opus", CodecKind::Pcmu => "PCMU", CodecKind::Pcma => "PCMA" },
            "buffered_samples": play.buf.len(),
            "pending_marks": play.marks.len(),
        })),
        "ping" | "call.ping" => Ok(json!("pong")),
        _ => Err((ws::METHOD_NOT_FOUND, format!("unknown method {method}"))),
    };
    if let Some(id) = id {
        out.insert(0, match outcome {
            Ok(v) => ws::result(id, v),
            Err((code, msg)) => ws::error(id, code, &msg),
        });
    }
    out
}

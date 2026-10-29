//! Per-call media bridge.
//!
//! ```text
//!  RTP in ─► jitter buffer ─► (every ptime tick) ─┬─ L16:       decode/PLC/FEC ─► PCM frames ─► WS binary
//!                                                  ├─ ogg-opus:  Opus passthrough ─► Ogg pages ─► WS binary
//!                                                  └─ ogg-opus:  decode ─► Opus encode ─► Ogg pages ─► WS binary
//!  WS binary (L16 or Ogg/Opus) ─► PCM playout queue ─► (tick) encode ─► RTP out
//!  WS text ◄─► JSON-RPC 2.0
//! ```

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
use crate::config::{Config, Endian, WsFormat};
use crate::jitter::{JbItem, JitterBuffer};
use crate::ogg::{opus_dtx_packets, opus_packet_dur, OggMuxer, OggOpusReader};
use crate::rtp::{dtmf_char, dtmf_event, DtmfEvent, RtpPacket, RtpSocket};
use crate::sip::message::{addr_uri, uri_user};
use crate::sip::sdp::Negotiated;
use crate::sip::server::{Dialog, EndReason, SipServer};
use crate::stt::{self, Effect};
use crate::transcript::{now_rfc3339, Transcript};
use crate::ws::{self, Incoming};

pub enum MediaCtl {
    /// Remote RTP address and/or direction changed via re-INVITE/UPDATE.
    Remote { addr: SocketAddr, direction: &'static str },
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

/// Inbound DTMF detection: report on the end packet, or when a new event starts
/// and the previous one never saw its end (all end packets lost).
#[derive(Default)]
struct DtmfRx {
    ts: Option<u32>,
    event: u8,
    duration: u16,
    reported: bool,
}

impl DtmfRx {
    /// Returns (event, duration) to report, if any.
    fn on_packet(&mut self, ts: u32, ev: DtmfEvent) -> Vec<(u8, u16)> {
        let mut out = Vec::new();
        if self.ts != Some(ts) {
            if self.ts.is_some() && !self.reported {
                out.push((self.event, self.duration));
            }
            *self = DtmfRx { ts: Some(ts), event: ev.event, duration: ev.duration, reported: false };
        }
        self.duration = self.duration.max(ev.duration);
        if ev.end && !self.reported {
            self.reported = true;
            out.push((self.event, self.duration));
        }
        out
    }
}

struct Playout {
    buf: VecDeque<i16>,
    max: usize,
    enqueued: u64,
    consumed: u64,
    marks: VecDeque<(u64, String)>,
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

/// Call audio → app, in the configured WebSocket format.
enum ToApp {
    L16 { pcm: Vec<i16>, frame: usize, endian: Endian },
    /// Opus packets from RTP copied into Ogg untouched; loss/silence become DTX frames.
    OggPassthrough { mux: OggMuxer },
    /// Decoded audio re-encoded with our own Opus encoder (G.711 calls, or passthrough off).
    OggEncode { mux: OggMuxer, enc: opus::Encoder, pcm: Vec<i16>, frame: usize, buf: Vec<u8> },
}

impl ToApp {
    /// Turns one jitter-buffer item into zero or more WebSocket binary messages.
    fn feed(&mut self, item: JbItem, tc: &mut Transcoder, clock: u32, out: &mut Vec<Vec<u8>>) {
        if let ToApp::OggPassthrough { mux } = self {
            match item {
                JbItem::Packet { payload, dur } => out.extend(mux.push(&payload, dur)),
                JbItem::Lost { dur, .. } | JbItem::Silence { dur } => {
                    for (p, n) in opus_dtx_packets(dur) {
                        out.extend(mux.push(&p, n));
                    }
                }
            }
            return;
        }
        let pcm = match self {
            ToApp::L16 { pcm, .. } | ToApp::OggEncode { pcm, .. } => pcm,
            ToApp::OggPassthrough { .. } => unreachable!(),
        };
        match item {
            JbItem::Packet { payload, .. } => {
                if let Err(e) = tc.decode(&payload, pcm) {
                    debug!("decode error: {e}");
                }
            }
            JbItem::Lost { dur, fec } => {
                let n = tc.clock_to_pcm(dur, clock);
                let recovered = fec.is_some_and(|next| tc.decode_fec(&next, n, pcm));
                if !recovered {
                    tc.conceal(n, pcm);
                }
            }
            JbItem::Silence { dur } => {
                let n = tc.clock_to_pcm(dur, clock);
                tc.silence(n, pcm);
            }
        }
        match self {
            ToApp::L16 { pcm, frame, endian } => {
                while pcm.len() >= *frame {
                    out.push(ws::pcm_to_bytes(&pcm[..*frame], *endian));
                    pcm.drain(..*frame);
                }
            }
            ToApp::OggEncode { mux, enc, pcm, frame, buf } => {
                while pcm.len() >= *frame {
                    buf.resize(1500, 0);
                    match enc.encode(&pcm[..*frame], buf) {
                        Ok(n) => out.extend(mux.push(&buf[..n], 960)),
                        Err(e) => debug!("opus encode error: {e}"),
                    }
                    pcm.drain(..*frame);
                }
            }
            ToApp::OggPassthrough { .. } => {}
        }
    }

    fn finish(&mut self) -> Option<Vec<u8>> {
        match self {
            ToApp::OggPassthrough { mux } | ToApp::OggEncode { mux, .. } => mux.finish(),
            ToApp::L16 { .. } => None,
        }
    }
}

/// App audio → caller.
enum FromApp {
    L16 { carry: Option<u8>, endian: Endian },
    OggOpus(Box<OggOpusReader>),
}

impl FromApp {
    fn feed(&mut self, data: &[u8], out: &mut Vec<i16>) {
        match self {
            FromApp::L16 { carry, endian } => ws::bytes_to_pcm(data, carry, *endian, out),
            FromApp::OggOpus(r) => r.feed(data, out),
        }
    }
}

async fn send_ctl(tx: &mpsc::Sender<Message>, m: Message) {
    let _ = tokio::time::timeout(Duration::from_secs(1), tx.send(m)).await;
}

fn g711_dur(p: &[u8]) -> Option<u32> {
    Some(p.len() as u32)
}

fn sends(direction: &str) -> bool {
    matches!(direction, "sendrecv" | "sendonly")
}

fn receives(direction: &str) -> bool {
    matches!(direction, "sendrecv" | "recvonly")
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

    // 2. Media pipeline setup.
    let pcm_rate = cfg.ws_sample_rate;
    let clock = neg.codec.clock_rate();
    let ts_step = clock * cfg.ptime / 1000;
    let mut tc = Transcoder::new(neg.codec, pcm_rate, cfg.ptime)?;
    let frame_samples = tc.frame_samples();
    let page_dur = 48 * cfg.ws_frame_ms;
    let serial: u32 = rand::random();

    let mut to_app = match cfg.ws_out_format {
        WsFormat::L16 => ToApp::L16 {
            pcm: Vec::new(),
            frame: (pcm_rate * cfg.ws_frame_ms / 1000) as usize,
            endian: cfg.ws_endian,
        },
        WsFormat::OggOpus if neg.codec == CodecKind::Opus && cfg.ogg_passthrough => {
            ToApp::OggPassthrough { mux: OggMuxer::new(serial, page_dur) }
        }
        WsFormat::OggOpus => {
            let mut enc = opus::Encoder::new(pcm_rate, opus::Channels::Mono, opus::Application::Voip)?;
            enc.set_bitrate(opus::Bitrate::Bits(cfg.ogg_bitrate))?;
            ToApp::OggEncode {
                mux: OggMuxer::new(serial, page_dur),
                enc,
                pcm: Vec::new(),
                frame: (pcm_rate / 50) as usize,
                buf: Vec::with_capacity(1500),
            }
        }
    };
    let mut from_app = match cfg.ws_in_format {
        WsFormat::L16 => FromApp::L16 { carry: None, endian: cfg.ws_endian },
        WsFormat::OggOpus => FromApp::OggOpus(Box::new(OggOpusReader::new(pcm_rate))),
    };

    let mut sip_headers = serde_json::Map::new();
    for (name, value) in &inv.headers {
        let lname = name.to_ascii_lowercase();
        if cfg.forward_headers.iter().any(|p| lname.starts_with(&p.to_ascii_lowercase())) {
            sip_headers.insert(name.clone(), Value::String(value.clone()));
        }
    }
    let endianness = match cfg.ws_endian {
        Endian::Le => "little",
        Endian::Be => "big",
    };
    let to_app_desc: Value = match &to_app {
        ToApp::L16 { .. } => json!({
            "encoding": "L16", "sample_rate": pcm_rate, "channels": 1,
            "endianness": endianness, "frame_ms": cfg.ws_frame_ms,
        }),
        ToApp::OggPassthrough { .. } | ToApp::OggEncode { .. } => json!({
            "encoding": "ogg/opus", "sample_rate": 48000, "channels": 1, "page_ms": cfg.ws_frame_ms,
            "opus": if matches!(to_app, ToApp::OggPassthrough { .. }) { "passthrough" } else { "encoded" },
            "serial": serial,
        }),
    };
    let from_app_desc = match cfg.ws_in_format {
        WsFormat::L16 => json!({"encoding": "L16", "sample_rate": pcm_rate, "channels": 1, "endianness": endianness}),
        WsFormat::OggOpus => json!({"encoding": "ogg/opus"}),
    };
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
                    "to_app": to_app_desc.clone(),
                    "from_app": from_app_desc,
                    "rtp_codec": neg.codec.name(),
                    "dtmf": neg.dtmf_pt.is_some(),
                }
            }),
        ),
    )
    .await;

    // STT handshake (docs/stt-protocol.md §3). Apps that don't do STT answer with an error.
    let header_list = |name: &str, default: &[String]| -> Vec<String> {
        match inv.header(name) {
            Some(v) => v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            None => default.to_vec(),
        }
    };
    let stt_languages = header_list("X-STT-Language", &cfg.stt_languages);
    let mut stt = SttState {
        session: stt::Session::new(cfg.barge_in, cfg.barge_in_min_chars),
        lines: Vec::new(),
        finalize_done: false,
    };
    if cfg.stt {
        send_ctl(
            &ws_tx,
            ws::request(
                stt::BEGIN_ID,
                "stt.begin",
                json!({
                    "protocol": stt::PROTOCOL,
                    "call_id": d.call_id,
                    "tracks": ["caller"],
                    "audio": to_app_desc,
                    "languages": stt_languages,
                    "interim_results": cfg.stt_interim,
                    "word_timestamps": cfg.stt_words,
                    "max_alternatives": cfg.stt_max_alternatives,
                    "hints": header_list("X-STT-Hints", &cfg.stt_hints),
                }),
            ),
        )
        .await;
    } else {
        stt.session.state = stt::State::Inactive;
    }

    // Ogg streams start with OpusHead + OpusTags pages, each its own binary message.
    let comments = vec![format!("SIPWS_CALL_ID={}", d.call_id), format!("SIPWS_FROM={from_uri}"), format!("SIPWS_TO={to_uri}")];
    match &mut to_app {
        ToApp::OggPassthrough { mux } => {
            for page in mux.headers(0, 48000, &comments) {
                send_ctl(&ws_tx, Message::Binary(page.into())).await;
            }
        }
        ToApp::OggEncode { mux, enc, .. } => {
            let lookahead = enc.get_lookahead().unwrap_or(0).max(0) as u32;
            let pre_skip = (lookahead * 48000 / pcm_rate) as u16;
            for page in mux.headers(pre_skip, pcm_rate, &comments) {
                send_ctl(&ws_tx, Message::Binary(page.into())).await;
            }
        }
        ToApp::L16 { .. } => {}
    }

    // 3. Answer the SIP leg.
    if !server.answer(d, answer_sdp).await {
        finish_ws(&ws_tx, writer, &mut ws_rx, d, &mut to_app, Value::Null, true).await;
        return Ok(());
    }
    info!(call_id = d.call_id, codec = neg.codec.name(), to_app = cfg.ws_out_format.name(), "call answered");
    let mut transcript = Transcript::open(cfg.transcript_dir.as_deref(), &d.call_id).await;
    transcript
        .write(&json!({"type": "call", "call_id": d.call_id, "from": from_uri, "to": to_uri,
            "answered_at": now_rfc3339(), "languages": stt_languages}))
        .await;

    // 4. Media loop.
    let dur_of: fn(&[u8]) -> Option<u32> = match neg.codec {
        CodecKind::Opus => opus_packet_dur,
        _ => g711_dur,
    };
    let mut jb = JitterBuffer::new(clock, cfg.jitter_target_ms, cfg.jitter_max_ms, ts_step, dur_of);

    let mut remote = SocketAddr::new(neg.remote_ip, neg.remote_port);
    let mut direction = neg.direction;
    let mut latched = false;
    let ssrc: u32 = rand::random();
    let mut seq: u16 = rand::random();
    let mut ts: u32 = rand::random();
    let mut first_packet = true;

    let mut rx_buf = vec![0u8; 2048];
    let mut rtcp_buf = vec![0u8; 1500];
    let mut tx_pkt = Vec::with_capacity(1500);
    let mut enc_buf = Vec::with_capacity(1500);
    let mut frame = Vec::with_capacity(frame_samples);
    let mut in_pcm: Vec<i16> = Vec::new();
    let mut out_msgs: Vec<Vec<u8>> = Vec::new();
    let mut rx_budget: i64 = 0;
    let mut in_ssrc: Option<u32> = None;
    let mut dtmf_rx = DtmfRx::default();
    let mut last_rtp = Instant::now();
    let mut dropped_frames: u64 = 0;
    // The audio clock (docs/stt-protocol.md §2.1): RTP clock units of audio handed to the app.
    let mut sent_clock: u64 = 0;
    let audio_ms = |sent: u64| sent * 1000 / clock as u64;

    let mut play = Playout {
        buf: VecDeque::new(),
        max: (pcm_rate as u64 * cfg.max_playout_ms as u64 / 1000) as usize,
        enqueued: 0,
        consumed: 0,
        marks: VecDeque::new(),
    };
    let mut dtmf = DtmfTx { queue: VecDeque::new(), current: None, gap_ticks: 0 };

    let mut tick = tokio::time::interval(Duration::from_millis(cfg.ptime as u64));
    let rtp_timeout = (cfg.rtp_timeout_secs > 0).then(|| Duration::from_secs(cfg.rtp_timeout_secs));

    loop {
        tokio::select! {
            _ = d.cancel.cancelled() => break,

            msg = ws_rx.next() => match msg {
                Some(Ok(Message::Binary(data))) => {
                    in_pcm.clear();
                    from_app.feed(&data, &mut in_pcm);
                    play.push(&in_pcm);
                }
                Some(Ok(Message::Text(text))) => {
                    let mut ctx = RpcCtx {
                        d, play: &mut play, dtmf: &mut dtmf, neg: &neg, clock,
                        jb_stats: serde_json::to_value(&jb.stats).unwrap_or_default(),
                        stt: &mut stt, audio_ms: audio_ms(sent_clock),
                    };
                    for reply in handle_rpc(&text, &mut ctx) {
                        send_ctl(&ws_tx, reply).await;
                    }
                    for line in stt.lines.drain(..) {
                        transcript.write(&line).await;
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
                if pkt.pt != neg.pt && Some(pkt.pt) != neg.dtmf_pt {
                    continue; // comfort noise, stray streams
                }
                // Symmetric RTP: latch onto the real source; re-latch when the stream changes.
                if in_ssrc != Some(pkt.ssrc) {
                    in_ssrc = Some(pkt.ssrc);
                    latched = false;
                }
                if cfg.symmetric_rtp && !latched {
                    if src != remote {
                        debug!(call_id = d.call_id, %src, "latching RTP to source address");
                    }
                    remote = src;
                    latched = true;
                }
                last_rtp = Instant::now();

                if pkt.pt == neg.pt {
                    jb.push(pkt.ssrc, pkt.seq, pkt.ts, pkt.payload, last_rtp);
                } else if let Some(ev) = DtmfEvent::parse(pkt.payload) {
                    jb.push_skip(pkt.ssrc, pkt.seq, last_rtp);
                    for (event, dur) in dtmf_rx.on_packet(pkt.ts, ev) {
                        if let Some(digit) = dtmf_char(event) {
                            let ms = dur as u32 * 1000 / clock;
                            let at = audio_ms(sent_clock);
                            send_ctl(&ws_tx, ws::notification("call.dtmf",
                                json!({"digit": digit.to_string(), "duration_ms": ms, "source": "rfc4733", "audio_ms": at}))).await;
                            transcript.write(&json!({"type": "dtmf", "digit": digit.to_string(), "audio_ms": at})).await;
                        }
                    }
                }
            },

            r = async { rtp.rtcp.as_ref().unwrap().recv_from(&mut rtcp_buf).await }, if rtp.rtcp.is_some() => {
                let _ = r; // reports are not used
            },

            _ = tick.tick() => {
                let now = Instant::now();
                if let Some(t) = rtp_timeout
                    && receives(direction)
                    && last_rtp.elapsed() > t
                {
                    info!(call_id = d.call_id, "RTP timeout");
                    d.terminate(EndReason::RtpTimeout);
                    break;
                }

                // Caller → app: release what the jitter buffer has due for this tick.
                // Gaps (start-up buffering, underrun, hold) become silence so the app
                // receives a continuous, real-time stream.
                rx_budget += ts_step as i64;
                out_msgs.clear();
                while rx_budget > 0 {
                    let item = jb.pop(now, rx_budget as u32).unwrap_or(JbItem::Silence { dur: rx_budget as u32 });
                    rx_budget -= item.dur() as i64;
                    sent_clock += item.dur() as u64;
                    to_app.feed(item, &mut tc, clock, &mut out_msgs);
                }
                for m in out_msgs.drain(..) {
                    if ws_tx.try_send(Message::Binary(m.into())).is_err() {
                        dropped_frames += 1;
                        if dropped_frames.is_power_of_two() {
                            warn!(call_id = d.call_id, dropped_frames, "WebSocket backpressure, dropping audio");
                        }
                    }
                }

                // App → caller.
                if sends(direction) {
                    send_tick(&rtp, remote, &neg, &mut dtmf, &mut play, &mut tc, &mut frame, &mut enc_buf,
                        &mut tx_pkt, ssrc, &mut seq, ts, ts_step, &mut first_packet, cfg.ptime, frame_samples).await;
                } else {
                    // On hold: keep the queue moving so marks still complete.
                    play.take(frame_samples, &mut frame);
                }
                ts = ts.wrapping_add(ts_step);

                for name in play.due_marks() {
                    send_ctl(&ws_tx, ws::notification("call.mark",
                        json!({"name": name, "cleared": false, "audio_ms": audio_ms(sent_clock)}))).await;
                }
            },

            Some(ctl) = media_rx.recv() => match ctl {
                MediaCtl::Remote { addr, direction: dir } => {
                    if addr != remote || dir != direction {
                        info!(call_id = d.call_id, %addr, direction = dir, "remote media updated");
                    }
                    remote = addr;
                    direction = dir;
                    latched = false;
                    last_rtp = Instant::now();
                }
                MediaCtl::Dtmf { digit, duration_ms } => {
                    let at = audio_ms(sent_clock);
                    send_ctl(&ws_tx, ws::notification("call.dtmf",
                        json!({"digit": digit.to_string(), "duration_ms": duration_ms, "source": "sip_info", "audio_ms": at}))).await;
                    transcript.write(&json!({"type": "dtmf", "digit": digit.to_string(), "audio_ms": at})).await;
                }
            },
        }
    }

    // Media is over: hang up the SIP leg right away, then give the STT engine its grace period.
    server.release_answered(d);
    let reason = d.end_reason().map(|r| r.as_str()).unwrap_or_else(|| "unknown".into());
    let end_ms = audio_ms(sent_clock);
    let mut ws_alive = d.end_reason() != Some(EndReason::WsClosed);

    if ws_alive && stt.session.state == stt::State::Active && cfg.stt_finalize_ms > 0 {
        send_ctl(&ws_tx, ws::request(stt::FINALIZE_ID, "stt.finalize", json!({"reason": reason, "audio_ms": end_ms}))).await;
        let deadline = tokio::time::sleep(Duration::from_millis(cfg.stt_finalize_ms));
        tokio::pin!(deadline);
        while !stt.finalize_done {
            tokio::select! {
                _ = &mut deadline => {
                    info!(call_id = d.call_id, "stt.finalize: no answer within {} ms", cfg.stt_finalize_ms);
                    break;
                }
                msg = ws_rx.next() => match msg {
                    Some(Ok(Message::Text(text))) => {
                        let mut ctx = RpcCtx {
                            d, play: &mut play, dtmf: &mut dtmf, neg: &neg, clock,
                            jb_stats: serde_json::to_value(&jb.stats).unwrap_or_default(),
                            stt: &mut stt, audio_ms: end_ms,
                        };
                        for reply in handle_rpc(&text, &mut ctx) {
                            send_ctl(&ws_tx, reply).await;
                        }
                        for line in stt.lines.drain(..) {
                            transcript.write(&line).await;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        ws_alive = false;
                        break;
                    }
                    Some(Ok(_)) => {} // audio after hangup is dropped
                },
            }
        }
    }
    for line in stt.session.abandon_all() {
        transcript.write(&line).await;
    }
    transcript.write(&json!({"type": "end", "reason": reason, "audio_ms": end_ms, "ended_at": now_rfc3339()})).await;

    let stats = serde_json::to_value(&jb.stats).unwrap_or_default();
    info!(call_id = d.call_id, rtp = %stats, "media stats");
    finish_ws(&ws_tx, writer, &mut ws_rx, d, &mut to_app, stats, ws_alive).await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn send_tick(
    rtp: &RtpSocket,
    remote: SocketAddr,
    neg: &Negotiated,
    dtmf: &mut DtmfTx,
    play: &mut Playout,
    tc: &mut Transcoder,
    frame: &mut Vec<i16>,
    enc_buf: &mut Vec<u8>,
    tx_pkt: &mut Vec<u8>,
    ssrc: u32,
    seq: &mut u16,
    ts: u32,
    ts_step: u32,
    first_packet: &mut bool,
    ptime: u32,
    frame_samples: usize,
) {
    let mut marker = *first_packet;
    *first_packet = false;

    if dtmf.current.is_none() && dtmf.gap_ticks == 0
        && let Some((event, total)) = dtmf.queue.pop_front()
    {
        dtmf.current = Some(DtmfCur { event, start_ts: ts, elapsed: 0, total, first: true });
    }
    if let Some(cur) = dtmf.current.as_mut() {
        // Audio playout is paused while an event is being sent.
        cur.elapsed = (cur.elapsed + ts_step).min(cur.total);
        let end = cur.elapsed >= cur.total;
        let ev = DtmfEvent { event: cur.event, end, volume: 10, duration: cur.elapsed.min(0xFFFF) as u16 };
        let pt = neg.dtmf_pt.unwrap_or(neg.pt);
        let bytes = ev.to_bytes();
        // The end packet is sent three times (RFC 4733 2.5.1.4).
        for _ in 0..if end { 3 } else { 1 } {
            let p = RtpPacket { marker: cur.first, pt, seq: *seq, ts: cur.start_ts, ssrc, payload: &bytes };
            p.write(tx_pkt);
            let _ = rtp.socket.send_to(tx_pkt, remote).await;
            *seq = seq.wrapping_add(1);
            cur.first = false;
        }
        if end {
            dtmf.current = None;
            dtmf.gap_ticks = (60 / ptime).max(1);
        }
        return;
    }
    if dtmf.gap_ticks > 0 {
        dtmf.gap_ticks -= 1;
        marker = true; // new talkspurt after the event
    }
    play.take(frame_samples, frame);
    if let Err(e) = tc.encode(frame, enc_buf) {
        debug!("encode error: {e}");
        return;
    }
    let p = RtpPacket { marker, pt: neg.pt, seq: *seq, ts, ssrc, payload: enc_buf };
    p.write(tx_pkt);
    let _ = rtp.socket.send_to(tx_pkt, remote).await;
    *seq = seq.wrapping_add(1);
}

async fn finish_ws(
    ws_tx: &mpsc::Sender<Message>,
    writer: tokio::task::JoinHandle<()>,
    ws_rx: &mut ws::WsStream,
    d: &Dialog,
    to_app: &mut ToApp,
    rtp_stats: Value,
    ws_alive: bool,
) {
    let reason = d.end_reason().map(|r| r.as_str()).unwrap_or_else(|| "unknown".into());
    if ws_alive && d.end_reason() != Some(EndReason::WsClosed) {
        // Close the Ogg stream properly (EOS page) before the call.end notification.
        if let Some(page) = to_app.finish() {
            send_ctl(ws_tx, Message::Binary(page.into())).await;
        }
        send_ctl(ws_tx, ws::notification("call.end", json!({"call_id": d.call_id, "reason": reason, "rtp": rtp_stats}))).await;
        send_ctl(ws_tx, Message::Close(None)).await;
        // Closing handshake: keep reading until the app's Close arrives. Dropping the TCP
        // connection while the app is still sending makes the kernel answer with RST, and the
        // app would then lose call.end and the final Ogg page still sitting in its receive buffer.
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(Ok(m)) = ws_rx.next().await {
                if matches!(m, Message::Close(_)) {
                    break;
                }
            }
        })
        .await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
}

/// STT state owned by the call task.
struct SttState {
    session: stt::Session,
    /// Transcript lines produced while handling a message; written by the caller (async).
    lines: Vec<Value>,
    finalize_done: bool,
}

struct RpcCtx<'a> {
    d: &'a Dialog,
    play: &'a mut Playout,
    dtmf: &'a mut DtmfTx,
    neg: &'a Negotiated,
    clock: u32,
    jb_stats: Value,
    stt: &'a mut SttState,
    audio_ms: u64,
}

/// Flushes queued playout; returns (samples, cleared marks) and queues cleared-mark notifications.
fn clear_playout(play: &mut Playout, audio_ms: u64, out: &mut Vec<Message>) -> (usize, Vec<String>) {
    let samples = play.clear();
    let cleared: Vec<String> = play.marks.drain(..).map(|(_, n)| n).collect();
    for name in &cleared {
        out.push(ws::notification("call.mark", json!({"name": name, "cleared": true, "audio_ms": audio_ms})));
    }
    (samples, cleared)
}

fn handle_response(id: &Value, result: Result<Value, Value>, ctx: &mut RpcCtx) {
    let st = &mut ctx.stt;
    match (id.as_str(), result) {
        (Some(stt::BEGIN_ID), Ok(r)) => {
            if st.session.state != stt::State::Failed {
                st.session.state = stt::State::Active;
            }
            let engine = r.get("engine").cloned().unwrap_or_default();
            info!(call_id = ctx.d.call_id, %engine, "STT session active");
            let mut line = json!({"type": "stt.begin"});
            for k in ["engine", "languages", "features"] {
                if let Some(v) = r.get(k) {
                    line[k] = v.clone();
                }
            }
            st.lines.push(line);
        }
        (Some(stt::BEGIN_ID), Err(e)) => {
            if st.session.state == stt::State::Pending {
                st.session.state = stt::State::Inactive;
            }
            debug!(call_id = ctx.d.call_id, error = %e, "app declined stt.begin");
        }
        (Some(stt::FINALIZE_ID), r) => {
            if let Err(e) = r {
                debug!(call_id = ctx.d.call_id, error = %e, "stt.finalize error");
            }
            st.finalize_done = true;
        }
        _ => {}
    }
}

fn handle_rpc(text: &str, ctx: &mut RpcCtx) -> Vec<Message> {
    let (id, method, params) = match ws::parse_rpc(text) {
        Incoming::Call { id, method, params } => (id, method, params),
        Incoming::Response { id, result } => {
            handle_response(&id, result, ctx);
            return vec![];
        }
        Incoming::Invalid { id, code, message } => return vec![ws::error(id, code, &message)],
    };
    let mut out = Vec::new();

    // Speech-to-text results (docs/stt-protocol.md §4).
    if let Some(r) = ctx.stt.session.handle(&method, &params, ctx.audio_ms) {
        match r {
            Ok((result, effects)) => {
                for fx in effects {
                    match fx {
                        Effect::BargeIn { utterance_id } => {
                            if !ctx.play.buf.is_empty() {
                                let mut notes = Vec::new();
                                let (samples, marks) = clear_playout(ctx.play, ctx.audio_ms, &mut notes);
                                info!(call_id = ctx.d.call_id, utterance_id, samples, "barge-in");
                                out.extend(notes);
                                out.push(ws::notification("call.barge_in", json!({
                                    "utterance_id": utterance_id, "audio_ms": ctx.audio_ms,
                                    "cleared_samples": samples, "cleared_marks": marks,
                                })));
                            }
                        }
                        Effect::Record(line) => ctx.stt.lines.push(line),
                        Effect::Final { utterance_id, text, end_of_turn } => {
                            info!(call_id = ctx.d.call_id, utterance_id, end_of_turn, text = %text, "transcript");
                        }
                        Effect::EngineError { code, message, fatal } => {
                            warn!(call_id = ctx.d.call_id, code, message, fatal, "STT engine error");
                        }
                    }
                }
                if let Some(id) = id {
                    out.insert(0, ws::result(id, result));
                }
            }
            Err(e) => {
                debug!(call_id = ctx.d.call_id, method, code = e.code, message = e.message, "rejected STT message");
                if let Some(id) = id {
                    out.push(ws::error_data(id, e.code, &e.message, e.data));
                }
            }
        }
        return out;
    }

    let outcome: Result<Value, (i64, String)> = match method.as_str() {
        "call.hangup" => {
            let reason = params.get("reason").and_then(Value::as_str).unwrap_or("").to_string();
            ctx.d.terminate(EndReason::AppHangup(reason));
            Ok(json!(true))
        }
        "call.clear" => {
            // Pending marks are reported back as cleared, so the app can track what was cut.
            let (samples, cleared) = clear_playout(ctx.play, ctx.audio_ms, &mut out);
            Ok(json!({"cleared_samples": samples, "cleared_marks": cleared}))
        }
        "call.mark" => match params.get("name").and_then(Value::as_str) {
            Some(name) => {
                ctx.play.marks.push_back((ctx.play.enqueued, name.to_string()));
                Ok(json!(true))
            }
            None => Err((ws::INVALID_PARAMS, "params.name (string) required".into())),
        },
        "call.dtmf" => {
            let digits = params.get("digits").and_then(Value::as_str).unwrap_or("");
            let ms = params.get("duration_ms").and_then(Value::as_u64).unwrap_or(100).clamp(40, 2000) as u32;
            if ctx.neg.dtmf_pt.is_none() {
                Err((ws::SERVER_ERROR, "telephone-event not negotiated on this call".into()))
            } else if digits.is_empty() || !digits.chars().all(|c| dtmf_event(c).is_some()) {
                Err((ws::INVALID_PARAMS, "params.digits must be non-empty and contain 0-9*#A-D".into()))
            } else {
                for c in digits.chars() {
                    ctx.dtmf.queue.push_back((dtmf_event(c).unwrap(), ms * ctx.clock / 1000));
                }
                Ok(json!({"queued": digits.len()}))
            }
        }
        "call.status" => Ok(json!({
            "call_id": ctx.d.call_id,
            "codec": ctx.neg.codec.name(),
            "audio_ms": ctx.audio_ms,
            "buffered_samples": ctx.play.buf.len(),
            "pending_marks": ctx.play.marks.len(),
            "stt": match ctx.stt.session.state {
                stt::State::Pending => "pending", stt::State::Active => "active",
                stt::State::Inactive => "inactive", stt::State::Failed => "failed",
            },
            "rtp": ctx.jb_stats,
        })),
        "call.transcript" => Ok(ctx.stt.session.transcript()),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event: u8, end: bool, duration: u16) -> DtmfEvent {
        DtmfEvent { event, end, volume: 10, duration }
    }

    #[test]
    fn dtmf_rx_reports_once_and_survives_lost_end() {
        let mut r = DtmfRx::default();
        assert!(r.on_packet(100, ev(5, false, 160)).is_empty());
        assert_eq!(r.on_packet(100, ev(5, true, 800)), [(5, 800)]);
        assert!(r.on_packet(100, ev(5, true, 800)).is_empty()); // retransmitted end
        assert!(r.on_packet(2000, ev(7, false, 160)).is_empty());
        // '7' never got an end packet; reported when '9' starts.
        assert_eq!(r.on_packet(4000, ev(9, false, 160)), [(7, 160)]);
    }
}

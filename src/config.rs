use std::net::{IpAddr, SocketAddr};

use anyhow::{bail, Result};
use clap::{Parser, ValueEnum};

use crate::codec::CodecKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Endian {
    /// Little-endian samples (what most ASR/TTS APIs call "linear16").
    Le,
    /// Network byte order, as RFC 3551 L16 is defined.
    Be,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WsFormat {
    /// Raw 16-bit linear PCM, mono.
    L16,
    /// Ogg-encapsulated Opus (RFC 7845), one Ogg page per binary message.
    OggOpus,
}

impl WsFormat {
    pub fn name(self) -> &'static str {
        match self {
            WsFormat::L16 => "L16",
            WsFormat::OggOpus => "ogg/opus",
        }
    }
}

/// SIP/RTP media server that bridges each call to a WebSocket carrying
/// JSON-RPC 2.0 control messages and binary L16 audio frames.
#[derive(Debug, Clone, Parser)]
#[command(version)]
pub struct Config {
    /// SIP UDP listen address.
    #[arg(long, env = "SIPWS_SIP_BIND", default_value = "0.0.0.0:5060")]
    pub sip_bind: SocketAddr,

    /// IP advertised in SDP and Contact. Auto-detected when omitted.
    #[arg(long, env = "SIPWS_PUBLIC_IP")]
    pub public_ip: Option<IpAddr>,

    /// IP to bind RTP sockets on.
    #[arg(long, env = "SIPWS_RTP_BIND_IP", default_value = "0.0.0.0")]
    pub rtp_bind_ip: IpAddr,

    #[arg(long, env = "SIPWS_RTP_PORT_MIN", default_value_t = 20000)]
    pub rtp_port_min: u16,

    #[arg(long, env = "SIPWS_RTP_PORT_MAX", default_value_t = 30000)]
    pub rtp_port_max: u16,

    /// Codec preference order for the SIP leg (opus, pcmu, pcma).
    #[arg(long, env = "SIPWS_CODECS", value_delimiter = ',', default_value = "opus,pcmu,pcma")]
    pub codecs: Vec<String>,

    /// RTP packetisation time in ms (10, 20, 40 or 60).
    #[arg(long, env = "SIPWS_PTIME", default_value_t = 20)]
    pub ptime: u32,

    /// WebSocket URL dialled for every call. Placeholders: {call_id} {from_user} {to_user}.
    #[arg(long, env = "SIPWS_WS_URL")]
    pub ws_url: String,

    /// Extra header(s) for the WebSocket handshake, "Name: value". Repeatable.
    #[arg(long = "ws-header", env = "SIPWS_WS_HEADERS", value_delimiter = '\n')]
    pub ws_headers: Vec<String>,

    /// L16 sample rate on the WebSocket (8000, 16000, 24000 or 48000).
    #[arg(long, env = "SIPWS_WS_SAMPLE_RATE", default_value_t = 16000)]
    pub ws_sample_rate: u32,

    /// Duration of each binary frame sent to the WebSocket, in ms.
    /// (For ogg-opus: audio duration collected into each Ogg page.)
    #[arg(long, env = "SIPWS_WS_FRAME_MS", default_value_t = 20)]
    pub ws_frame_ms: u32,

    /// Audio format sent to the WebSocket app.
    #[arg(long, env = "SIPWS_WS_OUT_FORMAT", value_enum, default_value_t = WsFormat::L16)]
    pub ws_out_format: WsFormat,

    /// Audio format the WebSocket app sends back for playback.
    #[arg(long, env = "SIPWS_WS_IN_FORMAT", value_enum, default_value_t = WsFormat::L16)]
    pub ws_in_format: WsFormat,

    /// In ogg-opus output, copy Opus packets from RTP straight into Ogg when the call uses Opus
    /// (no decode/re-encode). With false, or for G.711 calls, audio is (re)encoded.
    #[arg(long, env = "SIPWS_OGG_PASSTHROUGH", default_value_t = true, action = clap::ArgAction::Set)]
    pub ogg_passthrough: bool,

    /// Opus bitrate (bit/s) when sipws encodes the ogg-opus stream itself.
    #[arg(long, env = "SIPWS_OGG_BITRATE", default_value_t = 32000)]
    pub ogg_bitrate: i32,

    /// Jitter buffer target delay, ms. Playout starts once this much audio is queued.
    #[arg(long, env = "SIPWS_JITTER_TARGET_MS", default_value_t = 60)]
    pub jitter_target_ms: u32,

    /// Jitter buffer hard limit, ms; older audio is discarded beyond it.
    #[arg(long, env = "SIPWS_JITTER_MAX_MS", default_value_t = 500)]
    pub jitter_max_ms: u32,

    /// Byte order of L16 samples on the WebSocket.
    #[arg(long, env = "SIPWS_WS_ENDIAN", value_enum, default_value_t = Endian::Le)]
    pub ws_endian: Endian,

    #[arg(long, env = "SIPWS_WS_CONNECT_TIMEOUT_MS", default_value_t = 5000)]
    pub ws_connect_timeout_ms: u64,

    /// Maximum audio buffered for playback towards the caller, in ms; older audio is dropped.
    #[arg(long, env = "SIPWS_MAX_PLAYOUT_MS", default_value_t = 120_000)]
    pub max_playout_ms: u32,

    /// Hang up when no RTP is received for this many seconds (0 = never).
    #[arg(long, env = "SIPWS_RTP_TIMEOUT_SECS", default_value_t = 60)]
    pub rtp_timeout_secs: u64,

    /// Send RTP to the address packets actually arrive from (NAT traversal).
    #[arg(long, env = "SIPWS_SYMMETRIC_RTP", default_value_t = true, action = clap::ArgAction::Set)]
    pub symmetric_rtp: bool,

    /// SIP headers forwarded to the app in call.start (prefix match, case-insensitive).
    #[arg(long, env = "SIPWS_FORWARD_HEADERS", value_delimiter = ',', default_value = "X-,P-Asserted-Identity,Diversion")]
    pub forward_headers: Vec<String>,

    /// Maximum concurrent calls; further INVITEs get 486 Busy Here.
    #[arg(long, env = "SIPWS_MAX_CALLS", default_value_t = 500)]
    pub max_calls: usize,
}

impl Config {
    pub fn validate(&mut self) -> Result<Vec<CodecKind>> {
        if ![8000, 16000, 24000, 48000].contains(&self.ws_sample_rate) {
            bail!("--ws-sample-rate must be 8000, 16000, 24000 or 48000");
        }
        if ![10, 20, 40, 60].contains(&self.ptime) {
            bail!("--ptime must be 10, 20, 40 or 60");
        }
        if self.ws_frame_ms == 0 || self.ws_frame_ms > 1000 {
            bail!("--ws-frame-ms must be 1..=1000");
        }
        if self.jitter_target_ms > 1000 || self.jitter_max_ms > 5000 {
            bail!("jitter buffer limits too large (target <= 1000 ms, max <= 5000 ms)");
        }
        if !(6000..=510_000).contains(&self.ogg_bitrate) {
            bail!("--ogg-bitrate must be 6000..=510000");
        }
        if self.rtp_port_min >= self.rtp_port_max {
            bail!("empty RTP port range");
        }
        let mut codecs = Vec::new();
        for c in &self.codecs {
            match CodecKind::from_name(c.trim()) {
                Some(k) if !codecs.contains(&k) => codecs.push(k),
                Some(_) => {}
                None => bail!("unknown codec {c:?}"),
            }
        }
        if codecs.is_empty() {
            bail!("no codecs enabled");
        }
        if self.public_ip.is_none() {
            self.public_ip = Some(detect_local_ip()?);
        }
        Ok(codecs)
    }

    pub fn public_ip(&self) -> IpAddr {
        self.public_ip.expect("validated")
    }
}

fn detect_local_ip() -> Result<IpAddr> {
    // No packets are sent; connect() just selects the outbound interface.
    let s = std::net::UdpSocket::bind("0.0.0.0:0")?;
    s.connect("192.0.2.1:9")?;
    Ok(s.local_addr()?.ip())
}

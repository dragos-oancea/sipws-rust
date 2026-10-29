//! sipws: SIP/RTP media server bridging calls to WebSockets (JSON-RPC + L16 or Ogg/Opus).

pub mod call;
pub mod codec;
pub mod config;
pub mod jitter;
pub mod ogg;
pub mod rtp;
pub mod sip;
pub mod stt;
pub mod transcript;
pub mod ws;

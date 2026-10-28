//! RTP payload codecs <-> L16 PCM at the WebSocket sample rate.

pub mod g711;
pub mod resample;

use anyhow::{bail, Context, Result};
use resample::Resampler;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecKind {
    Opus,
    Pcmu,
    Pcma,
}

impl CodecKind {
    pub fn from_name(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "opus" => Some(Self::Opus),
            "pcmu" | "ulaw" | "g711u" => Some(Self::Pcmu),
            "pcma" | "alaw" | "g711a" => Some(Self::Pcma),
            _ => None,
        }
    }

    pub fn from_rtpmap(name: &str, rate: u32) -> Option<Self> {
        match (name.to_ascii_lowercase().as_str(), rate) {
            ("opus", 48000) => Some(Self::Opus),
            ("pcmu", 8000) => Some(Self::Pcmu),
            ("pcma", 8000) => Some(Self::Pcma),
            _ => None,
        }
    }

    pub fn from_static_pt(pt: u8) -> Option<Self> {
        match pt {
            0 => Some(Self::Pcmu),
            8 => Some(Self::Pcma),
            _ => None,
        }
    }

    /// RTP clock rate (Opus is always signalled at 48 kHz, RFC 7587).
    pub fn clock_rate(self) -> u32 {
        match self {
            Self::Opus => 48000,
            _ => 8000,
        }
    }

    pub fn rtpmap(self) -> (&'static str, u32, u32) {
        match self {
            Self::Opus => ("opus", 48000, 2),
            Self::Pcmu => ("PCMU", 8000, 1),
            Self::Pcma => ("PCMA", 8000, 1),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Opus => "opus",
            Self::Pcmu => "PCMU",
            Self::Pcma => "PCMA",
        }
    }
}

enum Inner {
    Opus { dec: opus::Decoder, enc: opus::Encoder },
    G711 { ulaw: bool, up: Resampler, down: Resampler },
}

/// Converts between RTP payloads and mono L16 at `pcm_rate`.
pub struct Transcoder {
    inner: Inner,
    pcm_rate: u32,
    ptime_ms: u32,
    scratch: Vec<i16>,
}

impl Transcoder {
    pub fn new(kind: CodecKind, pcm_rate: u32, ptime_ms: u32) -> Result<Self> {
        let inner = match kind {
            CodecKind::Opus => {
                if ![8000, 12000, 16000, 24000, 48000].contains(&pcm_rate) {
                    bail!("opus cannot run at {pcm_rate} Hz");
                }
                // libopus decodes/encodes natively at the WS rate: no resampling.
                let dec = opus::Decoder::new(pcm_rate, opus::Channels::Mono).context("opus decoder")?;
                let mut enc = opus::Encoder::new(pcm_rate, opus::Channels::Mono, opus::Application::Voip)
                    .context("opus encoder")?;
                enc.set_inband_fec(true).ok();
                enc.set_packet_loss_perc(5).ok();
                enc.set_bitrate(opus::Bitrate::Bits(32000)).ok();
                Inner::Opus { dec, enc }
            }
            CodecKind::Pcmu | CodecKind::Pcma => Inner::G711 {
                ulaw: kind == CodecKind::Pcmu,
                up: Resampler::new(8000, pcm_rate),
                down: Resampler::new(pcm_rate, 8000),
            },
        };
        Ok(Transcoder { inner, pcm_rate, ptime_ms, scratch: Vec::new() })
    }

    /// Samples of PCM (at the WS rate) per RTP packet.
    pub fn frame_samples(&self) -> usize {
        (self.pcm_rate * self.ptime_ms / 1000) as usize
    }

    /// Decodes one RTP payload, appending PCM at the WS rate to `out`.
    pub fn decode(&mut self, payload: &[u8], out: &mut Vec<i16>) -> Result<()> {
        match &mut self.inner {
            Inner::Opus { dec, .. } => {
                let max = (self.pcm_rate as usize) * 120 / 1000;
                self.scratch.resize(max, 0);
                let n = dec.decode(payload, &mut self.scratch, false)?;
                out.extend_from_slice(&self.scratch[..n]);
            }
            Inner::G711 { ulaw, up, .. } => {
                self.scratch.clear();
                if *ulaw {
                    self.scratch.extend(payload.iter().map(|&b| g711::ulaw_to_linear(b)));
                } else {
                    self.scratch.extend(payload.iter().map(|&b| g711::alaw_to_linear(b)));
                }
                up.process(&self.scratch, out);
            }
        }
        Ok(())
    }

    /// Conceals one lost packet (Opus PLC, silence for G.711).
    pub fn conceal(&mut self, out: &mut Vec<i16>) {
        let n = self.frame_samples();
        match &mut self.inner {
            Inner::Opus { dec, .. } => {
                self.scratch.resize(n, 0);
                match dec.decode(&[], &mut self.scratch, false) {
                    Ok(got) => out.extend_from_slice(&self.scratch[..got]),
                    Err(_) => out.extend(std::iter::repeat_n(0, n)),
                }
            }
            Inner::G711 { .. } => out.extend(std::iter::repeat_n(0, n)),
        }
    }

    /// Encodes exactly `frame_samples()` PCM samples into one RTP payload.
    pub fn encode(&mut self, pcm: &[i16], out: &mut Vec<u8>) -> Result<()> {
        out.clear();
        match &mut self.inner {
            Inner::Opus { enc, .. } => {
                out.resize(1500, 0);
                let n = enc.encode(pcm, out)?;
                out.truncate(n);
            }
            Inner::G711 { ulaw, down, .. } => {
                self.scratch.clear();
                down.process(pcm, &mut self.scratch);
                if *ulaw {
                    out.extend(self.scratch.iter().map(|&s| g711::linear_to_ulaw(s)));
                } else {
                    out.extend(self.scratch.iter().map(|&s| g711::linear_to_alaw(s)));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opus_roundtrip_frame_sizes() {
        for rate in [8000, 16000, 48000] {
            let mut t = Transcoder::new(CodecKind::Opus, rate, 20).unwrap();
            let pcm: Vec<i16> = (0..t.frame_samples())
                .map(|i| ((i as f64 * 0.1).sin() * 8000.0) as i16)
                .collect();
            let mut pkt = Vec::new();
            t.encode(&pcm, &mut pkt).unwrap();
            assert!(!pkt.is_empty() && pkt.len() < 400);
            let mut out = Vec::new();
            t.decode(&pkt, &mut out).unwrap();
            assert_eq!(out.len(), t.frame_samples());
            t.conceal(&mut out);
            assert_eq!(out.len(), 2 * t.frame_samples());
        }
    }

    #[test]
    fn g711_frame_sizes() {
        let mut t = Transcoder::new(CodecKind::Pcmu, 16000, 20).unwrap();
        let pcm = vec![1000i16; 320];
        let mut pkt = Vec::new();
        t.encode(&pcm, &mut pkt).unwrap();
        assert_eq!(pkt.len(), 160);
        let mut out = Vec::new();
        t.decode(&pkt, &mut out).unwrap();
        assert_eq!(out.len(), 320);
    }
}

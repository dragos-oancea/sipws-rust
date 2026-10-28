//! SDP offer parsing and answer generation (RFC 4566 / RFC 3264), audio only.

use std::fmt::Write as _;
use std::net::IpAddr;

use crate::codec::CodecKind;

#[derive(Debug, Clone)]
pub struct MediaDesc {
    pub media: String,
    pub port: u16,
    pub proto: String,
    pub fmts: Vec<String>,
    pub conn: Option<IpAddr>,
    pub rtpmaps: Vec<RtpMap>,
    pub fmtps: Vec<(u8, String)>,
    pub direction: Option<String>,
    pub ptime: Option<u32>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RtpMap {
    pub pt: u8,
    pub name: String,
    pub rate: u32,
    pub channels: u32,
}

#[derive(Debug, Clone)]
pub struct Sdp {
    pub conn: Option<IpAddr>,
    pub media: Vec<MediaDesc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Negotiated {
    pub codec: CodecKind,
    pub pt: u8,
    /// RFC 4733 telephone-event payload type at the codec's clock rate, if offered.
    pub dtmf_pt: Option<u8>,
    pub remote_ip: IpAddr,
    pub remote_port: u16,
    /// Index of the audio m-line we accepted.
    pub mline: usize,
    /// Our direction attribute in the answer.
    pub direction: &'static str,
}

#[derive(Debug, thiserror::Error)]
pub enum SdpError {
    #[error("malformed SDP: {0}")]
    Malformed(String),
    #[error("no acceptable audio stream/codec")]
    NotAcceptable,
}

impl Sdp {
    pub fn parse(body: &[u8]) -> Result<Sdp, SdpError> {
        let text = std::str::from_utf8(body).map_err(|_| SdpError::Malformed("utf8".into()))?;
        let mut sdp = Sdp { conn: None, media: Vec::new() };
        for raw in text.lines() {
            let line = raw.trim_end_matches('\r');
            let Some((k, v)) = line.split_once('=') else { continue };
            match k {
                "c" => {
                    let ip = parse_conn(v);
                    match sdp.media.last_mut() {
                        Some(m) => m.conn = ip,
                        None => sdp.conn = ip,
                    }
                }
                "m" => {
                    let mut parts = v.split_whitespace();
                    let media = parts.next().unwrap_or_default().to_string();
                    let port = parts
                        .next()
                        .and_then(|p| p.split('/').next())
                        .and_then(|p| p.parse().ok())
                        .ok_or_else(|| SdpError::Malformed(line.into()))?;
                    let proto = parts.next().unwrap_or_default().to_string();
                    let fmts = parts.map(str::to_string).collect();
                    sdp.media.push(MediaDesc {
                        media,
                        port,
                        proto,
                        fmts,
                        conn: None,
                        rtpmaps: Vec::new(),
                        fmtps: Vec::new(),
                        direction: None,
                        ptime: None,
                    });
                }
                "a" => {
                    let Some(m) = sdp.media.last_mut() else { continue };
                    let (name, val) = v.split_once(':').unwrap_or((v, ""));
                    match name {
                        "rtpmap" => {
                            if let Some(rm) = parse_rtpmap(val) {
                                m.rtpmaps.push(rm);
                            }
                        }
                        "fmtp" => {
                            if let Some((pt, rest)) = val.split_once(' ')
                                && let Ok(pt) = pt.parse() {
                                    m.fmtps.push((pt, rest.trim().to_string()));
                                }
                        }
                        "ptime" => m.ptime = val.trim().parse().ok(),
                        "sendrecv" | "sendonly" | "recvonly" | "inactive" => {
                            m.direction = Some(name.to_string())
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        Ok(sdp)
    }

    /// Picks the first audio m-line with a codec from `prefs` (in our preference order).
    pub fn negotiate(&self, prefs: &[CodecKind]) -> Result<Negotiated, SdpError> {
        for (idx, m) in self.media.iter().enumerate() {
            if m.media != "audio" || m.port == 0 || m.proto != "RTP/AVP" {
                continue;
            }
            let ip = m.conn.or(self.conn).ok_or_else(|| SdpError::Malformed("no c=".into()))?;
            let offered: Vec<(u8, CodecKind, u32)> = m
                .fmts
                .iter()
                .filter_map(|f| f.parse::<u8>().ok())
                .filter_map(|pt| {
                    let map = m.rtpmaps.iter().find(|r| r.pt == pt);
                    let kind = match map {
                        Some(r) => CodecKind::from_rtpmap(&r.name, r.rate)?,
                        None => CodecKind::from_static_pt(pt)?,
                    };
                    Some((pt, kind, kind.clock_rate()))
                })
                .collect();
            for want in prefs {
                if let Some((pt, kind, clock)) = offered.iter().find(|(_, k, _)| k == want) {
                    let dtmf_pt = m
                        .rtpmaps
                        .iter()
                        .find(|r| r.name.eq_ignore_ascii_case("telephone-event") && r.rate == *clock)
                        .map(|r| r.pt);
                    let direction = match m.direction.as_deref() {
                        Some("sendonly") => "recvonly",
                        Some("recvonly") => "sendonly",
                        Some("inactive") => "inactive",
                        _ => "sendrecv",
                    };
                    return Ok(Negotiated {
                        codec: *kind,
                        pt: *pt,
                        dtmf_pt,
                        remote_ip: ip,
                        remote_port: m.port,
                        mline: idx,
                        direction,
                    });
                }
            }
        }
        Err(SdpError::NotAcceptable)
    }
}

fn parse_conn(v: &str) -> Option<IpAddr> {
    // c=IN IP4 1.2.3.4[/ttl]
    let addr = v.split_whitespace().nth(2)?;
    addr.split('/').next()?.parse().ok()
}

fn parse_rtpmap(v: &str) -> Option<RtpMap> {
    let (pt, enc) = v.split_once(' ')?;
    let mut parts = enc.trim().split('/');
    Some(RtpMap {
        pt: pt.parse().ok()?,
        name: parts.next()?.to_string(),
        rate: parts.next()?.parse().ok()?,
        channels: parts.next().and_then(|c| c.parse().ok()).unwrap_or(1),
    })
}

/// Builds the SDP answer mirroring the offer's m-line count (RFC 3264 6).
pub fn build_answer(
    offer: &Sdp,
    neg: &Negotiated,
    local_ip: IpAddr,
    local_port: u16,
    ptime: u32,
    session_id: u64,
    version: u64,
) -> String {
    let ipver = if local_ip.is_ipv4() { "IP4" } else { "IP6" };
    let mut s = String::new();
    let _ = write!(
        s,
        "v=0\r\no=sipws {session_id} {version} IN {ipver} {local_ip}\r\ns=sipws\r\nc=IN {ipver} {local_ip}\r\nt=0 0\r\n"
    );
    for (idx, m) in offer.media.iter().enumerate() {
        if idx != neg.mline {
            let _ = write!(s, "m={} 0 {} {}\r\n", m.media, m.proto, m.fmts.join(" "));
            continue;
        }
        let mut fmts = neg.pt.to_string();
        if let Some(d) = neg.dtmf_pt {
            let _ = write!(fmts, " {d}");
        }
        let _ = write!(s, "m=audio {local_port} RTP/AVP {fmts}\r\n");
        let (name, rate, ch) = neg.codec.rtpmap();
        if ch > 1 {
            let _ = write!(s, "a=rtpmap:{} {name}/{rate}/{ch}\r\n", neg.pt);
        } else {
            let _ = write!(s, "a=rtpmap:{} {name}/{rate}\r\n", neg.pt);
        }
        if neg.codec == CodecKind::Opus {
            let _ = write!(s, "a=fmtp:{} useinbandfec=1;stereo=0;sprop-stereo=0\r\n", neg.pt);
        }
        if let Some(d) = neg.dtmf_pt {
            let _ = write!(s, "a=rtpmap:{d} telephone-event/{rate}\r\na=fmtp:{d} 0-15\r\n");
        }
        let _ = write!(s, "a=ptime:{ptime}\r\na={}\r\n", neg.direction);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFER: &str = "v=0\r\no=- 1 1 IN IP4 192.0.2.10\r\ns=-\r\nc=IN IP4 192.0.2.10\r\nt=0 0\r\n\
m=audio 40000 RTP/AVP 111 0 8 101 102\r\n\
a=rtpmap:111 opus/48000/2\r\na=fmtp:111 useinbandfec=1\r\n\
a=rtpmap:101 telephone-event/48000\r\na=rtpmap:102 telephone-event/8000\r\n\
a=ptime:20\r\na=sendrecv\r\n\
m=video 40002 RTP/AVP 96\r\na=rtpmap:96 VP8/90000\r\n";

    #[test]
    fn negotiates_by_preference() {
        let sdp = Sdp::parse(OFFER.as_bytes()).unwrap();
        let n = sdp.negotiate(&[CodecKind::Pcmu, CodecKind::Opus]).unwrap();
        assert_eq!((n.codec, n.pt, n.dtmf_pt), (CodecKind::Pcmu, 0, Some(102)));
        let n = sdp.negotiate(&[CodecKind::Opus]).unwrap();
        assert_eq!((n.codec, n.pt, n.dtmf_pt), (CodecKind::Opus, 111, Some(101)));
        assert_eq!(n.remote_port, 40000);

        let ans = build_answer(&sdp, &n, "198.51.100.1".parse().unwrap(), 20000, 20, 1, 1);
        assert!(ans.contains("m=audio 20000 RTP/AVP 111 101\r\n"));
        assert!(ans.contains("a=rtpmap:111 opus/48000/2\r\n"));
        assert!(ans.contains("m=video 0 RTP/AVP 96\r\n"));
    }

    #[test]
    fn rejects_without_common_codec() {
        let sdp = Sdp::parse(OFFER.as_bytes()).unwrap();
        assert!(sdp.negotiate(&[CodecKind::Pcma]).is_ok());
        let only_g722 = "v=0\r\nc=IN IP4 1.2.3.4\r\nm=audio 1 RTP/AVP 9\r\n";
        let sdp = Sdp::parse(only_g722.as_bytes()).unwrap();
        assert!(sdp.negotiate(&[CodecKind::Pcmu]).is_err());
    }
}

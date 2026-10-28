//! RTP (RFC 3550) packet handling, RFC 4733 DTMF events and RTP port allocation.

use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use tokio::net::UdpSocket;

#[derive(Debug, Clone, PartialEq)]
pub struct RtpPacket<'a> {
    pub marker: bool,
    pub pt: u8,
    pub seq: u16,
    pub ts: u32,
    pub ssrc: u32,
    pub payload: &'a [u8],
}

impl<'a> RtpPacket<'a> {
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < 12 || buf[0] >> 6 != 2 {
            return None;
        }
        let padding = buf[0] & 0x20 != 0;
        let extension = buf[0] & 0x10 != 0;
        let cc = (buf[0] & 0x0F) as usize;
        let pt = buf[1] & 0x7F;
        // RTCP multiplexed on the same port (RFC 5761): PT 72-76 with marker → skip.
        if (72..=76).contains(&pt) {
            return None;
        }
        let mut off = 12 + cc * 4;
        if extension {
            if buf.len() < off + 4 {
                return None;
            }
            let words = u16::from_be_bytes([buf[off + 2], buf[off + 3]]) as usize;
            off += 4 + words * 4;
        }
        let mut end = buf.len();
        if padding {
            let pad = *buf.last()? as usize;
            end = end.checked_sub(pad)?;
        }
        if off > end {
            return None;
        }
        Some(RtpPacket {
            marker: buf[1] & 0x80 != 0,
            pt,
            seq: u16::from_be_bytes([buf[2], buf[3]]),
            ts: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
            ssrc: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
            payload: &buf[off..end],
        })
    }

    pub fn write(&self, out: &mut Vec<u8>) {
        out.clear();
        out.push(0x80);
        out.push(self.pt | if self.marker { 0x80 } else { 0 });
        out.extend_from_slice(&self.seq.to_be_bytes());
        out.extend_from_slice(&self.ts.to_be_bytes());
        out.extend_from_slice(&self.ssrc.to_be_bytes());
        out.extend_from_slice(self.payload);
    }
}

/// RFC 4733 telephone-event payload.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DtmfEvent {
    pub event: u8,
    pub end: bool,
    pub volume: u8,
    pub duration: u16,
}

impl DtmfEvent {
    pub fn parse(p: &[u8]) -> Option<Self> {
        if p.len() < 4 {
            return None;
        }
        Some(DtmfEvent {
            event: p[0],
            end: p[1] & 0x80 != 0,
            volume: p[1] & 0x3F,
            duration: u16::from_be_bytes([p[2], p[3]]),
        })
    }

    pub fn to_bytes(self) -> [u8; 4] {
        let d = self.duration.to_be_bytes();
        [self.event, (if self.end { 0x80 } else { 0 }) | (self.volume & 0x3F), d[0], d[1]]
    }
}

pub fn dtmf_char(event: u8) -> Option<char> {
    Some(match event {
        0..=9 => (b'0' + event) as char,
        10 => '*',
        11 => '#',
        12..=15 => (b'A' + event - 12) as char,
        _ => return None,
    })
}

pub fn dtmf_event(c: char) -> Option<u8> {
    Some(match c.to_ascii_uppercase() {
        '0'..='9' => c as u8 - b'0',
        '*' => 10,
        '#' => 11,
        'A'..='D' => c.to_ascii_uppercase() as u8 - b'A' + 12,
        _ => return None,
    })
}

/// Hands out even RTP ports from a range; ports return to the pool when the guard drops.
#[derive(Clone)]
pub struct PortPool {
    free: Arc<Mutex<BTreeSet<u16>>>,
}

pub struct RtpSocket {
    pub socket: UdpSocket,
    pub port: u16,
    pool: PortPool,
}

impl Drop for RtpSocket {
    fn drop(&mut self) {
        self.pool.free.lock().unwrap().insert(self.port);
    }
}

impl PortPool {
    pub fn new(min: u16, max: u16) -> Self {
        let start = min + (min % 2);
        let free = (start..=max).step_by(2).collect();
        PortPool { free: Arc::new(Mutex::new(free)) }
    }

    pub async fn bind(&self, ip: IpAddr) -> std::io::Result<RtpSocket> {
        // Try a bounded number of ports; some may be busy with other processes.
        for _ in 0..64 {
            let port = {
                let mut free = self.free.lock().unwrap();
                let n = free.len();
                if n == 0 {
                    break;
                }
                let pick = *free.iter().nth(rand::random_range(0..n)).unwrap();
                free.remove(&pick);
                pick
            };
            match UdpSocket::bind(SocketAddr::new(ip, port)).await {
                Ok(socket) => return Ok(RtpSocket { socket, port, pool: self.clone() }),
                Err(_) => {
                    // Keep it out of the pool for now; it's in use elsewhere.
                    continue;
                }
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::AddrInUse, "no free RTP ports"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rtp_roundtrip() {
        let p = RtpPacket { marker: true, pt: 111, seq: 65535, ts: 0xDEADBEEF, ssrc: 7, payload: b"hey" };
        let mut buf = Vec::new();
        p.write(&mut buf);
        assert_eq!(RtpPacket::parse(&buf), Some(p));
    }

    #[test]
    fn rtp_with_csrc_extension_padding() {
        let mut buf = vec![0xB1, 0x00, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3];
        buf.extend_from_slice(&[0, 0, 0, 9]); // CSRC
        buf.extend_from_slice(&[0xBE, 0xDE, 0, 1, 1, 2, 3, 4]); // ext, 1 word
        buf.extend_from_slice(b"ab");
        buf.extend_from_slice(&[0, 2]); // 2 bytes padding
        let p = RtpPacket::parse(&buf).unwrap();
        assert_eq!(p.payload, b"ab");
    }

    #[test]
    fn dtmf() {
        let e = DtmfEvent { event: 11, end: true, volume: 10, duration: 800 };
        assert_eq!(DtmfEvent::parse(&e.to_bytes()), Some(e));
        assert_eq!(dtmf_char(11), Some('#'));
        assert_eq!(dtmf_event('a'), Some(12));
    }
}

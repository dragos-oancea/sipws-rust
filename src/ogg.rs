//! Ogg encapsulation of Opus (RFC 3533 + RFC 7845), streaming both ways.
//!
//! Muxer: OpusHead (BOS page) → OpusTags page → audio pages. Each page is one
//! WebSocket binary message; granule positions are 48 kHz sample counts.
//! Demuxer: incremental, survives arbitrary chunking, checks CRCs, resyncs on
//! "OggS", and handles chained streams (e.g. one Ogg file per TTS utterance).

const fn crc_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut r = (i as u32) << 24;
        let mut j = 0;
        while j < 8 {
            r = if r & 0x8000_0000 != 0 { (r << 1) ^ 0x04C1_1DB7 } else { r << 1 };
            j += 1;
        }
        t[i] = r;
        i += 1;
    }
    t
}

static CRC: [u32; 256] = crc_table();

/// Ogg CRC-32 (poly 0x04c11db7, no reflection, init 0, no final xor).
pub fn crc32(data: &[u8]) -> u32 {
    data.iter()
        .fold(0u32, |c, &b| (c << 8) ^ CRC[(((c >> 24) as u8) ^ b) as usize])
}

const FLAG_CONTINUED: u8 = 0x01;
const FLAG_BOS: u8 = 0x02;
const FLAG_EOS: u8 = 0x04;
const MAX_SEGMENTS: usize = 255;

fn lacing_len(len: usize) -> usize {
    len / 255 + 1
}

fn build_page(serial: u32, seq: u32, flags: u8, granule: u64, packets: &[Vec<u8>]) -> Vec<u8> {
    let mut lacing = Vec::new();
    for p in packets {
        let mut n = p.len();
        while n >= 255 {
            lacing.push(255u8);
            n -= 255;
        }
        lacing.push(n as u8);
    }
    debug_assert!(lacing.len() <= MAX_SEGMENTS);
    let body: usize = packets.iter().map(Vec::len).sum();
    let mut page = Vec::with_capacity(27 + lacing.len() + body);
    page.extend_from_slice(b"OggS");
    page.push(0); // version
    page.push(flags);
    page.extend_from_slice(&granule.to_le_bytes());
    page.extend_from_slice(&serial.to_le_bytes());
    page.extend_from_slice(&seq.to_le_bytes());
    page.extend_from_slice(&[0; 4]); // CRC placeholder
    page.push(lacing.len() as u8);
    page.extend_from_slice(&lacing);
    for p in packets {
        page.extend_from_slice(p);
    }
    let crc = crc32(&page);
    page[22..26].copy_from_slice(&crc.to_le_bytes());
    page
}

/// OpusHead identification header (RFC 7845 5.1), mapping family 0.
pub fn opus_head(channels: u8, pre_skip: u16, input_rate: u32) -> Vec<u8> {
    let mut h = Vec::with_capacity(19);
    h.extend_from_slice(b"OpusHead");
    h.push(1);
    h.push(channels);
    h.extend_from_slice(&pre_skip.to_le_bytes());
    h.extend_from_slice(&input_rate.to_le_bytes());
    h.extend_from_slice(&0i16.to_le_bytes()); // output gain
    h.push(0); // channel mapping family
    h
}

pub fn opus_tags(vendor: &str, comments: &[String]) -> Vec<u8> {
    let mut t = Vec::new();
    t.extend_from_slice(b"OpusTags");
    t.extend_from_slice(&(vendor.len() as u32).to_le_bytes());
    t.extend_from_slice(vendor.as_bytes());
    t.extend_from_slice(&(comments.len() as u32).to_le_bytes());
    for c in comments {
        t.extend_from_slice(&(c.len() as u32).to_le_bytes());
        t.extend_from_slice(c.as_bytes());
    }
    t
}

pub struct OggMuxer {
    serial: u32,
    seq: u32,
    granule: u64,
    pending: Vec<Vec<u8>>,
    pending_segments: usize,
    pending_dur: u32,
    page_dur: u32,
    finished: bool,
}

impl OggMuxer {
    /// `page_dur` is the audio duration (48 kHz units) collected before a page is emitted.
    pub fn new(serial: u32, page_dur: u32) -> Self {
        OggMuxer {
            serial,
            seq: 0,
            granule: 0,
            pending: Vec::new(),
            pending_segments: 0,
            pending_dur: 0,
            page_dur: page_dur.max(1),
            finished: false,
        }
    }

    /// The two header pages that must start the stream.
    pub fn headers(&mut self, pre_skip: u16, input_rate: u32, comments: &[String]) -> [Vec<u8>; 2] {
        let head = build_page(self.serial, self.seq, FLAG_BOS, 0, &[opus_head(1, pre_skip, input_rate)]);
        self.seq += 1;
        let vendor = concat!("sipws ", env!("CARGO_PKG_VERSION"));
        let tags = build_page(self.serial, self.seq, 0, 0, &[opus_tags(vendor, comments)]);
        self.seq += 1;
        [head, tags]
    }

    fn flush(&mut self, flags: u8) -> Vec<u8> {
        let packets = std::mem::take(&mut self.pending);
        let page = build_page(self.serial, self.seq, flags, self.granule, &packets);
        self.seq += 1;
        self.pending_segments = 0;
        self.pending_dur = 0;
        page
    }

    /// Adds one Opus packet of `dur` samples at 48 kHz. Returns a page when one is complete.
    pub fn push(&mut self, packet: &[u8], dur: u32) -> Option<Vec<u8>> {
        let mut out = None;
        let segs = lacing_len(packet.len());
        if self.pending_segments + segs > MAX_SEGMENTS {
            out = Some(self.flush(0));
        }
        self.pending.push(packet.to_vec());
        self.pending_segments += segs;
        self.pending_dur += dur;
        self.granule += dur as u64;
        if out.is_none() && self.pending_dur >= self.page_dur {
            out = Some(self.flush(0));
        }
        out
    }

    /// Final page with the EOS flag (carrying any pending packets).
    pub fn finish(&mut self) -> Option<Vec<u8>> {
        if self.finished {
            return None;
        }
        self.finished = true;
        Some(self.flush(FLAG_EOS))
    }

    pub fn granule(&self) -> u64 {
        self.granule
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OggPacket {
    pub data: Vec<u8>,
    pub serial: u32,
    /// First packet of a logical stream (BOS page).
    pub bos: bool,
    pub granule: u64,
}

#[derive(Default)]
pub struct OggDemuxer {
    buf: Vec<u8>,
    partial: Vec<u8>,
    serial: Option<u32>,
    pub crc_errors: u64,
}

impl OggDemuxer {
    pub fn feed(&mut self, data: &[u8], out: &mut Vec<OggPacket>) {
        self.buf.extend_from_slice(data);
        let mut pos = 0;
        loop {
            // Find capture pattern.
            let Some(off) = self.buf[pos..].windows(4).position(|w| w == b"OggS") else {
                pos = self.buf.len().saturating_sub(3);
                break;
            };
            pos += off;
            let b = &self.buf[pos..];
            if b.len() > 4 && b[4] != 0 {
                pos += 1; // not a version-0 page header: false sync
                continue;
            }
            if b.len() < 27 {
                break;
            }
            let nseg = b[26] as usize;
            if b.len() < 27 + nseg {
                break;
            }
            let lacing = &b[27..27 + nseg];
            let body_len: usize = lacing.iter().map(|&l| l as usize).sum();
            let total = 27 + nseg + body_len;
            if b.len() < total {
                break;
            }
            let mut page = b[..total].to_vec();
            let crc = u32::from_le_bytes([page[22], page[23], page[24], page[25]]);
            page[22..26].fill(0);
            if crc32(&page) != crc {
                self.crc_errors += 1;
                pos += 1; // false sync or corruption: rescan
                continue;
            }
            let flags = b[5];
            let granule = u64::from_le_bytes(b[6..14].try_into().unwrap());
            let serial = u32::from_le_bytes(b[14..18].try_into().unwrap());
            let bos = flags & FLAG_BOS != 0;
            if bos || self.serial.is_none() {
                self.serial = Some(serial);
                self.partial.clear();
            }
            if Some(serial) == self.serial {
                if flags & FLAG_CONTINUED == 0 {
                    self.partial.clear();
                }
                let mut first = true;
                let mut p = 27 + nseg;
                for &l in lacing {
                    self.partial.extend_from_slice(&b[p..p + l as usize]);
                    p += l as usize;
                    if l < 255 {
                        out.push(OggPacket {
                            data: std::mem::take(&mut self.partial),
                            serial,
                            bos: bos && first,
                            granule,
                        });
                        first = false;
                    }
                }
            }
            pos += total;
        }
        self.buf.drain(..pos.min(self.buf.len()));
    }
}

/// Duration of an Opus packet in 48 kHz samples (RFC 6716 3.1).
pub fn opus_packet_dur(p: &[u8]) -> Option<u32> {
    opus::packet::get_nb_samples(p, 48000).ok().map(|n| n as u32)
}

/// TOC-only Opus packets (zero-length CELT frames, which decoders conceal as
/// lost/DTX) covering `dur` 48 kHz samples. Used to keep Ogg time continuous
/// across loss and silence without re-encoding.
pub fn opus_dtx_packets(mut dur: u32) -> Vec<(Vec<u8>, u32)> {
    // CELT-only fullband configs: 31 = 20 ms, 30 = 10 ms, 29 = 5 ms, 28 = 2.5 ms.
    const FRAMES: [(u8, u32); 4] = [(31, 960), (30, 480), (29, 240), (28, 120)];
    let mut out = Vec::new();
    for (config, n) in FRAMES {
        while dur >= n {
            out.push((vec![config << 3], n));
            dur -= n;
        }
    }
    out
}

/// Pulls Opus audio out of an incoming Ogg stream and decodes it to mono PCM.
pub struct OggOpusReader {
    demux: OggDemuxer,
    pcm_rate: u32,
    decoder: Option<opus::Decoder>,
    state: ReaderState,
    skip: usize,
    pkts: Vec<OggPacket>,
    scratch: Vec<i16>,
    pub errors: u64,
}

#[derive(PartialEq)]
enum ReaderState {
    WaitHead,
    WaitTags,
    Audio,
}

impl OggOpusReader {
    pub fn new(pcm_rate: u32) -> Self {
        OggOpusReader {
            demux: OggDemuxer::default(),
            pcm_rate,
            decoder: None,
            state: ReaderState::WaitHead,
            skip: 0,
            pkts: Vec::new(),
            scratch: vec![0; pcm_rate as usize * 120 / 1000],
            errors: 0,
        }
    }

    pub fn feed(&mut self, data: &[u8], out: &mut Vec<i16>) {
        self.pkts.clear();
        self.demux.feed(data, &mut self.pkts);
        for pkt in std::mem::take(&mut self.pkts) {
            if pkt.bos || pkt.data.starts_with(b"OpusHead") {
                if pkt.data.len() >= 19 && pkt.data.starts_with(b"OpusHead") {
                    let pre_skip = u16::from_le_bytes([pkt.data[10], pkt.data[11]]) as u64;
                    self.skip = (pre_skip * self.pcm_rate as u64 / 48000) as usize;
                    // A fresh decoder per logical stream (chained Ogg files).
                    self.decoder = opus::Decoder::new(self.pcm_rate, opus::Channels::Mono).ok();
                    self.state = ReaderState::WaitTags;
                } else {
                    self.errors += 1;
                    self.state = ReaderState::WaitHead;
                }
                continue;
            }
            match self.state {
                ReaderState::WaitHead => {}
                ReaderState::WaitTags => {
                    if pkt.data.starts_with(b"OpusTags") {
                        self.state = ReaderState::Audio;
                    }
                }
                ReaderState::Audio => {
                    let Some(dec) = self.decoder.as_mut() else { continue };
                    match dec.decode(&pkt.data, &mut self.scratch, false) {
                        Ok(n) => {
                            let drop = self.skip.min(n);
                            self.skip -= drop;
                            out.extend_from_slice(&self.scratch[drop..n]);
                        }
                        Err(_) => self.errors += 1,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_ffmpeg_produced_ogg_opus() {
        // 1 s 440 Hz sine, mono, written by ffmpeg/libopus: validates CRC and lacing against libogg.
        let file = include_bytes!("../tests/fixtures/sine440_1s.opus");
        let mut d = OggDemuxer::default();
        let mut pkts = Vec::new();
        d.feed(file, &mut pkts);
        assert_eq!(d.crc_errors, 0);
        assert!(pkts[0].data.starts_with(b"OpusHead") && pkts[0].bos);
        let mut r = OggOpusReader::new(48000);
        let mut pcm = Vec::new();
        r.feed(file, &mut pcm);
        assert!((47000..=49000).contains(&pcm.len()), "{}", pcm.len());
        assert!(pcm.iter().map(|s| s.unsigned_abs()).max().unwrap() > 1000);
    }

    #[test]
    fn mux_demux_roundtrip_with_odd_chunking() {
        let mut m = OggMuxer::new(77, 960 * 3);
        let mut bytes = Vec::new();
        for p in m.headers(312, 16000, &[]) {
            bytes.extend(p);
        }
        let packets: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 100 + i * 37]).collect();
        let mut pages = 0;
        for p in &packets {
            if let Some(page) = m.push(p, 960) {
                bytes.extend(page);
                pages += 1;
            }
        }
        bytes.extend(m.finish().unwrap());
        assert_eq!(pages, 6);
        let mut d = OggDemuxer::default();
        let mut out = Vec::new();
        for chunk in bytes.chunks(7) {
            d.feed(chunk, &mut out);
        }
        assert_eq!(out.len(), 22);
        assert!(out[0].bos && out[0].data.starts_with(b"OpusHead"));
        assert!(out[1].data.starts_with(b"OpusTags"));
        let got: Vec<Vec<u8>> = out[2..].iter().map(|p| p.data.clone()).collect();
        assert_eq!(got, packets);
        assert_eq!(out.last().unwrap().granule, 20 * 960);
        assert_eq!(d.crc_errors, 0);
    }

    #[test]
    fn large_packets_split_across_lacing_and_pages() {
        let mut m = OggMuxer::new(1, u32::MAX);
        let big = vec![7u8; 255 * 3]; // needs 4 lacing values (255,255,255,0)
        let mut bytes = Vec::new();
        for _ in 0..70 {
            if let Some(p) = m.push(&big, 960) {
                bytes.extend(p);
            }
        }
        bytes.extend(m.finish().unwrap());
        let mut d = OggDemuxer::default();
        let mut out = Vec::new();
        d.feed(&bytes, &mut out);
        assert_eq!(out.len(), 70);
        assert!(out.iter().all(|p| p.data == big));
    }

    #[test]
    fn demux_resyncs_after_garbage_and_corruption() {
        let mut m = OggMuxer::new(5, 960);
        let mut good = Vec::new();
        for i in 0..3u8 {
            good.extend(m.push(&[i; 10], 960).unwrap());
        }
        let mut bytes = b"garbageOggS..".to_vec();
        let mut corrupt = good.clone();
        corrupt[30] ^= 0xFF; // damage first page body
        bytes.extend(corrupt);
        let mut d = OggDemuxer::default();
        let mut out = Vec::new();
        d.feed(&bytes, &mut out);
        assert_eq!(out.iter().map(|p| p.data[0]).collect::<Vec<_>>(), [1, 2]);
        assert!(d.crc_errors >= 1);
    }

    #[test]
    fn dtx_packets_cover_duration_and_decode() {
        let pk = opus_dtx_packets(960 * 2 + 480 + 120);
        assert_eq!(pk.iter().map(|p| p.1).sum::<u32>(), 2520);
        let mut dec = opus::Decoder::new(48000, opus::Channels::Mono).unwrap();
        let mut buf = vec![0i16; 5760];
        for (p, n) in pk {
            assert_eq!(opus_packet_dur(&p), Some(n));
            assert_eq!(dec.decode(&p, &mut buf, false).unwrap() as u32, n);
        }
    }

    #[test]
    fn reader_decodes_chained_streams() {
        let mut enc = opus::Encoder::new(16000, opus::Channels::Mono, opus::Application::Voip).unwrap();
        let mut bytes = Vec::new();
        for serial in [1u32, 2] {
            let mut m = OggMuxer::new(serial, 960);
            for p in m.headers(0, 16000, &[]) {
                bytes.extend(p);
            }
            for _ in 0..10 {
                let pcm = vec![0i16; 320];
                let pkt = enc.encode_vec(&pcm, 1500).unwrap();
                bytes.extend(m.push(&pkt, 960).unwrap());
            }
            bytes.extend(m.finish().unwrap());
        }
        let mut r = OggOpusReader::new(16000);
        let mut pcm = Vec::new();
        for c in bytes.chunks(100) {
            r.feed(c, &mut pcm);
        }
        assert_eq!(pcm.len(), 2 * 10 * 320);
        assert_eq!(r.errors, 0);
    }
}

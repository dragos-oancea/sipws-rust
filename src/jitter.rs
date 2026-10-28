//! Adaptive RTP jitter buffer.
//!
//! Packets are ordered by (unwrapped) sequence number and released on the
//! playout clock, not on arrival. The buffer:
//! * waits until `target` worth of media is queued (or the oldest packet has
//!   waited that long) before starting, and again after every underrun, so
//!   the delay adapts upwards to the network jitter;
//! * reorders and de-duplicates; packets arriving after their slot was played are dropped;
//! * reports missing packets as `Lost` (with the following packet attached so
//!   the Opus decoder can recover the frame from in-band FEC);
//! * turns timestamp jumps with contiguous sequence numbers (DTX, VAD, silence
//!   suppression) into `Silence` of the right length, shortening that silence
//!   when the buffer has grown well past target, which shrinks delay back down;
//! * trims the oldest media if the buffer exceeds `max`;
//! * resets on SSRC change or a large sequence jump.
//!
//! All durations are in RTP clock units.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum JbItem {
    Packet { payload: Vec<u8>, dur: u32 },
    /// A packet that never arrived. `fec` is the next packet, if it is the very next one.
    Lost { dur: u32, fec: Option<Vec<u8>> },
    /// Sender-side silence (timestamp gap) or buffering.
    Silence { dur: u32 },
}

impl JbItem {
    pub fn dur(&self) -> u32 {
        match self {
            JbItem::Packet { dur, .. } | JbItem::Lost { dur, .. } | JbItem::Silence { dur } => *dur,
        }
    }
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct JbStats {
    pub received: u64,
    pub played: u64,
    pub lost: u64,
    pub fec_available: u64,
    pub late: u64,
    pub duplicate: u64,
    pub underruns: u64,
    pub overflow_dropped: u64,
    pub resets: u64,
    /// Current buffered media, ms.
    pub depth_ms: u32,
}

struct Slot {
    /// false for packets of another payload type sharing the stream (RFC 4733 events):
    /// they occupy a sequence number but carry no audio.
    media: bool,
    ts: u32,
    dur: u32,
    payload: Vec<u8>,
    arrival: Instant,
}

pub struct JitterBuffer {
    clock: u32,
    target: u32,
    max: u32,
    default_dur: u32,
    max_gap: u32,
    dur_of: fn(&[u8]) -> Option<u32>,
    slots: BTreeMap<u64, Slot>,
    buffered: u32,
    highest: Option<u64>,
    ssrc: Option<u32>,
    playing: bool,
    /// next_seq is meaningful (set once playout started; survives underruns).
    positioned: bool,
    next_seq: u64,
    next_ts: u32,
    last_dur: u32,
    pub stats: JbStats,
}

/// Unwraps a 16-bit sequence number relative to the highest one seen.
fn unwrap_seq(highest: u64, seq: u16) -> u64 {
    let base = highest & !0xFFFF;
    let cand = [base | seq as u64, (base | seq as u64).wrapping_add(0x10000), (base | seq as u64).wrapping_sub(0x10000)];
    *cand.iter().min_by_key(|c| (**c as i64 - highest as i64).unsigned_abs()).unwrap()
}

impl JitterBuffer {
    /// `dur_of` returns a payload's duration in clock units (None = use the last known).
    pub fn new(clock: u32, target_ms: u32, max_ms: u32, default_dur: u32, dur_of: fn(&[u8]) -> Option<u32>) -> Self {
        JitterBuffer {
            clock,
            target: clock / 1000 * target_ms,
            max: clock / 1000 * max_ms.max(target_ms + 40),
            default_dur,
            max_gap: clock * 5, // longer timestamp jumps are treated as a resync, not silence
            dur_of,
            slots: BTreeMap::new(),
            buffered: 0,
            highest: None,
            ssrc: None,
            playing: false,
            positioned: false,
            next_seq: 0,
            next_ts: 0,
            last_dur: default_dur,
            stats: JbStats::default(),
        }
    }

    fn reset(&mut self) {
        self.slots.clear();
        self.buffered = 0;
        self.highest = None;
        self.playing = false;
        self.positioned = false;
        self.last_dur = self.default_dur;
        self.stats.resets += 1;
    }

    pub fn depth_ms(&self) -> u32 {
        (self.buffered as u64 * 1000 / self.clock as u64) as u32
    }

    pub fn push(&mut self, ssrc: u32, seq: u16, ts: u32, payload: &[u8], now: Instant) {
        self.insert(ssrc, seq, ts, Some(payload), now);
    }

    /// Registers a non-audio packet (e.g. telephone-event) so its sequence number
    /// isn't mistaken for a lost audio packet.
    pub fn push_skip(&mut self, ssrc: u32, seq: u16, now: Instant) {
        self.insert(ssrc, seq, 0, None, now);
    }

    fn insert(&mut self, ssrc: u32, seq: u16, ts: u32, payload: Option<&[u8]>, now: Instant) {
        if self.ssrc != Some(ssrc) {
            if self.ssrc.is_some() {
                self.reset();
            }
            self.ssrc = Some(ssrc);
        }
        let ext = match self.highest {
            None => (1u64 << 32) | seq as u64,
            Some(h) => {
                let e = unwrap_seq(h, seq);
                if e.abs_diff(h) > 3000 {
                    // Sender restarted its sequence space without changing SSRC.
                    self.reset();
                    (1u64 << 32) | seq as u64
                } else {
                    e
                }
            }
        };
        if payload.is_some() {
            self.stats.received += 1;
        }
        if self.positioned && ext < self.next_seq {
            self.stats.late += 1;
            return;
        }
        if self.slots.contains_key(&ext) {
            self.stats.duplicate += 1;
            return;
        }
        let Some(payload) = payload else {
            self.slots.insert(ext, Slot { media: false, ts, dur: 0, payload: Vec::new(), arrival: now });
            self.highest = Some(self.highest.map_or(ext, |h| h.max(ext)));
            return;
        };
        let dur = (self.dur_of)(payload)
            .filter(|d| *d > 0 && *d <= self.clock / 1000 * 120)
            .unwrap_or(self.last_dur);
        self.slots.insert(ext, Slot { media: true, ts, dur, payload: payload.to_vec(), arrival: now });
        self.buffered += dur;
        self.highest = Some(self.highest.map_or(ext, |h| h.max(ext)));
    }

    fn take_first(&mut self) -> Option<(u64, Slot)> {
        let (k, s) = self.slots.pop_first()?;
        self.buffered -= s.dur;
        Some((k, s))
    }

    /// First slot carrying audio: (key, ts, arrival).
    fn first_media(&self) -> Option<(u64, u32, Instant)> {
        self.slots.iter().find(|(_, s)| s.media).map(|(k, s)| (*k, s.ts, s.arrival))
    }

    /// Drops every slot before `key` (only non-audio/stale ones are expected there).
    fn drop_before(&mut self, key: u64) {
        while self.slots.first_key_value().is_some_and(|(k, _)| *k < key) {
            self.take_first();
        }
    }

    /// Next item due for playout, or None when there is nothing to play (buffering/underrun).
    /// Silence is handed out in pieces of at most `max_silence` so the caller can pace it.
    pub fn pop(&mut self, now: Instant, max_silence: u32) -> Option<JbItem> {
        self.stats.depth_ms = self.depth_ms();
        if self.positioned {
            // Event slots at the play position just consume their sequence number.
            self.drop_before(self.next_seq);
            while self.slots.first_key_value().is_some_and(|(k, s)| *k == self.next_seq && !s.media) {
                self.slots.pop_first();
                self.next_seq += 1;
            }
        }
        let Some((mut key, mut ts, arrival)) = self.first_media() else {
            if self.playing {
                self.playing = false;
                self.stats.underruns += 1;
            }
            return None;
        };
        if !self.playing {
            let waited = now.saturating_duration_since(arrival);
            let target_time = Duration::from_millis(self.target as u64 * 1000 / self.clock as u64);
            if self.buffered < self.target && waited < target_time {
                return None;
            }
            // (Re)start on the oldest audio packet. After an underrun the silence already played
            // covers the pause, so the timestamp gap must not be played again; audio packets that
            // went missing during the underrun count as lost.
            if self.positioned && key > self.next_seq {
                let events = self.slots.range(self.next_seq..key).count() as u64;
                self.stats.lost += key - self.next_seq - events;
            }
            self.drop_before(key);
            self.playing = true;
            self.positioned = true;
            self.next_seq = key;
            self.next_ts = ts;
        }
        // Too much queued (sender burst, clock drift): drop the oldest audio.
        while self.buffered > self.max {
            self.drop_before(key + 1);
            self.stats.overflow_dropped += 1;
            let Some((k, t, _)) = self.first_media() else { break };
            self.drop_before(k);
            (key, ts) = (k, t);
            self.next_seq = k;
            self.next_ts = t;
        }
        if key == self.next_seq {
            let gap = ts.wrapping_sub(self.next_ts) as i32;
            if gap > 0 && (gap as u32) <= self.max_gap {
                // DTX/silence suppression: play the silence, shortened if we're running deep
                // (this is how accumulated delay drains back towards the target).
                let mut gap = gap as u32;
                let excess = self.buffered.saturating_sub(self.target * 2);
                let skip = gap.min(excess);
                self.next_ts = self.next_ts.wrapping_add(skip);
                gap -= skip;
                if gap > 0 {
                    let dur = gap.min(max_silence.max(1));
                    self.next_ts = self.next_ts.wrapping_add(dur);
                    return Some(JbItem::Silence { dur });
                }
            }
            let (_, s) = self.take_first().unwrap();
            self.next_seq += 1;
            self.next_ts = s.ts.wrapping_add(s.dur);
            self.last_dur = s.dur;
            self.stats.played += 1;
            return Some(JbItem::Packet { payload: s.payload, dur: s.dur });
        }
        // Hole at next_seq while later audio is here: it is lost. Event slots in between
        // aren't audio, so they don't count towards the missing packets.
        let events = self.slots.range(self.next_seq..key).count() as u32;
        let missing = ((key - self.next_seq) as u32).saturating_sub(events).max(1);
        let span = ts.wrapping_sub(self.next_ts) as i32;
        let dur = if span > 0 && span as u32 >= missing && (span as u32) / missing <= self.clock / 1000 * 120 {
            span as u32 / missing
        } else {
            self.last_dur
        };
        let fec = (missing == 1).then(|| self.slots[&key].payload.clone());
        if fec.is_some() {
            self.stats.fec_available += 1;
        }
        self.stats.lost += 1;
        self.next_seq += 1;
        self.next_ts = self.next_ts.wrapping_add(dur);
        Some(JbItem::Lost { dur, fec })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g711_dur(p: &[u8]) -> Option<u32> {
        Some(p.len() as u32)
    }

    fn jb() -> JitterBuffer {
        JitterBuffer::new(8000, 60, 300, 160, g711_dur)
    }

    fn pkt(n: u8) -> Vec<u8> {
        vec![n; 160]
    }

    fn drain(j: &mut JitterBuffer, now: Instant) -> Vec<JbItem> {
        let mut v = Vec::new();
        while let Some(i) = j.pop(now, u32::MAX) {
            v.push(i);
        }
        v
    }

    fn ids(items: &[JbItem]) -> Vec<String> {
        items
            .iter()
            .map(|i| match i {
                JbItem::Packet { payload, .. } => format!("p{}", payload[0]),
                JbItem::Lost { fec, .. } => format!("L{}", if fec.is_some() { "f" } else { "" }),
                JbItem::Silence { dur } => format!("s{dur}"),
            })
            .collect()
    }

    #[test]
    fn waits_for_target_then_reorders() {
        let mut j = jb();
        let t = Instant::now();
        j.push(1, 10, 1600, &pkt(10), t);
        j.push(1, 12, 1920, &pkt(12), t);
        assert_eq!(j.pop(t, u32::MAX), None, "only 40 ms buffered, target 60");
        j.push(1, 11, 1760, &pkt(11), t);
        assert_eq!(ids(&drain(&mut j, t)), ["p10", "p11", "p12"]);
        assert_eq!(j.stats.underruns, 1);
    }

    #[test]
    fn starts_on_timeout_even_if_short() {
        let mut j = jb();
        let t = Instant::now();
        j.push(1, 1, 0, &pkt(1), t);
        assert_eq!(j.pop(t, u32::MAX), None);
        assert_eq!(ids(&drain(&mut j, t + Duration::from_millis(61))), ["p1"]);
    }

    #[test]
    fn loss_with_fec_hint_and_late_drop() {
        let mut j = jb();
        let t = Instant::now();
        for s in [1u16, 2, 4, 6, 7] {
            j.push(1, s, s as u32 * 160, &pkt(s as u8), t);
        }
        let out = drain(&mut j, t);
        assert_eq!(ids(&out), ["p1", "p2", "Lf", "p4", "Lf", "p6", "p7"]);
        assert!(out.iter().all(|i| i.dur() == 160));
        j.push(1, 3, 480, &pkt(3), t); // arrives after its slot
        assert_eq!(j.stats.late, 1);
        j.push(1, 7, 7 * 160, &pkt(7), t);
        assert_eq!(j.stats.lost, 2);
    }

    #[test]
    fn multi_packet_hole_uses_timestamp_span() {
        let mut j = jb();
        let t = Instant::now();
        j.push(1, 1, 0, &vec![1; 320], t); // 40 ms packets
        j.push(1, 4, 960, &vec![4; 320], t);
        let out = drain(&mut j, t);
        assert_eq!(ids(&out), ["p1", "L", "Lf", "p4"]);
        assert_eq!(out[1].dur(), 320);
    }

    #[test]
    fn dtx_gap_becomes_silence() {
        let mut j = jb();
        let t = Instant::now();
        for (s, ts) in [(1u16, 0u32), (2, 160), (3, 160 * 50), (4, 160 * 51)] {
            j.push(1, s, ts, &pkt(s as u8), t);
        }
        assert_eq!(ids(&drain(&mut j, t)), ["p1", "p2", "s7680", "p3", "p4"]);
    }

    #[test]
    fn duplicates_wraparound_and_ssrc_reset() {
        let mut j = jb();
        let t = Instant::now();
        for s in [65534u16, 65535, 0, 0, 1] {
            j.push(1, s, s.wrapping_add(10) as u32 * 160, &pkt(s as u8), t);
        }
        assert_eq!(j.stats.duplicate, 1);
        assert_eq!(drain(&mut j, t).len(), 4);
        j.push(2, 500, 0, &pkt(9), t);
        assert_eq!(j.stats.resets, 1);
        assert_eq!(ids(&drain(&mut j, t + Duration::from_millis(100))), ["p9"]);
    }

    #[test]
    fn telephone_event_packets_are_not_loss() {
        let mut j = jb();
        let t = Instant::now();
        j.push(1, 1, 0, &pkt(1), t);
        j.push_skip(1, 2, t);
        j.push_skip(1, 3, t);
        j.push(1, 4, 160, &pkt(4), t);
        j.push(1, 6, 480, &pkt(6), t); // 5 really lost
        j.push(1, 7, 640, &pkt(7), t);
        assert_eq!(ids(&drain(&mut j, t)), ["p1", "p4", "Lf", "p6", "p7"]);
        assert_eq!(j.stats.lost, 1);
    }

    #[test]
    fn overflow_trims_oldest() {
        let mut j = jb();
        let t = Instant::now();
        for s in 0..40u16 {
            j.push(1, s, s as u32 * 160, &pkt(s as u8), t);
        }
        let out = drain(&mut j, t);
        assert!(j.stats.overflow_dropped > 0);
        assert!(out.len() as u32 * 20 <= 300);
        assert_eq!(ids(&out).last().unwrap(), "p39");
    }
}

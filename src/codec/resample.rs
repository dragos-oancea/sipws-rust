//! Streaming integer-ratio resampler (polyphase windowed-sinc FIR).
//! Only integer ratios are needed: G.711 runs at 8 kHz and the WebSocket side
//! is 8/16/24/48 kHz, so the ratio is always 1, 2, 3 or 6 in one direction.

use std::collections::VecDeque;

const TAPS_PER_PHASE: usize = 24;

pub struct Resampler {
    mode: Mode,
    taps: Vec<f32>,
    hist: VecDeque<f32>,
    phase: usize,
}

enum Mode {
    Passthrough,
    Up(usize),
    Down(usize),
}

fn design(factor: usize, cutoff: f64, gain: f64) -> Vec<f32> {
    let n = factor * TAPS_PER_PHASE;
    let mid = (n as f64 - 1.0) / 2.0;
    (0..n)
        .map(|i| {
            let x = i as f64 - mid;
            let sinc = if x == 0.0 {
                2.0 * cutoff
            } else {
                (2.0 * std::f64::consts::PI * cutoff * x).sin() / (std::f64::consts::PI * x)
            };
            let w = 0.42 - 0.5 * (2.0 * std::f64::consts::PI * i as f64 / (n - 1) as f64).cos()
                + 0.08 * (4.0 * std::f64::consts::PI * i as f64 / (n - 1) as f64).cos();
            (sinc * w * gain) as f32
        })
        .collect()
}

impl Resampler {
    pub fn new(from_rate: u32, to_rate: u32) -> Self {
        assert!(from_rate > 0 && to_rate > 0);
        let (mode, taps, hist_len) = if from_rate == to_rate {
            (Mode::Passthrough, Vec::new(), 0)
        } else if to_rate > from_rate {
            assert_eq!(to_rate % from_rate, 0, "only integer ratios supported");
            let l = (to_rate / from_rate) as usize;
            (Mode::Up(l), design(l, 0.45 / l as f64, l as f64), TAPS_PER_PHASE)
        } else {
            assert_eq!(from_rate % to_rate, 0, "only integer ratios supported");
            let m = (from_rate / to_rate) as usize;
            (Mode::Down(m), design(m, 0.45 / m as f64, 1.0), m * TAPS_PER_PHASE)
        };
        Resampler {
            mode,
            taps,
            hist: VecDeque::from(vec![0.0; hist_len]),
            phase: 0,
        }
    }

    pub fn process(&mut self, input: &[i16], out: &mut Vec<i16>) {
        match self.mode {
            Mode::Passthrough => out.extend_from_slice(input),
            Mode::Up(l) => {
                for &x in input {
                    self.hist.pop_front();
                    self.hist.push_back(x as f32);
                    let newest = self.hist.len() - 1;
                    for p in 0..l {
                        let mut acc = 0.0f32;
                        for k in 0..TAPS_PER_PHASE {
                            acc += self.taps[p + k * l] * self.hist[newest - k];
                        }
                        out.push(clamp(acc));
                    }
                }
            }
            Mode::Down(m) => {
                for &x in input {
                    self.hist.pop_front();
                    self.hist.push_back(x as f32);
                    self.phase += 1;
                    if self.phase == m {
                        self.phase = 0;
                        let newest = self.hist.len() - 1;
                        let acc: f32 = self
                            .taps
                            .iter()
                            .enumerate()
                            .map(|(k, h)| h * self.hist[newest - k])
                            .sum();
                        out.push(clamp(acc));
                    }
                }
            }
        }
    }
}

fn clamp(v: f32) -> i16 {
    v.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(rate: u32, freq: f64, n: usize) -> Vec<i16> {
        (0..n)
            .map(|i| ((2.0 * std::f64::consts::PI * freq * i as f64 / rate as f64).sin() * 10000.0) as i16)
            .collect()
    }

    fn rms(x: &[i16]) -> f64 {
        (x.iter().map(|&v| (v as f64).powi(2)).sum::<f64>() / x.len() as f64).sqrt()
    }

    #[test]
    fn up_and_down_preserve_inband_tone() {
        for (a, b) in [(8000, 16000), (8000, 48000), (16000, 8000), (48000, 8000), (24000, 8000)] {
            let mut r = Resampler::new(a, b);
            let mut out = Vec::new();
            for chunk in tone(a, 1000.0, a as usize).chunks(160) {
                r.process(chunk, &mut out);
            }
            assert_eq!(out.len(), b as usize, "{a}->{b}");
            let level = rms(&out[out.len() / 2..]);
            assert!((level - 7071.0).abs() < 500.0, "{a}->{b} rms {level}");
        }
    }

    #[test]
    fn downsampling_rejects_out_of_band() {
        let mut r = Resampler::new(48000, 8000);
        let mut out = Vec::new();
        r.process(&tone(48000, 12000.0, 48000), &mut out);
        assert!(rms(&out[1000..]) < 300.0);
    }
}

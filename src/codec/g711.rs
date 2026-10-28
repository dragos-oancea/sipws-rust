//! ITU-T G.711 mu-law and A-law (reference-style bit manipulation).

const BIAS: i32 = 0x84;
const CLIP: i32 = 32635;

pub fn linear_to_ulaw(sample: i16) -> u8 {
    let mut s = sample as i32;
    let sign = if s < 0 {
        s = -s;
        0x80
    } else {
        0
    };
    if s > CLIP {
        s = CLIP;
    }
    s += BIAS;
    let exponent = exp_for(s);
    let mantissa = (s >> (exponent + 3)) & 0x0F;
    !((sign | (exponent << 4) | mantissa) as u8)
}

fn exp_for(s: i32) -> i32 {
    let mut exp = 7;
    let mut mask = 0x4000;
    while exp > 0 && (s & mask) == 0 {
        exp -= 1;
        mask >>= 1;
    }
    exp
}

pub fn ulaw_to_linear(u: u8) -> i16 {
    let u = !u as i32;
    let sign = u & 0x80;
    let exponent = (u >> 4) & 0x07;
    let mantissa = u & 0x0F;
    let mut s = ((mantissa << 3) + BIAS) << exponent;
    s -= BIAS;
    (if sign != 0 { -s } else { s }) as i16
}

pub fn linear_to_alaw(sample: i16) -> u8 {
    let mut s = sample as i32;
    let sign = if s >= 0 { 0xD5 } else { 0x55 };
    if s < 0 {
        s = -s - 1;
    }
    s = s.min(32767);
    let out = if s < 256 {
        s >> 4
    } else {
        let mut exp = 7;
        let mut mask = 0x4000;
        while exp > 1 && (s & mask) == 0 {
            exp -= 1;
            mask >>= 1;
        }
        (exp << 4) | ((s >> (exp + 3)) & 0x0F)
    };
    (out ^ sign) as u8
}

pub fn alaw_to_linear(a: u8) -> i16 {
    let a = (a ^ 0x55) as i32;
    let sign = a & 0x80;
    let exp = (a >> 4) & 0x07;
    let mant = a & 0x0F;
    let mag = if exp == 0 {
        (mant << 4) + 8
    } else {
        ((mant << 4) + 0x108) << (exp - 1)
    };
    (if sign != 0 { mag } else { -mag }) as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulaw_known_values() {
        assert_eq!(linear_to_ulaw(0), 0xFF);
        assert_eq!(ulaw_to_linear(0xFF), 0);
        assert_eq!(ulaw_to_linear(0x00), -32124);
        assert_eq!(ulaw_to_linear(0x80), 32124);
    }

    #[test]
    fn alaw_known_values() {
        assert_eq!(linear_to_alaw(0), 0xD5);
        assert_eq!(alaw_to_linear(0xD5), 8);
        assert_eq!(alaw_to_linear(0x55), -8);
    }

    #[test]
    fn roundtrip_error_is_bounded() {
        for s in (-32768i32..32767).step_by(7) {
            let s = s as i16;
            let u = ulaw_to_linear(linear_to_ulaw(s)) as i32;
            let a = alaw_to_linear(linear_to_alaw(s)) as i32;
            let tol = (s as i32).abs() / 16 + 16;
            assert!((u - s as i32).abs() <= tol.max(140), "ulaw {s} -> {u}");
            assert!((a - s as i32).abs() <= tol.max(140), "alaw {s} -> {a}");
        }
    }
}

//! Per-call JSONL transcript files (docs/stt-protocol.md §8).

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tracing::warn;

pub struct Transcript {
    file: Option<tokio::fs::File>,
    call_id: String,
}

/// Call-IDs can contain anything; keep file names portable and unambiguous.
fn file_name(call_id: &str) -> String {
    let mut s: String = call_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || "-_.@".contains(c) { c } else { '_' })
        .take(150)
        .collect();
    if s.is_empty() || s.starts_with('.') {
        s.insert(0, '_');
    }
    s + ".jsonl"
}

/// RFC 3339 UTC timestamp with milliseconds, without pulling in a date crate.
pub fn now_rfc3339() -> String {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60,
        d.subsec_millis()
    )
}

impl Transcript {
    pub async fn open(dir: Option<&Path>, call_id: &str) -> Self {
        let file = match dir {
            None => None,
            Some(dir) => {
                let path = dir.join(file_name(call_id));
                match tokio::fs::OpenOptions::new().create(true).append(true).open(&path).await {
                    Ok(f) => Some(f),
                    Err(e) => {
                        warn!(call_id, path = %path.display(), "cannot open transcript: {e}");
                        None
                    }
                }
            }
        };
        Transcript { file, call_id: call_id.to_string() }
    }

    pub async fn write(&mut self, line: &Value) {
        let Some(f) = self.file.as_mut() else { return };
        let mut bytes = line.to_string().into_bytes();
        bytes.push(b'\n');
        // One write per line plus flush: a crash leaves at most a partial last line.
        if let Err(e) = async { f.write_all(&bytes).await?; f.flush().await }.await {
            warn!(call_id = self.call_id, "transcript write failed, disabling: {e}");
            self.file = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_dates() {
        assert_eq!(file_name("a84b/../x y@host"), "a84b_.._x_y@host.jsonl");
        assert_eq!(file_name("..."), "_....jsonl");
        let t = now_rfc3339();
        assert_eq!(t.len(), 24);
        assert!(t.starts_with("20") && t.ends_with('Z'), "{t}");
    }
}

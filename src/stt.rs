//! `sipws.stt/1`: speech-to-text results coming back over the call's WebSocket.
//! See docs/stt-protocol.md. This module is the I/O-free state machine; call.rs wires it up.

use std::collections::{HashMap, HashSet};

use clap::ValueEnum;
use serde_json::{json, Map, Value};

pub const PROTOCOL: &str = "sipws.stt/1";
pub const BEGIN_ID: &str = "stt-begin";
pub const FINALIZE_ID: &str = "stt-finalize";

pub const ERR_FINALIZED: i64 = -32010;
pub const ERR_ORDER: i64 = -32011;
pub const ERR_TIME: i64 = -32012;
pub const ERR_TOO_MANY: i64 = -32013;
pub const ERR_TRACK: i64 = -32014;
const ERR_PARAMS: i64 = -32602;

const MAX_OPEN: usize = 16;
const MAX_ID: usize = 128;
const MAX_TEXT: usize = 64 * 1024;
const MAX_WORDS: usize = 2000;
const MAX_ALTS: usize = 10;
/// Results may run slightly ahead of what we've sent (clock rounding, engine padding).
const TIME_SLACK_MS: u64 = 2000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BargeIn {
    Off,
    /// stt.speech_started, or the first non-empty partial/final.
    Speech,
    /// First partial/final with at least --barge-in-min-chars non-space characters.
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// stt.begin sent, no answer yet.
    Pending,
    Active,
    /// App answered stt.begin with an error (doesn't do STT), or never answered.
    Inactive,
    /// App reported a fatal stt.error.
    Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Value,
}

fn err(code: i64, message: impl Into<String>, data: Value) -> RpcError {
    RpcError { code, message: message.into(), data }
}

fn bad(field: &str, why: &str) -> RpcError {
    err(ERR_PARAMS, format!("{field}: {why}"), json!({ "field": field }))
}

/// Things the caller has to act on after a message was accepted.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// Cut queued playout (if any) for this utterance.
    BargeIn { utterance_id: String },
    /// Line for the transcript file (finals, errors).
    Record(Value),
    /// A committed, non-empty final (also included as Record).
    Final { utterance_id: String, text: String, end_of_turn: bool },
    /// The engine reported an error.
    EngineError { code: String, message: String, fatal: bool },
}

#[derive(Default)]
struct Open {
    start_ms: Option<u64>,
    last_seq: Option<u64>,
    last_text: String,
    barged: bool,
}

pub struct Session {
    pub state: State,
    barge_in: BargeIn,
    min_chars: usize,
    open: HashMap<String, Open>,
    finalized: HashSet<String>,
    finals: Vec<Value>,
}

/// Typed access to params with spec-conformant error messages.
struct P<'a>(&'a Map<String, Value>);

impl<'a> P<'a> {
    fn str_opt(&self, f: &str, max: usize) -> Result<Option<&'a str>, RpcError> {
        match self.0.get(f) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if s.len() <= max => Ok(Some(s)),
            Some(Value::String(_)) => Err(bad(f, "too long")),
            Some(_) => Err(bad(f, "must be a string")),
        }
    }
    fn str_req(&self, f: &str, max: usize) -> Result<&'a str, RpcError> {
        self.str_opt(f, max)?.ok_or_else(|| bad(f, "required"))
    }
    fn ms_opt(&self, f: &str) -> Result<Option<u64>, RpcError> {
        match self.0.get(f) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v.as_u64().map(Some).ok_or_else(|| bad(f, "must be a non-negative integer")),
        }
    }
    fn ms_req(&self, f: &str) -> Result<u64, RpcError> {
        self.ms_opt(f)?.ok_or_else(|| bad(f, "required"))
    }
    fn conf(&self, f: &str) -> Result<Option<f64>, RpcError> {
        match self.0.get(f) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => match v.as_f64() {
                Some(c) if (0.0..=1.0).contains(&c) => Ok(Some(c)),
                _ => Err(bad(f, "must be a number in 0..1")),
            },
        }
    }
    fn bool_opt(&self, f: &str) -> Result<Option<bool>, RpcError> {
        match self.0.get(f) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(_) => Err(bad(f, "must be a boolean")),
        }
    }
    fn arr(&self, f: &str, max: usize) -> Result<&'a [Value], RpcError> {
        match self.0.get(f) {
            None | Some(Value::Null) => Ok(&[]),
            Some(Value::Array(a)) if a.len() <= max => Ok(a),
            Some(Value::Array(_)) => Err(bad(f, &format!("at most {max} entries"))),
            Some(_) => Err(bad(f, "must be an array")),
        }
    }
}

fn check_time(id: &str, ms: u64, audio_ms: u64) -> Result<(), RpcError> {
    if ms > audio_ms + TIME_SLACK_MS {
        return Err(err(
            ERR_TIME,
            format!("timestamp {ms} ms is past the audio sent ({audio_ms} ms)"),
            json!({ "utterance_id": id, "audio_ms": audio_ms }),
        ));
    }
    Ok(())
}

fn non_space_chars(s: &str) -> usize {
    s.chars().filter(|c| !c.is_whitespace()).count()
}

fn set(obj: &mut Map<String, Value>, k: &str, v: Option<Value>) {
    if let Some(v) = v {
        obj.insert(k.into(), v);
    }
}

impl Session {
    pub fn new(barge_in: BargeIn, min_chars: usize) -> Self {
        Session {
            state: State::Pending,
            barge_in,
            min_chars: min_chars.max(1),
            open: HashMap::new(),
            finalized: HashSet::new(),
            finals: Vec::new(),
        }
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    /// Non-empty finals so far, ordered by start_ms.
    pub fn transcript(&self) -> Value {
        let mut v = self.finals.clone();
        v.sort_by_key(|f| f["start_ms"].as_u64().unwrap_or(0));
        json!({ "utterances": v, "open": self.open.len() })
    }

    /// Open utterances at the end of the call, as transcript lines.
    pub fn abandon_all(&mut self) -> Vec<Value> {
        let mut out: Vec<(String, Open)> = self.open.drain().collect();
        out.sort_by_key(|(_, o)| o.start_ms.unwrap_or(u64::MAX));
        out.into_iter()
            .map(|(id, o)| {
                let mut m = Map::new();
                m.insert("type".into(), json!("abandoned"));
                m.insert("utterance_id".into(), json!(id));
                m.insert("text".into(), json!(o.last_text));
                set(&mut m, "start_ms", o.start_ms.map(Value::from));
                Value::Object(m)
            })
            .collect()
    }

    /// Opens (or returns) an utterance, enforcing finalization and the open limit.
    fn open_utt(&mut self, id: &str) -> Result<&mut Open, RpcError> {
        if self.finalized.contains(id) {
            return Err(err(ERR_FINALIZED, "utterance already finalized", json!({ "utterance_id": id })));
        }
        if !self.open.contains_key(id) && self.open.len() >= MAX_OPEN {
            return Err(err(ERR_TOO_MANY, "too many open utterances", json!({ "utterance_id": id })));
        }
        Ok(self.open.entry(id.to_string()).or_default())
    }

    fn maybe_barge(&mut self, id: &str, text: Option<&str>, speech_event: bool, effects: &mut Vec<Effect>) {
        let trigger = match self.barge_in {
            BargeIn::Off => false,
            BargeIn::Speech => speech_event || text.is_some_and(|t| non_space_chars(t) > 0),
            BargeIn::Partial => text.is_some_and(|t| non_space_chars(t) >= self.min_chars),
        };
        if !trigger {
            return;
        }
        if let Some(o) = self.open.get_mut(id) {
            if o.barged {
                return;
            }
            o.barged = true;
        }
        effects.push(Effect::BargeIn { utterance_id: id.to_string() });
    }

    /// Handles one app → sipws `stt.*` call. Returns None if `method` isn't an STT result method.
    /// On error nothing changes.
    pub fn handle(&mut self, method: &str, params: &Value, audio_ms: u64) -> Option<Result<(Value, Vec<Effect>), RpcError>> {
        let empty = Map::new();
        let map = match params {
            Value::Object(m) => m,
            Value::Null => &empty,
            _ => return Some(Err(err(ERR_PARAMS, "params must be an object", json!({ "field": "params" })))),
        };
        let r = match method {
            "stt.speech_started" => self.speech_started(P(map), audio_ms),
            "stt.partial" => self.partial(P(map), audio_ms),
            "stt.final" => self.final_(P(map), audio_ms),
            "stt.speech_ended" => self.speech_ended(P(map), audio_ms),
            "stt.error" => self.error(P(map), audio_ms),
            _ => return None,
        };
        // Engines that skipped (or failed) the stt.begin handshake become active with their
        // first valid result. A session failed by a fatal stt.error stays failed.
        if r.is_ok() && method != "stt.error" && matches!(self.state, State::Pending | State::Inactive) {
            self.state = State::Active;
        }
        Some(r)
    }

    fn track(p: &P) -> Result<(), RpcError> {
        match p.str_opt("track", 64)? {
            None | Some("caller") => Ok(()),
            Some(t) => Err(err(ERR_TRACK, format!("unknown track {t:?}"), json!({ "track": t }))),
        }
    }

    fn accepted(id: &str) -> Value {
        json!({ "accepted": true, "utterance_id": id })
    }

    fn speech_started(&mut self, p: P, audio_ms: u64) -> Result<(Value, Vec<Effect>), RpcError> {
        let id = p.str_req("utterance_id", MAX_ID)?;
        let start = p.ms_req("start_ms")?;
        Self::track(&p)?;
        check_time(id, start, audio_ms)?;
        if id.is_empty() {
            return Err(bad("utterance_id", "must not be empty"));
        }
        let o = self.open_utt(id)?;
        o.start_ms.get_or_insert(start);
        let mut fx = Vec::new();
        self.maybe_barge(id, None, true, &mut fx);
        Ok((Self::accepted(id), fx))
    }

    fn partial(&mut self, p: P, audio_ms: u64) -> Result<(Value, Vec<Effect>), RpcError> {
        let id = p.str_req("utterance_id", MAX_ID)?;
        if id.is_empty() {
            return Err(bad("utterance_id", "must not be empty"));
        }
        let text = p.str_req("text", MAX_TEXT)?;
        let seq = p.ms_opt("seq")?;
        let start = p.ms_opt("start_ms")?;
        let end = p.ms_opt("end_ms")?;
        p.conf("confidence")?;
        p.str_opt("language", 64)?;
        if let Some(sc) = p.ms_opt("stable_chars")?
            && sc as usize > text.chars().count() {
                return Err(bad("stable_chars", "longer than text"));
            }
        Self::track(&p)?;
        for t in [start, end].into_iter().flatten() {
            check_time(id, t, audio_ms)?;
        }
        if let (Some(s), Some(e)) = (start, end)
            && e < s {
                return Err(err(ERR_ORDER, "end_ms before start_ms", json!({ "utterance_id": id })));
            }
        // Validate against current state before mutating anything.
        if let (Some(seq), Some(o)) = (seq, self.open.get(id))
            && o.last_seq.is_some_and(|l| seq <= l) {
                return Err(err(ERR_ORDER, "seq must increase", json!({ "utterance_id": id })));
            }
        let o = self.open_utt(id)?;
        if seq.is_some() {
            o.last_seq = seq;
        }
        if let Some(s) = start {
            o.start_ms.get_or_insert(s);
        }
        o.last_text = text.to_string();
        let mut fx = Vec::new();
        self.maybe_barge(id, Some(text), false, &mut fx);
        Ok((Self::accepted(id), fx))
    }

    fn final_(&mut self, p: P, audio_ms: u64) -> Result<(Value, Vec<Effect>), RpcError> {
        let id = p.str_req("utterance_id", MAX_ID)?;
        if id.is_empty() {
            return Err(bad("utterance_id", "must not be empty"));
        }
        let text = p.str_req("text", MAX_TEXT)?;
        let start = p.ms_req("start_ms")?;
        let end = p.ms_req("end_ms")?;
        let confidence = p.conf("confidence")?;
        let language = p.str_opt("language", 64)?;
        let end_of_turn = p.bool_opt("end_of_turn")?.unwrap_or(true);
        Self::track(&p)?;
        check_time(id, end, audio_ms)?;
        if end < start {
            return Err(err(ERR_ORDER, "end_ms before start_ms", json!({ "utterance_id": id })));
        }
        let mut words = Vec::new();
        for (i, w) in p.arr("words", MAX_WORDS)?.iter().enumerate() {
            let f = format!("words[{i}]");
            let Value::Object(wm) = w else { return Err(bad(&f, "must be an object")) };
            let wp = P(wm);
            let wt = wp.str_req("text", 1024).map_err(|_| bad(&format!("{f}.text"), "required string"))?;
            let ws = wp.ms_req("start_ms").map_err(|_| bad(&format!("{f}.start_ms"), "required integer"))?;
            let we = wp.ms_req("end_ms").map_err(|_| bad(&format!("{f}.end_ms"), "required integer"))?;
            let wc = wp.conf("confidence").map_err(|_| bad(&format!("{f}.confidence"), "must be 0..1"))?;
            if we < ws || ws < start || we > end {
                return Err(err(ERR_ORDER, format!("{f} outside the utterance span"), json!({ "utterance_id": id })));
            }
            let mut o = Map::new();
            o.insert("text".into(), json!(wt));
            o.insert("start_ms".into(), json!(ws));
            o.insert("end_ms".into(), json!(we));
            set(&mut o, "confidence", wc.map(Value::from));
            set(&mut o, "speaker", wm.get("speaker").filter(|s| s.is_string() || s.is_u64()).cloned());
            words.push(Value::Object(o));
        }
        let mut alts = Vec::new();
        for (i, a) in p.arr("alternatives", MAX_ALTS)?.iter().enumerate() {
            let f = format!("alternatives[{i}]");
            let Value::Object(am) = a else { return Err(bad(&f, "must be an object")) };
            let ap = P(am);
            let at = ap.str_req("text", MAX_TEXT).map_err(|_| bad(&format!("{f}.text"), "required string"))?;
            let ac = ap.conf("confidence").map_err(|_| bad(&format!("{f}.confidence"), "must be 0..1"))?;
            let mut o = Map::new();
            o.insert("text".into(), json!(at));
            set(&mut o, "confidence", ac.map(Value::from));
            alts.push(Value::Object(o));
        }

        self.open_utt(id)?; // finalized / limit checks
        let mut fx = Vec::new();
        self.maybe_barge(id, Some(text), false, &mut fx);
        self.open.remove(id);
        self.finalized.insert(id.to_string());

        if !text.trim().is_empty() {
            let mut rec = Map::new();
            rec.insert("utterance_id".into(), json!(id));
            rec.insert("text".into(), json!(text));
            rec.insert("start_ms".into(), json!(start));
            rec.insert("end_ms".into(), json!(end));
            set(&mut rec, "confidence", confidence.map(Value::from));
            set(&mut rec, "language", language.map(Value::from));
            rec.insert("end_of_turn".into(), json!(end_of_turn));
            if !words.is_empty() {
                rec.insert("words".into(), Value::Array(words));
            }
            if !alts.is_empty() {
                rec.insert("alternatives".into(), Value::Array(alts));
            }
            let rec = Value::Object(rec);
            self.finals.push(rec.clone());
            let mut line = rec.as_object().unwrap().clone();
            line.insert("type".into(), json!("final"));
            fx.push(Effect::Record(Value::Object(line)));
            fx.push(Effect::Final { utterance_id: id.to_string(), text: text.to_string(), end_of_turn });
        }
        Ok((Self::accepted(id), fx))
    }

    fn speech_ended(&mut self, p: P, audio_ms: u64) -> Result<(Value, Vec<Effect>), RpcError> {
        let id = p.str_req("utterance_id", MAX_ID)?;
        let end = p.ms_req("end_ms")?;
        Self::track(&p)?;
        check_time(id, end, audio_ms)?;
        if let Some(s) = self.open.get(id).and_then(|o| o.start_ms)
            && end < s {
                return Err(err(ERR_ORDER, "end_ms before start_ms", json!({ "utterance_id": id })));
            }
        // Informational: valid before or after the final; doesn't open anything.
        Ok((Self::accepted(id), Vec::new()))
    }

    fn error(&mut self, p: P, audio_ms: u64) -> Result<(Value, Vec<Effect>), RpcError> {
        let code = p.str_req("code", 128)?.to_string();
        let message = p.str_opt("message", 4096)?.unwrap_or("").to_string();
        let fatal = p.bool_opt("fatal")?.unwrap_or(false);
        if fatal {
            self.state = State::Failed;
        }
        let rec = json!({"type": "stt.error", "code": code, "message": message, "fatal": fatal, "audio_ms": audio_ms});
        Ok((json!({ "accepted": true }), vec![Effect::Record(rec), Effect::EngineError { code, message, fatal }]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &mut Session, m: &str, p: Value) -> Vec<Effect> {
        s.handle(m, &p, 10_000).unwrap().unwrap().1
    }

    fn code(s: &mut Session, m: &str, p: Value) -> i64 {
        s.handle(m, &p, 10_000).unwrap().unwrap_err().code
    }

    #[test]
    fn full_lifecycle_and_transcript() {
        let mut s = Session::new(BargeIn::Off, 2);
        assert!(ok(&mut s, "stt.speech_started", json!({"utterance_id":"u1","start_ms":100})).is_empty());
        assert_eq!(s.state, State::Active, "results activate a session without handshake");
        ok(&mut s, "stt.partial", json!({"utterance_id":"u1","seq":1,"text":"hel","stable_chars":2}));
        ok(&mut s, "stt.partial", json!({"utterance_id":"u1","seq":2,"text":"hello"}));
        let fx = ok(&mut s, "stt.final", json!({"utterance_id":"u1","text":"Hello.","start_ms":100,"end_ms":900,
            "confidence":0.9,"words":[{"text":"Hello.","start_ms":100,"end_ms":900}],"end_of_turn":false}));
        assert!(matches!(&fx[1], Effect::Final { text, end_of_turn: false, .. } if text == "Hello."));
        ok(&mut s, "stt.speech_ended", json!({"utterance_id":"u1","end_ms":950}));
        ok(&mut s, "stt.final", json!({"utterance_id":"u0","text":"First","start_ms":0,"end_ms":50}));
        ok(&mut s, "stt.final", json!({"utterance_id":"u2","text":"  ","start_ms":1000,"end_ms":1100}));
        let t = s.transcript();
        let texts: Vec<_> = t["utterances"].as_array().unwrap().iter().map(|u| u["text"].as_str().unwrap()).collect();
        assert_eq!(texts, ["First", "Hello."], "ordered by start, empty finals dropped");
        assert_eq!(t["open"], 0);
    }

    #[test]
    fn rejections_leave_state_untouched() {
        let mut s = Session::new(BargeIn::Off, 2);
        ok(&mut s, "stt.partial", json!({"utterance_id":"u1","seq":5,"text":"a"}));
        assert_eq!(code(&mut s, "stt.partial", json!({"utterance_id":"u1","seq":5,"text":"b"})), ERR_ORDER);
        assert_eq!(code(&mut s, "stt.partial", json!({"utterance_id":"u1","text":1})), ERR_PARAMS);
        assert_eq!(code(&mut s, "stt.final", json!({"utterance_id":"u1","text":"x","start_ms":5,"end_ms":4})), ERR_ORDER);
        assert_eq!(code(&mut s, "stt.final", json!({"utterance_id":"u1","text":"x","start_ms":0,"end_ms":20_000})), ERR_TIME);
        assert_eq!(code(&mut s, "stt.final", json!({"utterance_id":"u1","text":"x","start_ms":0,"end_ms":10,
            "words":[{"text":"x","start_ms":0,"end_ms":11}]})), ERR_ORDER);
        assert_eq!(code(&mut s, "stt.final", json!({"utterance_id":"u1","text":"x","start_ms":0,"end_ms":10,"track":"bot"})), ERR_TRACK);
        assert_eq!(code(&mut s, "stt.final", json!({"utterance_id":"u1","text":"x","start_ms":0,"end_ms":10,"confidence":2})), ERR_PARAMS);
        assert_eq!(s.open_count(), 1);
        ok(&mut s, "stt.partial", json!({"utterance_id":"u1","seq":6,"text":"ab"}));
        ok(&mut s, "stt.final", json!({"utterance_id":"u1","text":"abc","start_ms":0,"end_ms":10}));
        assert_eq!(code(&mut s, "stt.partial", json!({"utterance_id":"u1","text":"again"})), ERR_FINALIZED);
        assert_eq!(code(&mut s, "stt.final", json!({"utterance_id":"u1","text":"x","start_ms":0,"end_ms":10})), ERR_FINALIZED);
        assert!(s.handle("stt.begin", &json!({}), 0).is_none());
    }

    #[test]
    fn open_limit() {
        let mut s = Session::new(BargeIn::Off, 2);
        for i in 0..MAX_OPEN {
            ok(&mut s, "stt.speech_started", json!({"utterance_id": format!("u{i}"), "start_ms": i}));
        }
        assert_eq!(code(&mut s, "stt.partial", json!({"utterance_id":"x","text":"t"})), ERR_TOO_MANY);
        ok(&mut s, "stt.final", json!({"utterance_id":"u0","text":"t","start_ms":0,"end_ms":1}));
        ok(&mut s, "stt.partial", json!({"utterance_id":"x","text":"t"}));
        let ab = s.abandon_all();
        assert_eq!(ab.len(), MAX_OPEN);
        assert_eq!(ab[0]["utterance_id"], "u1");
    }

    #[test]
    fn barge_in_modes_fire_once() {
        let mut s = Session::new(BargeIn::Speech, 2);
        let fx = ok(&mut s, "stt.speech_started", json!({"utterance_id":"u1","start_ms":0}));
        assert_eq!(fx, [Effect::BargeIn { utterance_id: "u1".into() }]);
        assert!(ok(&mut s, "stt.partial", json!({"utterance_id":"u1","text":"hi"})).is_empty());

        let mut s = Session::new(BargeIn::Partial, 3);
        assert!(ok(&mut s, "stt.speech_started", json!({"utterance_id":"u1","start_ms":0})).is_empty());
        assert!(ok(&mut s, "stt.partial", json!({"utterance_id":"u1","text":"h m"})).is_empty());
        let fx = ok(&mut s, "stt.partial", json!({"utterance_id":"u1","text":"hmm"}));
        assert_eq!(fx, [Effect::BargeIn { utterance_id: "u1".into() }]);
        let fx = ok(&mut s, "stt.final", json!({"utterance_id":"u1","text":"hmm ok","start_ms":0,"end_ms":5}));
        assert!(!fx.iter().any(|e| matches!(e, Effect::BargeIn { .. })));
    }

    #[test]
    fn fatal_error_fails_session() {
        let mut s = Session::new(BargeIn::Off, 2);
        s.state = State::Active;
        let fx = ok(&mut s, "stt.error", json!({"code":"quota_exceeded","fatal":true}));
        assert_eq!(s.state, State::Failed);
        assert!(matches!(&fx[1], Effect::EngineError { fatal: true, .. }));
        ok(&mut s, "stt.final", json!({"utterance_id":"u","text":"late","start_ms":0,"end_ms":1}));
        assert_eq!(s.state, State::Failed, "a failed session doesn't come back to life");
    }
}

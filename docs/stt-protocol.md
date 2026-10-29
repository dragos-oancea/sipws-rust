# sipws speech-to-text protocol — `sipws.stt/1`

This protocol is how a speech-to-text engine connected to sipws sends
recognised text back. It runs over the same per-call WebSocket that carries
the caller's audio, as JSON-RPC 2.0 on text frames, under the `stt.` method
namespace.

```
 sipws                                              STT engine / voice app
   │ ── call.start (notification) ─────────────────────► │
   │ ── stt.begin  (request, id) ──────────────────────► │  what to recognise, how
   │ ◄───────────────────────────── result: capabilities │
   │ ══ binary audio frames (L16 or Ogg/Opus) ═════════► │
   │ ◄──────────────────────────── stt.speech_started ── │
   │ ◄─────────────────────────────────── stt.partial ── │  (0..n per utterance)
   │ ◄───────────────────────────────────── stt.final ── │  (exactly 1 per utterance)
   │ ◄────────────────────────────── stt.speech_ended ── │
   │                     … more utterances …             │
   │ ── stt.finalize (request, id) ────────────────────► │  call is ending
   │ ◄────────── remaining stt.final, then the result ── │
   │ ── call.end, close ───────────────────────────────► │
```

The key words MUST, SHOULD and MAY are used as in RFC 2119.

## 1. Design goals

* **Streaming first.** Partial hypotheses arrive while the caller speaks, and a final arrives
  as soon as the engine commits.
* **Sample-accurate timing.** Every result is placed on the audio clock of the stream sipws
  sent, so results line up with the audio, DTMF, marks and recordings, whatever the network delay.
* **An explicit utterance lifecycle.** Each utterance has an id and a strict
  `started → partial* → final` order. Mistakes are reported back to the engine instead of
  silently corrupting the transcript.
* **Nothing lost at hangup.** `stt.finalize` gives the engine a bounded grace period to flush.
* **Optional reliability.** Each result can be a notification (fire and forget) or a request
  with an `id`, which sipws acknowledges or rejects.
* **Engine-neutral.** It maps directly onto Google, Deepgram, AWS, Azure, Whisper-streaming and
  Vosk style outputs, and nothing in it is specific to one vendor.

## 2. Conventions

### 2.1 The audio clock: `*_ms`

All times are **integer milliseconds on the audio clock of the stream sipws sends** (`to_app`
in `call.start`):

* `0` is the first sample of the first audio frame of the call.
* For L16, `ms = samples_received / sample_rate * 1000`.
* For Ogg/Opus, `ms = granule_position / 48`.

sipws sends audio continuously in real time: loss, silence and hold are sent as audio too. So
the audio clock also equals the time since the call was answered. sipws puts `audio_ms` on its
own `call.dtmf`, `call.mark` and `call.barge_in` notifications, on the same clock.

### 2.2 Utterances

An **utterance** is one stretch of speech that the engine finalises as a unit (Google
"result", Deepgram `is_final` segment, Azure "phrase").

* `utterance_id`: a string of 1–128 characters chosen by the engine, unique within the call.
  A per-call counter (`"u1"`, `"u2"`, …) is fine.
* Lifecycle: `open` (after `stt.speech_started` or the first `stt.partial`), then `final`
  (after `stt.final`). An id MUST NOT be used again after its final.
* `stt.speech_started` and `stt.speech_ended` are OPTIONAL. An engine with no VAD events MAY
  send only `stt.partial`/`stt.final`, or only `stt.final`.
* At most **16** utterances may be open at once. Overlapping utterances are allowed, for
  engines that finalise out of order.

### 2.3 Tracks

`track` names whose speech it is. In version 1 the only track is `"caller"`, and it is the
default when the field is left out. The field is reserved so that a later version can also
stream the bot's own audio, for echo-aware recognition or two-party transcripts.

### 2.4 Notifications versus requests

Every app → sipws `stt.*` message MAY be sent:

* **as a notification** (no `id`). sipws processes it and replies with nothing, even if it is
  invalid. Rejections are still logged.
* **as a request** (with an `id`). sipws replies with
  `{"accepted": true, "utterance_id": "…"}` or with a JSON-RPC error (§6).

An engine SHOULD use requests while it is being developed and MAY switch to notifications in
production.

## 3. Session set-up: `stt.begin` (sipws → app, request)

sipws sends this right after `call.start`, before any audio. It says what should be
recognised.

```json
{"jsonrpc":"2.0","id":"stt-begin","method":"stt.begin","params":{
  "protocol": "sipws.stt/1",
  "call_id": "a84b4c76e66710",
  "tracks": ["caller"],
  "audio": {"encoding":"L16","sample_rate":16000,"channels":1,"endianness":"little","frame_ms":20},
  "languages": ["ro-RO","en-US"],
  "interim_results": true,
  "word_timestamps": true,
  "max_alternatives": 1,
  "hints": ["Nyquist", "factură"]
}}
```

| field | meaning |
|---|---|
| `protocol` | Always `"sipws.stt/1"`. Engines SHOULD reject a major version they don't know. |
| `audio` | Copy of `call.start.media.to_app`. |
| `languages` | BCP-47 tags in preference order; empty means auto-detect. It comes from `--stt-languages`, overridden per call by the `X-STT-Language` SIP header. |
| `interim_results` | Whether sipws wants `stt.partial`. Engines MAY send partials anyway; sipws then ignores them. |
| `word_timestamps` | Whether sipws wants `words` on finals. |
| `max_alternatives` | The highest number of `alternatives` worth sending. |
| `hints` | Phrase hints or boosted vocabulary, from `--stt-hints` and the `X-STT-Hints` header (comma separated). |

**Result** (the app accepts the session and declares what it supports):

```json
{"jsonrpc":"2.0","id":"stt-begin","result":{
  "protocol": "sipws.stt/1",
  "engine": "deepgram-nova-3",
  "languages": ["ro-RO"],
  "features": {"interim_results": true, "word_timestamps": true, "speech_events": true, "end_of_turn": true}
}}
```

Every field in the result is OPTIONAL, so `{}` is a valid acceptance.

* A **result** marks the session `active`. sipws will then run `stt.finalize` at hangup.
* An **error** (for example `-32601` method not found, from an app that does no STT) or no
  reply leaves the session `inactive`.
* If an `inactive` session receives a valid `stt.*` result anyway, it becomes `active`.
  Engines that skip the handshake still work; they just don't get the finalize grace period
  unless they answered `stt.begin`.

## 4. Results (app → sipws)

### 4.1 `stt.speech_started`

The engine's VAD detected the start of speech.

```json
{"jsonrpc":"2.0","method":"stt.speech_started","params":{"utterance_id":"u7","start_ms":51230}}
```

| field | type | required |
|---|---|---|
| `utterance_id` | string | yes |
| `start_ms` | integer ≥ 0 | yes |
| `track` | string | no, default `"caller"` |

This opens the utterance, and may trigger barge-in (§7).

### 4.2 `stt.partial`

An interim hypothesis. Each partial **replaces** the previous one for the same utterance; it
does not append to it.

```json
{"jsonrpc":"2.0","method":"stt.partial","params":{
  "utterance_id":"u7","seq":3,"text":"aș vrea să plătesc fac","stable_chars":17,
  "start_ms":51230,"end_ms":52480,"confidence":0.62}}
```

| field | type | required | notes |
|---|---|---|---|
| `utterance_id` | string | yes | Opens the utterance if it isn't open yet. |
| `text` | string | yes | The full hypothesis for the utterance so far. |
| `seq` | integer ≥ 0 | no | If present, it MUST be strictly greater than the previous partial's `seq` for this utterance (error `-32011` otherwise). Use it when partials might be reordered. |
| `stable_chars` | integer | no | The length (in Unicode scalar values) of the prefix of `text` that will not change. UIs can render it differently. |
| `start_ms`, `end_ms` | integer | no | The audio span covered so far. |
| `confidence` | number 0–1 | no | |
| `language` | string | no | The detected language. |
| `track` | string | no | |

### 4.3 `stt.final`

The committed result for an utterance. It closes the utterance, and exactly one MUST be sent
per utterance.

```json
{"jsonrpc":"2.0","id":41,"method":"stt.final","params":{
  "utterance_id":"u7",
  "text":"Aș vrea să plătesc factura.",
  "start_ms":51230,"end_ms":53110,
  "confidence":0.93,
  "language":"ro-RO",
  "end_of_turn": true,
  "words":[
    {"text":"Aș","start_ms":51230,"end_ms":51390,"confidence":0.97},
    {"text":"vrea","start_ms":51390,"end_ms":51620,"confidence":0.95},
    {"text":"să","start_ms":51620,"end_ms":51700},
    {"text":"plătesc","start_ms":51700,"end_ms":52260},
    {"text":"factura.","start_ms":52260,"end_ms":53110}
  ],
  "alternatives":[{"text":"Aș vrea să plătesc factura","confidence":0.71}]
}}
← {"jsonrpc":"2.0","id":41,"result":{"accepted":true,"utterance_id":"u7"}}
```

| field | type | required | notes |
|---|---|---|---|
| `utterance_id` | string | yes | It need not have been opened first. |
| `text` | string | yes | It MAY be `""` when the engine decides the sound wasn't speech. Empty finals close the utterance but are left out of the transcript. |
| `start_ms`, `end_ms` | integer | yes | `0 ≤ start_ms ≤ end_ms ≤ audio sent + 2000`, otherwise `-32012`. |
| `confidence` | number 0–1 | no | |
| `language` | string | no | |
| `end_of_turn` | boolean | no, default `true` | `false` means the engine committed a segment, but the caller is probably still talking. Voice agents SHOULD wait for a final with `end_of_turn: true` before answering. |
| `words` | array | no | `{text, start_ms, end_ms, confidence?, speaker?}`, at most 2000 entries, inside `[start_ms, end_ms]`. |
| `alternatives` | array | no | `{text, confidence?}`, at most 10, best first. They don't include the primary `text`. |
| `track` | string | no | |

### 4.4 `stt.speech_ended`

The engine's VAD detected the end of speech. It is optional, and it may come before or after
the final.

```json
{"jsonrpc":"2.0","method":"stt.speech_ended","params":{"utterance_id":"u7","end_ms":53180}}
```

### 4.5 `stt.error`

The engine reports a problem of its own: a quota, a vendor outage, an unsupported language.

```json
{"jsonrpc":"2.0","method":"stt.error","params":{"code":"upstream_unavailable","message":"vendor timeout","fatal":true}}
```

| field | type | required |
|---|---|---|
| `code` | string | yes. Suggested values: `unsupported_language`, `unsupported_audio`, `quota_exceeded`, `upstream_unavailable`, `internal` |
| `message` | string | no |
| `fatal` | boolean | no, default `false`. If `true`, no further results will come for this call, and sipws marks the session `failed` (it won't wait at finalize). |

sipws logs the error and writes it to the transcript. It does not hang up: whether a call can
go on without STT is the application's decision.

## 5. Teardown: `stt.finalize` (sipws → app, request)

When a call ends for any reason other than the WebSocket closing, and the session is `active`,
sipws stops sending audio and sends:

```json
{"jsonrpc":"2.0","id":"stt-finalize","method":"stt.finalize","params":{"reason":"remote_hangup","audio_ms":184220}}
```

The engine SHOULD then:

1. finalise every open utterance, sending `stt.final` for each;
2. reply with a result. All fields are optional; `{"utterances": n}` is a useful count.

sipws keeps processing `stt.*` messages until it gets the reply, or until `--stt-finalize-ms`
runs out (1500 ms by default; 0 disables the wait). Then it ends the Ogg stream (if one is in
use), sends `call.end`, and closes the WebSocket. Utterances still open at that point are
written to the transcript as `abandoned`, with their last partial.

## 6. Errors (sipws → app)

These replies are only sent for requests, meaning messages that have an `id`.

| code | meaning | `error.data` |
|---|---|---|
| `-32602` | Invalid params: a missing or ill-typed field, a value out of range, or a limit exceeded (text over 64 KiB, too many words or alternatives) | `{"field": "…"}` |
| `-32010` | The utterance was already finalised | `{"utterance_id": "…"}` |
| `-32011` | Out of order: `seq` not increasing, `end_ms < start_ms`, or a word outside the utterance span | `{"utterance_id": "…"}` |
| `-32012` | A timestamp is past the audio actually sent (2 s slack) | `{"utterance_id": "…", "audio_ms": n}` |
| `-32013` | Too many open utterances (16) | `{"utterance_id": "…"}` |
| `-32014` | Unknown track | `{"track": "…"}` |

A rejected message has no effect, so the engine can fix it and send it again.

## 7. Barge-in

When the caller starts talking over the bot's audio, sipws can cut that audio itself, without
the round trip through the app. This is set with `--barge-in`:

| value | trigger |
|---|---|
| `off` (default) | never |
| `speech` | `stt.speech_started`, or the first partial or final with non-empty text |
| `partial` | the first `stt.partial` or `stt.final` with at least `--barge-in-min-chars` (default 2) non-space characters. This is more robust against coughs and noise. |

Barge-in fires at most once per utterance, and only when playout audio is actually queued. It
works like `call.clear`: pending marks are reported as `cleared:true`, and then:

```json
{"jsonrpc":"2.0","method":"call.barge_in","params":{"utterance_id":"u7","audio_ms":51290,"cleared_samples":38400,"cleared_marks":["greeting-done"]}}
```

If the app does its own turn-taking, it should leave this `off` and call `call.clear`
itself.

## 8. Reading the transcript back

* **`call.transcript`** (app → sipws request, no params) returns
  `{"utterances":[{utterance_id, text, start_ms, end_ms, confidence?, language?, end_of_turn, words?, alternatives?}], "open": n}`,
  meaning every non-empty final so far, in order of `start_ms`.
* **`--transcript-dir DIR`** makes sipws write `DIR/<call_id>.jsonl`, one JSON object per line:

```json
{"type":"call","call_id":"a84b…","from":"sip:alice@…","to":"sip:bot@…","answered_at":"2026-10-29T15:59:01.120Z","languages":["ro-RO"]}
{"type":"stt.begin","engine":"deepgram-nova-3","features":{…}}
{"type":"final","utterance_id":"u1","text":"Bună ziua.","start_ms":820,"end_ms":1610,"confidence":0.98,"end_of_turn":true}
{"type":"dtmf","digit":"5","audio_ms":4200}
{"type":"stt.error","code":"upstream_unavailable","message":"…","fatal":false,"audio_ms":9000}
{"type":"abandoned","utterance_id":"u9","text":"și încă","start_ms":180100}
{"type":"end","reason":"remote_hangup","audio_ms":184220,"ended_at":"2026-10-29T16:02:05.340Z"}
```

  The file is written as results come in, so it survives a crash of sipws or of the engine.

## 9. Mapping from common engines

| engine output | message |
|---|---|
| Google `SpeechEventType.SPEECH_ACTIVITY_BEGIN` | `stt.speech_started` |
| Google result `is_final=false` / `true` | `stt.partial` / `stt.final` (`result_end_offset` gives `end_ms`) |
| Deepgram `SpeechStarted` | `stt.speech_started` |
| Deepgram `Results` with `is_final=false` / `true` | `stt.partial` / `stt.final` with `end_of_turn = speech_final` |
| Deepgram `UtteranceEnd` | `stt.speech_ended` |
| AWS Transcribe `IsPartial` true / false | `stt.partial` / `stt.final` (one `ResultId` per `utterance_id`) |
| Azure `Recognizing` / `Recognized` | `stt.partial` / `stt.final` |
| Whisper-streaming committed / buffer text | `stt.final` with `end_of_turn:false` / `stt.partial` |

Vendor offsets count from the start of the audio *you* streamed to the vendor. If you forward
sipws audio byte-for-byte from the first frame, they are already on the sipws audio clock.
Otherwise, add the `*_ms` of the first frame you forwarded.

## 10. Versioning

`protocol` is `sipws.stt/<major>`. New optional fields and new `stt.*` notifications can be
added within a major version, so receivers MUST ignore fields they don't understand. Anything
that changes the meaning of an existing field, or makes a new field required, gets a new major
version.

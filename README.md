# sipws

A SIP/RTP media server in Rust that bridges every inbound call to a WebSocket.
The SIP leg speaks Opus, PCMU or PCMA over RTP through an adaptive jitter
buffer; the WebSocket leg carries JSON-RPC 2.0 control messages on text frames
and audio on binary frames, in both directions, as either **raw L16 mono PCM**
or an **Ogg/Opus stream** (RFC 7845). Speech-to-text results come back on the same
WebSocket through the JSON-RPC protocol [`sipws.stt/1`](docs/stt-protocol.md).

```
 SIP trunk / PBX / softphone                       your app (bot, ASR/TTS, recorder…)
 ───────────────────────────        sipws          ───────────────────────────────────
  INVITE/ACK/BYE  (UDP 5060)  ◄──►  SIP UAS
  RTP opus|PCMU|PCMA + DTMF   ◄──►  jitter buffer ◄──► ws(s)://…  text:   JSON-RPC 2.0
                                    decode/encode       binary: L16 frames or Ogg/Opus pages
```

## Build & run

```sh
cargo build --release          # libopus is compiled from source (needs cmake + a C compiler)

./target/release/sipws \
  --ws-url 'wss://bot.example.com/media/{call_id}?to={to_user}' \
  --public-ip 203.0.113.10 \
  --ws-sample-rate 16000
```

Try it with a softphone and the bundled echo app:

```sh
cargo run --example echo_app -- 127.0.0.1:8080
cargo run -- --ws-url 'ws://127.0.0.1:8080/{call_id}' --sip-bind 0.0.0.0:5060
# call sip:anything@<host>:5060 — you hear yourself; * clears playback, # hangs up, 1 sends DTMF 123
```

All options also work as environment variables (`SIPWS_*`), see `sipws --help`.

| Option | Default | Notes |
|---|---|---|
| `--sip-bind` | `0.0.0.0:5060` | SIP over UDP |
| `--public-ip` | auto-detected | Advertised in SDP `c=` and `Contact` |
| `--rtp-bind-ip`, `--rtp-port-min/max` | `0.0.0.0`, 20000–30000 | One even port per call |
| `--codecs` | `opus,pcmu,pcma` | Preference order when answering |
| `--ptime` | 20 | 10/20/40/60 ms |
| `--ws-url` | — | `{call_id}`, `{from_user}`, `{to_user}` are substituted (URL-encoded) |
| `--ws-header` | — | Repeatable, e.g. `--ws-header 'Authorization: Bearer xyz'` |
| `--ws-sample-rate` | 16000 | 8000/16000/24000/48000 |
| `--ws-frame-ms` | 20 | Size of each binary frame sent to the app (Ogg: audio per page) |
| `--ws-out-format` | `l16` | Call audio to the app: `l16` or `ogg-opus` |
| `--ws-in-format` | `l16` | Audio from the app: `l16` or `ogg-opus` |
| `--ogg-passthrough` | true | Opus calls: copy RTP Opus packets into Ogg untouched |
| `--ogg-bitrate` | 32000 | When sipws encodes the Ogg/Opus stream itself |
| `--jitter-target-ms` | 60 | Buffer this much before playout (and again after an underrun) |
| `--jitter-max-ms` | 500 | Hard cap; oldest audio is dropped beyond it |
| `--ws-endian` | `le` | `be` gives strict RFC 3551 L16 byte order |
| `--max-playout-ms` | 120000 | Cap on audio queued towards the caller |
| `--rtp-timeout-secs` | 60 | Hang up on RTP silence (0 disables) |
| `--symmetric-rtp` | true | Latch RTP to the source address (NAT) |
| `--forward-headers` | `X-,P-Asserted-Identity,Diversion` | INVITE headers copied into `call.start` |
| `--max-calls` | 500 | Beyond this, `486 Busy Here` |
| `--stt` | true | Send the `stt.begin` handshake on every call |
| `--stt-languages` | — | BCP-47 list, e.g. `ro-RO,en-US`; per call: `X-STT-Language` header |
| `--stt-hints` | — | Phrase hints; per call: `X-STT-Hints` header |
| `--stt-interim`, `--stt-words`, `--stt-max-alternatives` | true, false, 1 | What `stt.begin` asks the engine for |
| `--stt-finalize-ms` | 1500 | Grace period for the engine at hangup (0 = none) |
| `--barge-in` | `off` | `speech` or `partial`: cut the bot's audio when the caller talks |
| `--barge-in-min-chars` | 2 | Minimum non-space characters for `--barge-in partial` |
| `--transcript-dir` | — | Write `<call_id>.jsonl` transcripts here |

## Call flow

1. INVITE arrives → SDP negotiated → `100 Trying`.
2. sipws dials `--ws-url`. If that fails within `--ws-connect-timeout-ms`, the caller gets `503`.
3. `call.start` is sent, then the call is answered with `200 OK` (retransmitted until ACK).
4. Audio flows both ways until either side hangs up. A caller BYE / CANCEL, app
   `call.hangup`, WebSocket close, RTP timeout or missing ACK all end the call;
   sipws sends BYE whenever the caller doesn't already know.
5. The SIP leg is released as soon as media stops. If an STT session is active, sipws then
   sends `stt.finalize` and waits up to `--stt-finalize-ms` for the last results before the final Ogg page,
   `call.end`, and the WebSocket close.

## WebSocket protocol

### Binary frames: L16 (`--ws-out-format l16`, `--ws-in-format l16`)

* **sipws → app:** exactly `--ws-frame-ms` of L16 mono at `--ws-sample-rate`
  (640 bytes per 20 ms at 16 kHz), one frame per playout tick. The stream is
  continuous: jitter-buffer start-up, loss, DTX and hold all appear as concealed audio or silence.
* **app → sipws:** L16 mono, same rate and byte order, **any length**. Audio is
  queued and played out in real time; send TTS as fast as you like. When the
  queue is empty the caller hears silence.

### Binary frames: Ogg/Opus (`--ws-out-format ogg-opus`, `--ws-in-format ogg-opus`)

**sipws → app** is a single Ogg logical stream for the whole call; concatenating
the binary messages gives a valid `.opus` file (ffprobe/ffmpeg read it as-is).

1. Message 1: the `OpusHead` page (BOS), mono, mapping family 0.
2. Message 2: the `OpusTags` page (vendor `sipws x.y`, comments `SIPWS_CALL_ID`, `SIPWS_FROM`, `SIPWS_TO`).
3. Then **one Ogg page per message**, each holding `--ws-frame-ms` of audio (20 ms by default → one Opus packet
   per page; raise it, e.g. to 100–200 ms, to cut page overhead at the cost of latency).
   The granule position is the running 48 kHz sample count, so the app can timestamp every page exactly.
4. The final page carries the EOS flag and is sent just before `call.end`.

How the Opus packets are produced:

* **Opus call, `--ogg-passthrough true` (default):** the caller's RTP Opus packets go into Ogg
  **byte-for-byte**, in jitter-buffer order. There's no decode/re-encode, so no quality loss and almost no CPU.
  Lost packets, start-up buffering, DTX and hold become TOC-only Opus packets (zero-length CELT
  frames, 2.5–20 ms). Decoders treat those as lost/DTX frames and conceal them, so stream time stays exactly
  aligned with call time. `pre-skip` is 0 and `input_sample_rate` is 48000.
* **G.711 call (or passthrough off):** audio is decoded, concealed (PLC/FEC) and resampled to
  `--ws-sample-rate`, then encoded with libopus (VoIP mode, `--ogg-bitrate`, 20 ms frames).
  `pre-skip` is the encoder's lookahead.

`call.start.media.to_app` tells the app which case applies: `"opus": "passthrough" | "encoded"`.

**app → sipws** accepts any Ogg/Opus bytes in any chunking (pages may be split across messages or
batched together). Pages are CRC-checked, and the reader resyncs on garbage. A new `OpusHead`/BOS starts a fresh
decoder, so the app can simply send **one complete Ogg file per utterance** (what most TTS APIs return) back to back.
`pre-skip` is honoured; audio is decoded at `--ws-sample-rate` and queued for playout like L16.

### Text frames: JSON-RPC 2.0

Notifications from sipws:

| method | params |
|---|---|
| `call.start` | `call_id, from, to, request_uri, sip_headers{}, media{to_app{…}, from_app{…}, rtp_codec, dtmf}`. `to_app` is `{encoding:"L16", sample_rate, channels, endianness, frame_ms}` or `{encoding:"ogg/opus", sample_rate:48000, channels, page_ms, opus:"passthrough"\|"encoded", serial}` |
| `call.dtmf` | `digit, duration_ms, source` (`rfc4733` or `sip_info`), `audio_ms` |
| `call.mark` | `name, cleared, audio_ms`. A mark's audio finished playing (`cleared:true` if it was dropped by `call.clear` or barge-in) |
| `call.barge_in` | `utterance_id, audio_ms, cleared_samples, cleared_marks[]`. Caller speech cut the bot's audio (`--barge-in`) |
| `call.end` | `call_id, reason, rtp{…jitter-buffer stats}`. `reason` is one of `remote_hangup`, `cancelled`, `app_hangup[:reason]`, `rtp_timeout`, `ack_timeout`, `shutdown`, `error:…` |

Methods the app can call (send with an `id` to get a response, or without for fire-and-forget):

| method | params | result |
|---|---|---|
| `call.hangup` | `reason?` | `true`; BYE is sent, then `call.end` |
| `call.clear` | — | `{cleared_samples, cleared_marks[]}`; flushes queued playback (barge-in) |
| `call.mark` | `name` | `true`; `call.mark` fires when everything queued before it has played |
| `call.dtmf` | `digits`, `duration_ms?` (default 100) | `{queued}`; RFC 4733 to the caller; error if telephone-event wasn't negotiated |
| `call.status` | — | `{call_id, codec, audio_ms, buffered_samples, pending_marks, stt:"pending"\|"active"\|"inactive"\|"failed", rtp{received, played, lost, fec_available, late, duplicate, underruns, overflow_dropped, resets, depth_ms}}` |
| `call.transcript` | — | `{utterances[…finals by start_ms], open}` |
| `ping` | — | `"pong"` |

Errors use the standard codes (`-32700` parse, `-32601` unknown method, `-32602` bad params, `-32000` call-level).

`audio_ms` is the **audio clock**: milliseconds of audio sipws has sent to the app so far. Speech-to-text
results use the same clock.

### Speech-to-text: `sipws.stt/1`

Recognised text comes back over the same WebSocket as JSON-RPC. **The full spec is in
[docs/stt-protocol.md](docs/stt-protocol.md).** In short:

* **Handshake.** sipws → `stt.begin` (a request carrying audio format, languages, hints and wanted
  features). The engine answers with its capabilities. An error or no answer means the app doesn't do STT.
* **Results.** Engine → sipws, each sent as a notification or as a request (requests are acked or rejected):
  * `stt.speech_started {utterance_id, start_ms}`
  * `stt.partial {utterance_id, seq?, text, stable_chars?, …}`. Each partial replaces the previous hypothesis.
  * `stt.final {utterance_id, text, start_ms, end_ms, confidence?, language?, end_of_turn, words?, alternatives?}`. Exactly one per utterance.
  * `stt.speech_ended {utterance_id, end_ms}`
  * `stt.error {code, message, fatal}`
* **Validation.** sipws enforces the utterance lifecycle and limits. Rejections carry their own codes: `-32010` already final,
  `-32011` out of order, `-32012` timestamp past the audio sent, `-32013` too many open, `-32014` unknown track.
* **Teardown.** sipws → `stt.finalize`. The engine flushes its open utterances and answers.
* **Barge-in.** `--barge-in speech|partial` cuts queued bot audio at the caller's first speech, without a round trip.
* **Transcript.** `call.transcript` returns the finals so far. `--transcript-dir` writes JSONL with call metadata,
  finals, DTMF, engine errors, abandoned utterances and the end reason.

```json
← {"jsonrpc":"2.0","id":"stt-begin","method":"stt.begin","params":{"protocol":"sipws.stt/1","call_id":"a84b4c76","tracks":["caller"],"audio":{…},"languages":["ro-RO"],"interim_results":true,"word_timestamps":false,"max_alternatives":1,"hints":[]}}
→ {"jsonrpc":"2.0","id":"stt-begin","result":{"engine":"deepgram-nova-3"}}
→ {"jsonrpc":"2.0","method":"stt.speech_started","params":{"utterance_id":"u1","start_ms":820}}
← {"jsonrpc":"2.0","method":"call.barge_in","params":{"utterance_id":"u1","audio_ms":840,"cleared_samples":22400,"cleared_marks":["greeting"]}}
→ {"jsonrpc":"2.0","method":"stt.partial","params":{"utterance_id":"u1","seq":1,"text":"bună"}}
→ {"jsonrpc":"2.0","id":5,"method":"stt.final","params":{"utterance_id":"u1","text":"Bună ziua.","start_ms":820,"end_ms":1610,"confidence":0.98,"end_of_turn":true}}
← {"jsonrpc":"2.0","id":5,"result":{"accepted":true,"utterance_id":"u1"}}
```

`cargo run --example stt_mock` is a VAD-based mock engine that speaks the protocol (it detects speech but doesn't transcribe it), for trying barge-in
and transcripts with a softphone.

Example session:

```json
← {"jsonrpc":"2.0","method":"call.start","params":{"call_id":"a84b4c76","from":"sip:alice@example.com","to":"sip:bot@10.0.0.1","request_uri":"sip:bot@10.0.0.1","sip_headers":{"X-Tenant":"acme"},"media":{"to_app":{"encoding":"L16","sample_rate":16000,"channels":1,"endianness":"little","frame_ms":20},"from_app":{"encoding":"L16","sample_rate":16000,"channels":1,"endianness":"little"},"rtp_codec":"opus","dtmf":true}}}
← (binary 640 bytes) …
→ (binary TTS audio, any size) …
→ {"jsonrpc":"2.0","id":7,"method":"call.mark","params":{"name":"greeting-done"}}
← {"jsonrpc":"2.0","id":7,"result":true}
← {"jsonrpc":"2.0","method":"call.mark","params":{"name":"greeting-done","cleared":false}}
→ {"jsonrpc":"2.0","id":8,"method":"call.hangup","params":{"reason":"bye"}}
← {"jsonrpc":"2.0","id":8,"result":true}
← {"jsonrpc":"2.0","method":"call.end","params":{"call_id":"a84b4c76","reason":"app_hangup:bye","rtp":{"received":4012,"played":3990,"lost":12,"fec_available":11,"late":0,"duplicate":0,"underruns":1,"overflow_dropped":0,"resets":0,"depth_ms":60}}}
```

## SIP/RTP details

* UAS over UDP: INVITE, ACK, BYE, CANCEL (→ 487), OPTIONS, INFO (`application/dtmf-relay`, `application/dtmf`), UPDATE, PRACK; others get 405.
* RFC 3581 `rport`/`received`, Record-Route honoured on BYE, response retransmission caching, 2xx retransmission with T1/T2 back-off until ACK (BYE after 32 s without ACK).
* re-INVITE/UPDATE with SDP: re-answered with the same codec, remote RTP address updated (hold/resume, media moves). Offerless re-INVITE (session refresh) is re-offered.
* SDP answer mirrors all offered m-lines (unused ones get port 0) and the direction attribute.
* Opus: RTP clock 48 kHz (RFC 7587), mono, in-band FEC on, 32 kbit/s; libopus runs natively at the WebSocket rate, so there is no resampling on that path.
* G.711: polyphase windowed-sinc resampler between 8 kHz and the WebSocket rate.
* RTP parsing handles CSRC lists, header extensions and padding. Media is sent continuously (silence when idle) and stops while the caller has us on hold (`sendonly`/`inactive`).
* An RTCP socket is bound on port+1 and drained.
* Symmetric RTP re-latches when the SSRC changes or after a re-INVITE.

### Jitter buffer

Incoming RTP goes through an adaptive jitter buffer (`src/jitter.rs`) and is released on the
ptime playout clock, not on arrival:

* It reorders by unwrapped 16-bit sequence number, drops duplicates, and drops packets that arrive after their slot was played (`late`).
* Playout starts once `--jitter-target-ms` is buffered, or the oldest packet has waited that long. After an underrun it re-buffers, so delay grows to match the network.
* **Loss:** a hole with later packets queued is concealed.
  * Opus: if the very next packet is present, the frame is rebuilt from its in-band FEC (LBRR); otherwise libopus PLC is used.
  * G.711: silence.
  * The concealed duration comes from the timestamp span, so 40/60 ms packets are handled.
* **DTX / silence suppression:** a timestamp jump with contiguous sequence numbers becomes silence of exactly that length, handed out tick by tick. When the buffer is running deep, that silence is shortened, which is how extra delay drains back to the target. Beyond `--jitter-max-ms` the oldest audio is dropped.
* **DTMF:** RFC 4733 events share the audio stream's sequence space. They occupy their slot in the buffer, so DTMF is never counted as loss.
* **Resets:** the buffer resets on SSRC change or a jump of more than 3000 sequence numbers.
* **Stats** are exposed in `call.status` and `call.end`.

### Not implemented (yet)

* SIP over TCP/TLS, SRTP (`RTP/SAVP` offers are rejected with 488), digest auth, REGISTER.
* Delayed-offer INVITEs (no SDP in INVITE) → 488.
* Outbound calls; codec change on re-INVITE; time-stretching (the jitter buffer adapts delay only at underruns, during DTX/silence, and at the hard cap).
* Sending RTCP reports.

## Tests

```sh
cargo test
```

Unit tests cover:

* the SIP parser, SDP negotiation, G.711, the resampler and RTP/DTMF;
* the jitter buffer: reorder, loss + FEC hints, multi-packet holes, DTX, wraparound, SSRC reset, overflow and telephone-event slots;
* the STT session: lifecycle, every validation rule and error code, the open-utterance limit, barge-in modes and fatal errors;
* the Ogg muxer/demuxer: lacing, page splitting, chunked input, CRC resync and chained streams, plus reading an Ogg/Opus file produced by ffmpeg/libopus.

`tests/e2e.rs` starts the real binary and drives full calls with a scripted SIP UAC and an in-process
WebSocket echo app. Every call runs INVITE → 200 → ACK, gets the caller's tone echoed back over RTP, fires `call.mark`, and ends with
`call.hangup` producing a BYE. The variants are:

* PCMU and Opus with L16 at 16 and 48 kHz. RFC 4733 DTMF must not register as loss.
* An impaired network (10% loss, reordering, duplicates). Measured: lost = exactly the dropped packets, late = 0, and the duplicates and FEC are detected.
* Ogg/Opus both ways, in passthrough (impaired Opus call) and encoded (PCMU call). The app echoes the Ogg bytes back. The stream is checked with our demuxer (CRC, page per message, granule = sample count). When installed, **ffprobe/ffmpeg** must decode it cleanly with the expected duration.
* The STT protocol, with a scripted engine and bot:
  * the handshake and language hints;
  * barge-in cutting 3 s of TTS (the caller hears it stop), with its mark reported as cleared;
  * partials, a final with words, and invalid results rejected with the right codes;
  * `call.transcript`;
  * `stt.finalize` delivering the last utterance before `call.end`;
  * the JSONL transcript file contents.
* 503 when the WebSocket is unreachable, and 488 for a codec mismatch.

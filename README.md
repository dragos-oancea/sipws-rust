# sipws

A SIP/RTP media server in Rust that bridges every inbound call to a WebSocket.
The SIP leg speaks Opus, PCMU or PCMA over RTP; the WebSocket leg carries
JSON-RPC 2.0 control messages on text frames and **raw L16 mono PCM** on
binary frames, in both directions.

```
 SIP trunk / PBX / softphone                       your app (bot, ASR/TTS, recorder…)
 ───────────────────────────        sipws          ───────────────────────────────────
  INVITE/ACK/BYE  (UDP 5060)  ◄──►  SIP UAS
  RTP opus|PCMU|PCMA + DTMF   ◄──►  decode/encode ◄──► ws(s)://…  text:   JSON-RPC 2.0
                                    resample            binary: L16 frames (both ways)
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
| `--ws-frame-ms` | 20 | Size of each binary frame sent to the app |
| `--ws-endian` | `le` | `be` gives strict RFC 3551 L16 byte order |
| `--max-playout-ms` | 120000 | Cap on audio queued towards the caller |
| `--rtp-timeout-secs` | 60 | Hang up on RTP silence (0 disables) |
| `--symmetric-rtp` | true | Latch RTP to the source address (NAT) |
| `--forward-headers` | `X-,P-Asserted-Identity,Diversion` | INVITE headers copied into `call.start` |
| `--max-calls` | 500 | Beyond this, `486 Busy Here` |

## Call flow

1. INVITE arrives → SDP negotiated → `100 Trying`.
2. sipws dials `--ws-url`. If that fails within `--ws-connect-timeout-ms`, the caller gets `503`.
3. `call.start` is sent, then the call is answered with `200 OK` (retransmitted until ACK).
4. Audio flows both ways until either side hangs up. A caller BYE / CANCEL, app
   `call.hangup`, WebSocket close, RTP timeout or missing ACK all end the call;
   sipws sends BYE whenever the caller doesn't already know.

## WebSocket protocol

### Binary frames

* **sipws → app:** exactly `--ws-frame-ms` of L16 mono at `--ws-sample-rate`
  (640 bytes per 20 ms at 16 kHz). Lost packets are concealed (Opus PLC / silence for G.711), so timing stays continuous.
* **app → sipws:** L16 mono, same rate and byte order, **any length**. Audio is
  queued and played out in real time; send TTS as fast as you like. When the
  queue is empty the caller hears silence.

### Text frames: JSON-RPC 2.0

Notifications from sipws:

| method | params |
|---|---|
| `call.start` | `call_id, from, to, request_uri, sip_headers{}, media{encoding:"L16", sample_rate, channels:1, endianness, frame_ms, rtp_codec, dtmf}` |
| `call.dtmf` | `digit, duration_ms, source` (`rfc4733` or `sip_info`) |
| `call.mark` | `name, cleared` — a mark's audio finished playing (`cleared:true` if dropped by `call.clear`) |
| `call.end` | `call_id, reason` — `remote_hangup`, `cancelled`, `app_hangup[:reason]`, `rtp_timeout`, `ack_timeout`, `shutdown`, `error:…` |

Methods the app can call (send with an `id` to get a response, or without for fire-and-forget):

| method | params | result |
|---|---|---|
| `call.hangup` | `reason?` | `true`; BYE is sent, then `call.end` |
| `call.clear` | — | `{cleared_samples, cleared_marks[]}`; flushes queued playback (barge-in) |
| `call.mark` | `name` | `true`; `call.mark` fires when everything queued before it has played |
| `call.dtmf` | `digits`, `duration_ms?` (default 100) | `{queued}`; RFC 4733 to the caller; error if telephone-event wasn't negotiated |
| `call.status` | — | `{call_id, codec, buffered_samples, pending_marks}` |
| `ping` | — | `"pong"` |

Errors use the standard codes (`-32700` parse, `-32601` unknown method, `-32602` bad params, `-32000` call-level).

Example session:

```json
← {"jsonrpc":"2.0","method":"call.start","params":{"call_id":"a84b4c76","from":"sip:alice@example.com","to":"sip:bot@10.0.0.1","request_uri":"sip:bot@10.0.0.1","sip_headers":{"X-Tenant":"acme"},"media":{"encoding":"L16","sample_rate":16000,"channels":1,"endianness":"little","frame_ms":20,"rtp_codec":"opus","dtmf":true}}}
← (binary 640 bytes) …
→ (binary TTS audio, any size) …
→ {"jsonrpc":"2.0","id":7,"method":"call.mark","params":{"name":"greeting-done"}}
← {"jsonrpc":"2.0","id":7,"result":true}
← {"jsonrpc":"2.0","method":"call.mark","params":{"name":"greeting-done","cleared":false}}
→ {"jsonrpc":"2.0","id":8,"method":"call.hangup","params":{"reason":"bye"}}
← {"jsonrpc":"2.0","id":8,"result":true}
← {"jsonrpc":"2.0","method":"call.end","params":{"call_id":"a84b4c76","reason":"app_hangup:bye"}}
```

## SIP/RTP details

* UAS over UDP: INVITE, ACK, BYE, CANCEL (→ 487), OPTIONS, INFO (`application/dtmf-relay`, `application/dtmf`), UPDATE, PRACK; others get 405.
* RFC 3581 `rport`/`received`, Record-Route honoured on BYE, response retransmission caching, 2xx retransmission with T1/T2 back-off until ACK (BYE after 32 s without ACK).
* re-INVITE/UPDATE with SDP: re-answered with the same codec, remote RTP address updated (hold/resume, media moves). Offerless re-INVITE (session refresh) is re-offered.
* SDP answer mirrors all offered m-lines (unused ones get port 0) and the direction attribute.
* Opus: RTP clock 48 kHz (RFC 7587), mono, in-band FEC on, 32 kbit/s; libopus runs natively at the WebSocket rate, so there is no resampling on that path.
* G.711: polyphase windowed-sinc resampler between 8 kHz and the WebSocket rate.
* RTP: CSRC/extension/padding parsing, SSRC change resync, late/duplicate drop, up to 5 lost packets concealed, continuous outbound stream (silence when idle), RTCP-mux packets ignored.

### Not implemented (yet)

* SIP over TCP/TLS, SRTP (`RTP/SAVP` offers are rejected with 488), digest auth, REGISTER.
* Delayed-offer INVITEs (no SDP in INVITE) → 488.
* Outbound calls; codec change on re-INVITE; adaptive jitter buffer (packets are decoded in arrival order).

## Tests

```sh
cargo test
```

Unit tests cover the SIP parser, SDP negotiation, G.711, resampler, RTP/DTMF and the WebSocket framing.
`tests/e2e.rs` starts the real binary and drives full calls with a scripted SIP UAC and an in-process
WebSocket echo app — PCMU at 16 kHz and Opus at 48 kHz: INVITE → 200 → ACK, caller tone echoed back over
RTP, RFC 4733 DTMF reported, `call.mark`, then `call.hangup` producing a BYE. It also checks 503 when the
WebSocket is unreachable and 488 for codec mismatch.

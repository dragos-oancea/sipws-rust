//! End-to-end: real sipws binary, a scripted SIP UAC + RTP endpoint, and an in-test WebSocket app.

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};
use tokio_tungstenite::tungstenite::Message;

struct Proc(Child);
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn spawn_server(sip_port: u16, ws_url: &str, rate: u32, codecs: &str, rtp_base: u16, extra: &[&str]) -> Proc {
    Proc(
        Command::new(env!("CARGO_BIN_EXE_sipws"))
            .args([
                "--sip-bind", &format!("127.0.0.1:{sip_port}"),
                "--public-ip", "127.0.0.1",
                "--rtp-bind-ip", "127.0.0.1",
                "--rtp-port-min", &rtp_base.to_string(),
                "--rtp-port-max", &(rtp_base + 100).to_string(),
                "--ws-url", ws_url,
                "--ws-sample-rate", &rate.to_string(),
                "--codecs", codecs,
                "--ws-connect-timeout-ms", "1000",
            ])
            .args(extra)
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn sipws"),
    )
}

fn ulaw(sample: i16) -> u8 {
    let mut s = sample as i32;
    let sign = if s < 0 { s = -s; 0x80 } else { 0 };
    s = s.min(32635) + 0x84;
    let mut exp = 7;
    while exp > 0 && s & (0x4000 >> (7 - exp)) == 0 {
        exp -= 1;
    }
    !((sign | (exp << 4) | ((s >> (exp + 3)) & 0x0F)) as u8)
}

fn rtp(pt: u8, marker: bool, seq: u16, ts: u32, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0x80, pt | if marker { 0x80 } else { 0 }];
    v.extend_from_slice(&seq.to_be_bytes());
    v.extend_from_slice(&ts.to_be_bytes());
    v.extend_from_slice(&0x1234_5678u32.to_be_bytes());
    v.extend_from_slice(payload);
    v
}

fn header<'a>(msg: &'a str, name: &str) -> Option<&'a str> {
    msg.lines().find_map(|l| {
        let (n, v) = l.split_once(':')?;
        n.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

async fn recv_sip(sock: &UdpSocket, want: impl Fn(&str) -> bool) -> (String, SocketAddr) {
    let mut buf = vec![0u8; 8192];
    loop {
        let (n, from) = timeout(Duration::from_secs(10), sock.recv_from(&mut buf))
            .await
            .expect("timed out waiting for SIP message")
            .unwrap();
        let s = String::from_utf8_lossy(&buf[..n]).to_string();
        if want(&s) {
            return (s, from);
        }
    }
}

#[derive(Debug, Default)]
struct AppLog {
    start: Option<Value>,
    binary_msgs: usize,
    frame_sizes: Vec<usize>,
    bytes: Vec<u8>,
    dtmf: Vec<String>,
    marks: Vec<Value>,
    results: Vec<Value>,
    end: Option<Value>,
}

/// Echo app: returns every binary message as-is, drops a mark after 25 messages, hangs up after `hangup_after`.
async fn run_app(listener: TcpListener, hangup_after: usize, done: mpsc::Sender<AppLog>) {
    let (tcp, _) = listener.accept().await.unwrap();
    let ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
    let (mut tx, mut rx) = ws.split();
    let mut log = AppLog::default();
    while let Some(Ok(m)) = rx.next().await {
        match m {
            Message::Binary(b) => {
                log.binary_msgs += 1;
                log.frame_sizes.push(b.len());
                log.bytes.extend_from_slice(&b);
                let _ = tx.send(Message::Binary(b)).await; // server may already be closing
                if log.binary_msgs == 25 {
                    let r = json!({"jsonrpc":"2.0","id":1,"method":"call.mark","params":{"name":"m1"}});
                    let _ = tx.send(Message::Text(r.to_string().into())).await;
                }
                if log.binary_msgs == hangup_after {
                    let r = json!({"jsonrpc":"2.0","id":2,"method":"call.hangup","params":{"reason":"done"}});
                    let _ = tx.send(Message::Text(r.to_string().into())).await;
                }
            }
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                match v["method"].as_str() {
                    Some("call.start") => log.start = Some(v["params"].clone()),
                    Some("call.dtmf") => log.dtmf.push(v["params"]["digit"].as_str().unwrap().to_string()),
                    Some("call.mark") => log.marks.push(v["params"].clone()),
                    Some("call.end") => log.end = Some(v["params"].clone()),
                    _ => log.results.push(v),
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    let _ = done.send(log).await;
}

fn invite(sip: SocketAddr, uac: SocketAddr, rtp_port: u16, call_id: &str, sdp_media: &str) -> String {
    let sdp = format!(
        "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio {rtp_port} RTP/AVP {sdp_media}a=ptime:20\r\na=sendrecv\r\n"
    );
    format!(
        "INVITE sip:bot@{sip} SIP/2.0\r\nVia: SIP/2.0/UDP {uac};branch=z9hG4bK{call_id};rport\r\nMax-Forwards: 70\r\n\
From: \"Alice\" <sip:alice@{uac}>;tag=uac1\r\nTo: <sip:bot@{sip}>\r\nCall-ID: {call_id}\r\nCSeq: 1 INVITE\r\n\
Contact: <sip:alice@{uac}>\r\nX-Tenant: acme\r\nContent-Type: application/sdp\r\nContent-Length: {}\r\n\r\n{sdp}",
        sdp.len()
    )
}

struct Case {
    name: &'static str,
    /// "pcmu" or "opus": what the UAC offers/sends.
    offer: &'static str,
    server_codecs: &'static str,
    ws_rate: u32,
    rtp_base: u16,
    extra: &'static [&'static str],
    ogg: bool,
    /// Drop every 10th packet, swap every 7th with its successor, duplicate every 13th.
    impair: bool,
}

struct Outcome {
    log: AppLog,
    loud_packets: usize,
}

async fn run_call(c: Case) -> Outcome {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_port = listener.local_addr().unwrap().port();
    let (done_tx, mut done_rx) = mpsc::channel(1);
    let hangup_after = if c.ogg { 102 } else { 100 };
    tokio::spawn(run_app(listener, hangup_after, done_tx));

    let sip_port = free_udp_port();
    let url = format!("ws://127.0.0.1:{ws_port}/call/{{call_id}}");
    let _srv = spawn_server(sip_port, &url, c.ws_rate, c.server_codecs, c.rtp_base, c.extra);
    sleep(Duration::from_millis(400)).await;
    let sip: SocketAddr = format!("127.0.0.1:{sip_port}").parse().unwrap();

    let uac = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let uac_addr = uac.local_addr().unwrap();
    let media = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let media_port = media.local_addr().unwrap().port();

    let call_id = format!("e2e-{}", c.name);
    let offer = match c.offer {
        "pcmu" => "0 101\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:101 telephone-event/8000\r\n",
        _ => "111 0\r\na=rtpmap:111 opus/48000/2\r\na=fmtp:111 useinbandfec=1\r\na=rtpmap:0 PCMU/8000\r\n",
    };
    uac.send_to(invite(sip, uac_addr, media_port, &call_id, offer).as_bytes(), sip).await.unwrap();

    let (ok, _) = recv_sip(&uac, |s| s.starts_with("SIP/2.0 200")).await;
    let to = header(&ok, "To").unwrap().to_string();
    assert!(to.contains("tag="), "200 OK must carry a To tag");
    let srv_rtp: u16 = ok
        .lines()
        .find_map(|l| l.strip_prefix("m=audio "))
        .and_then(|l| l.split_whitespace().next())
        .unwrap()
        .parse()
        .unwrap();
    let pt: u8 = if c.offer == "pcmu" {
        assert!(ok.contains("a=rtpmap:0 PCMU/8000"));
        0
    } else {
        assert!(ok.contains("a=rtpmap:111 opus/48000/2"));
        111
    };
    let ack = format!(
        "ACK sip:bot@{sip} SIP/2.0\r\nVia: SIP/2.0/UDP {uac_addr};branch=z9hG4bKack1\r\nFrom: \"Alice\" <sip:alice@{uac_addr}>;tag=uac1\r\n\
To: {to}\r\nCall-ID: {call_id}\r\nCSeq: 1 ACK\r\nContent-Length: 0\r\n\r\n"
    );
    uac.send_to(ack.as_bytes(), sip).await.unwrap();

    // Caller audio: 440 Hz tone, plus a '5' DTMF event (PCMU only).
    let srv_media: SocketAddr = format!("127.0.0.1:{srv_rtp}").parse().unwrap();
    let mut opus_enc = opus::Encoder::new(48000, opus::Channels::Mono, opus::Application::Voip).unwrap();
    opus_enc.set_inband_fec(true).unwrap();
    opus_enc.set_packet_loss_perc(10).unwrap();
    let mut opus_dec = opus::Decoder::new(48000, opus::Channels::Mono).unwrap();
    let mut seq = 100u16;
    let mut ts = 5000u32;
    let mut loud_packets = 0;
    let mut rx_packets = 0;
    let mut held: Option<Vec<u8>> = None;
    let mut buf = vec![0u8; 2048];
    let mut sbuf = vec![0u8; 8192];
    let start = Instant::now();
    let mut next = Instant::now();
    let mut n = 0u32;
    let bye = loop {
        if Instant::now() >= next && n < 600 {
            let payload: Vec<u8> = if pt == 0 {
                (0..160)
                    .map(|i| ulaw((((n * 160 + i) as f64 * 2.0 * std::f64::consts::PI * 440.0 / 8000.0).sin() * 12000.0) as i16))
                    .collect()
            } else {
                let pcm: Vec<i16> = (0..960)
                    .map(|i| (((n * 960 + i) as f64 * 2.0 * std::f64::consts::PI * 440.0 / 48000.0).sin() * 12000.0) as i16)
                    .collect();
                opus_enc.encode_vec(&pcm, 1500).unwrap()
            };
            let pkt = rtp(pt, n == 0, seq, ts, &payload);
            seq = seq.wrapping_add(1);
            if c.impair && n > 0 && n % 10 == 5 {
                // lost on the network
            } else if c.impair && n % 7 == 3 {
                held = Some(pkt); // arrives after the next one
            } else {
                media.send_to(&pkt, srv_media).await.unwrap();
                if let Some(h) = held.take() {
                    media.send_to(&h, srv_media).await.unwrap();
                }
                if c.impair && n.is_multiple_of(13) {
                    media.send_to(&pkt, srv_media).await.unwrap();
                }
            }
            if pt == 0 && n == 30 {
                for (i, d) in [400u16, 800, 800, 800].iter().enumerate() {
                    let end = if i > 0 { 0x80 } else { 0 };
                    let ev = [5u8, end | 10, (d >> 8) as u8, *d as u8];
                    media.send_to(&rtp(101, i == 0, seq, ts, &ev), srv_media).await.unwrap();
                    seq = seq.wrapping_add(1);
                }
            }
            ts = ts.wrapping_add(if pt == 0 { 160 } else { 960 });
            n += 1;
            next += Duration::from_millis(20);
        }
        tokio::select! {
            r = media.recv_from(&mut buf) => {
                let (len, _) = r.unwrap();
                rx_packets += 1;
                let p = &buf[12..len];
                assert_eq!(buf[1] & 0x7F, pt);
                let loud = if pt == 0 {
                    p.iter().filter(|&&b| b & 0x7F < 0x40).count() > 20
                } else {
                    let mut out = vec![0i16; 5760];
                    let k = opus_dec.decode(p, &mut out, false).unwrap();
                    out[..k].iter().map(|&s| (s as i64).abs()).max().unwrap_or(0) > 3000
                };
                if loud { loud_packets += 1; }
            }
            r = uac.recv_from(&mut sbuf) => {
                let (len, from) = r.unwrap();
                let s = String::from_utf8_lossy(&sbuf[..len]).to_string();
                if s.starts_with("BYE ") { break (s, from); }
            }
            _ = sleep(Duration::from_millis(5)) => {}
        }
        assert!(start.elapsed() < Duration::from_secs(15), "no BYE from server");
    };

    let (bye, from) = bye;
    let mut ok = String::from("SIP/2.0 200 OK\r\n");
    for name in ["Via", "From", "To", "Call-ID", "CSeq"] {
        ok += &format!("{name}: {}\r\n", header(&bye, name).unwrap());
    }
    ok += "Content-Length: 0\r\n\r\n";
    uac.send_to(ok.as_bytes(), from).await.unwrap();
    assert_eq!(header(&bye, "Call-ID"), Some(call_id.as_str()));

    let log = timeout(Duration::from_secs(5), done_rx.recv()).await.unwrap().unwrap();
    eprintln!(
        "[{}] rx_rtp={rx_packets} loud={loud_packets} ws_msgs={} marks={:?} dtmf={:?} rtp_stats={}",
        c.name, log.binary_msgs, log.marks, log.dtmf, log.end.as_ref().map(|e| e["rtp"].clone()).unwrap_or_default()
    );

    let start = log.start.as_ref().expect("call.start");
    assert_eq!(start["call_id"], call_id);
    assert_eq!(start["sip_headers"]["X-Tenant"], "acme");
    assert!(log.results.iter().any(|r| r["id"] == 1 && r["result"] == true), "{:?}", log.results);
    assert!(log.results.iter().any(|r| r["id"] == 2 && r["result"] == true));
    assert_eq!(log.marks.first().map(|m| m["name"].clone()), Some(json!("m1")));
    assert_eq!(log.end.as_ref().expect("call.end")["reason"], "app_hangup:done");
    assert!(rx_packets > 50, "server must stream RTP");
    if pt == 0 {
        assert_eq!(log.dtmf, vec!["5"]);
    }
    Outcome { log, loud_packets }
}

fn check_l16(o: &Outcome, ws_rate: u32) {
    let media = &o.log.start.as_ref().unwrap()["media"]["to_app"];
    assert_eq!(media["encoding"], "L16");
    assert_eq!(media["sample_rate"], ws_rate);
    let frame_bytes = (ws_rate / 50 * 2) as usize;
    assert!(o.log.frame_sizes.iter().all(|&s| s == frame_bytes), "frames must be 20 ms of L16");
    assert!(o.loud_packets > 20, "echoed caller audio must come back over RTP ({})", o.loud_packets);
}

/// Validates the Ogg/Opus stream the app received with our demuxer and, when installed, ffmpeg.
fn check_ogg(o: &Outcome, name: &str) {
    let media = &o.log.start.as_ref().unwrap()["media"]["to_app"];
    assert_eq!(media["encoding"], "ogg/opus");
    let bytes = &o.log.bytes;
    assert!(bytes.starts_with(b"OggS"));

    let mut d = sipws::ogg::OggDemuxer::default();
    let mut pkts = Vec::new();
    d.feed(bytes, &mut pkts);
    assert_eq!(d.crc_errors, 0);
    assert!(pkts[0].bos && pkts[0].data.starts_with(b"OpusHead"));
    assert!(pkts[1].data.starts_with(b"OpusTags"));
    // Every binary message is exactly one whole page.
    let pages = o.log.frame_sizes.len();
    assert_eq!(bytes.windows(4).filter(|w| *w == b"OggS").count(), pages);
    // Granule positions must be the running 48 kHz sample count.
    let total: u64 = pkts[2..].iter().map(|p| sipws::ogg::opus_packet_dur(&p.data).unwrap() as u64).sum();
    assert_eq!(pkts.last().unwrap().granule, total);

    let mut reader = sipws::ogg::OggOpusReader::new(48000);
    let mut pcm = Vec::new();
    reader.feed(bytes, &mut pcm);
    assert_eq!(reader.errors, 0);
    let secs = pcm.len() as f64 / 48000.0;
    let loud = pcm.chunks(960).filter(|c| c.iter().map(|s| s.unsigned_abs()).max().unwrap_or(0) > 3000).count();
    eprintln!("[{name}] ogg: {} pages, {secs:.2}s audio, {loud} loud 20ms blocks", pages);
    assert!(loud > 50, "caller tone must be in the Ogg stream");
    assert!(o.loud_packets > 20, "Ogg echoed back must be played to the caller ({})", o.loud_packets);

    if Command::new("ffprobe").arg("-version").output().is_ok() {
        let path = std::env::temp_dir().join(format!("sipws-{name}.opus"));
        std::fs::write(&path, bytes).unwrap();
        let probe = Command::new("ffprobe")
            .args(["-v", "error", "-show_entries", "stream=codec_name,channels:format=duration", "-of", "default=nw=1"])
            .arg(&path)
            .output()
            .unwrap();
        let info = String::from_utf8_lossy(&probe.stdout);
        eprintln!("[{name}] ffprobe: {}", info.replace('\n', " "));
        assert!(info.contains("codec_name=opus") && info.contains("channels=1"), "{info}");
        let dur: f64 = info.lines().find_map(|l| l.strip_prefix("duration=")).unwrap().parse().unwrap();
        assert!((dur - secs).abs() < 0.1, "ffprobe {dur} vs decoded {secs}");
        let decode = Command::new("ffmpeg").args(["-v", "error", "-i"]).arg(&path).args(["-f", "null", "-"]).output().unwrap();
        assert!(decode.status.success() && decode.stderr.is_empty(), "ffmpeg: {}", String::from_utf8_lossy(&decode.stderr));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pcmu_call_with_16k_l16() {
    let o = run_call(Case { name: "pcmu-l16", offer: "pcmu", server_codecs: "pcmu", ws_rate: 16000, rtp_base: 21000, extra: &[], ogg: false, impair: false }).await;
    check_l16(&o, 16000);
    let s = &o.log.end.as_ref().unwrap()["rtp"];
    assert_eq!((s["lost"].as_u64(), s["late"].as_u64()), (Some(0), Some(0)), "DTMF must not count as loss: {s}");
}

#[tokio::test(flavor = "multi_thread")]
async fn opus_call_with_48k_l16() {
    let o = run_call(Case { name: "opus-l16", offer: "opus", server_codecs: "opus,pcmu", ws_rate: 48000, rtp_base: 22000, extra: &[], ogg: false, impair: false }).await;
    check_l16(&o, 48000);
}

#[tokio::test(flavor = "multi_thread")]
async fn jitter_buffer_handles_loss_reorder_duplicates() {
    let o = run_call(Case { name: "pcmu-impaired", offer: "pcmu", server_codecs: "pcmu", ws_rate: 16000, rtp_base: 25000, extra: &[], ogg: false, impair: true }).await;
    check_l16(&o, 16000);
    let s = &o.log.end.as_ref().unwrap()["rtp"];
    assert!(s["lost"].as_u64().unwrap() >= 5, "{s}");
    assert!(s["duplicate"].as_u64().unwrap() >= 1, "{s}");
    assert_eq!(s["late"], 0, "1-packet reordering must be absorbed by the buffer: {s}");
}

#[tokio::test(flavor = "multi_thread")]
async fn opus_impaired_uses_fec() {
    let o = run_call(Case { name: "opus-impaired", offer: "opus", server_codecs: "opus", ws_rate: 16000, rtp_base: 26000, extra: &[], ogg: false, impair: true }).await;
    check_l16(&o, 16000);
    let s = &o.log.end.as_ref().unwrap()["rtp"];
    assert!(s["fec_available"].as_u64().unwrap() >= 5, "{s}");
    assert_eq!(s["late"], 0, "{s}");
}

#[tokio::test(flavor = "multi_thread")]
async fn opus_call_ogg_passthrough_both_ways() {
    let o = run_call(Case {
        name: "opus-ogg-pass", offer: "opus", server_codecs: "opus", ws_rate: 16000, rtp_base: 27000,
        extra: &["--ws-out-format", "ogg-opus", "--ws-in-format", "ogg-opus"], ogg: true, impair: true,
    }).await;
    assert_eq!(o.log.start.as_ref().unwrap()["media"]["to_app"]["opus"], "passthrough");
    check_ogg(&o, "opus-ogg-pass");
}

#[tokio::test(flavor = "multi_thread")]
async fn pcmu_call_ogg_encoded_both_ways() {
    let o = run_call(Case {
        name: "pcmu-ogg-enc", offer: "pcmu", server_codecs: "pcmu", ws_rate: 16000, rtp_base: 28000,
        extra: &["--ws-out-format", "ogg-opus", "--ws-in-format", "ogg-opus"], ogg: true, impair: false,
    }).await;
    assert_eq!(o.log.start.as_ref().unwrap()["media"]["to_app"]["opus"], "encoded");
    check_ogg(&o, "pcmu-ogg-enc");
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_websocket_rejects_with_503() {
    let sip_port = free_udp_port();
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    let _srv = spawn_server(sip_port, &format!("ws://127.0.0.1:{dead}/"), 8000, "pcmu", 23000, &[]);
    sleep(Duration::from_millis(400)).await;
    let sip: SocketAddr = format!("127.0.0.1:{sip_port}").parse().unwrap();
    let uac = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let a = uac.local_addr().unwrap();
    uac.send_to(invite(sip, a, 4000, "dead-ws", "0\r\n").as_bytes(), sip).await.unwrap();
    let (resp, _) = recv_sip(&uac, |s| s.starts_with("SIP/2.0") && !s.starts_with("SIP/2.0 100")).await;
    assert!(resp.starts_with("SIP/2.0 503"), "{resp}");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_common_codec_gets_488() {
    let sip_port = free_udp_port();
    let _srv = spawn_server(sip_port, "ws://127.0.0.1:1/", 8000, "opus", 24000, &[]);
    sleep(Duration::from_millis(400)).await;
    let sip: SocketAddr = format!("127.0.0.1:{sip_port}").parse().unwrap();
    let uac = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let a = uac.local_addr().unwrap();
    uac.send_to(invite(sip, a, 4000, "g722-only", "9\r\n").as_bytes(), sip).await.unwrap();
    let (resp, _) = recv_sip(&uac, |s| s.starts_with("SIP/2.0")).await;
    assert!(resp.starts_with("SIP/2.0 488"), "{resp}");
}

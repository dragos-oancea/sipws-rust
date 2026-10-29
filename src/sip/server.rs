//! UDP SIP user agent server: transactions (the parts a UAS needs), dialogs, BYE.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::message::{addr_uri, param, uri_hostport, SipMessage};
use super::sdp::{build_answer, Sdp};
use crate::call::{self, MediaCtl};
use crate::codec::CodecKind;
use crate::config::Config;
use crate::rtp::PortPool;

const T1: Duration = Duration::from_millis(500);
const T2: Duration = Duration::from_secs(4);
const TIMER_H: Duration = Duration::from_secs(32);
const ALLOW: &str = "INVITE, ACK, BYE, CANCEL, OPTIONS, INFO, UPDATE";
const USER_AGENT: &str = concat!("sipws/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, PartialEq)]
pub enum EndReason {
    RemoteBye,
    Cancelled,
    AckTimeout,
    AppHangup(String),
    WsClosed,
    RtpTimeout,
    Shutdown,
    Error(String),
}

impl EndReason {
    pub fn as_str(&self) -> String {
        match self {
            EndReason::RemoteBye => "remote_hangup".into(),
            EndReason::Cancelled => "cancelled".into(),
            EndReason::AckTimeout => "ack_timeout".into(),
            EndReason::AppHangup(r) if r.is_empty() => "app_hangup".into(),
            EndReason::AppHangup(r) => format!("app_hangup:{r}"),
            EndReason::WsClosed => "ws_closed".into(),
            EndReason::RtpTimeout => "rtp_timeout".into(),
            EndReason::Shutdown => "shutdown".into(),
            EndReason::Error(e) => format!("error:{e}"),
        }
    }

    /// Whether we have to send a BYE (i.e. the far end doesn't know yet).
    fn needs_bye(&self) -> bool {
        !matches!(self, EndReason::RemoteBye | EndReason::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum InviteState {
    Proceeding,
    Answered,
    Terminated,
}

struct DialogInner {
    state: InviteState,
    /// Last response to the most recent INVITE (for retransmissions).
    last_invite_resp: Vec<u8>,
    last_invite_cseq: u32,
    acked_cseq: u32,
    sdp_version: u64,
    local_cseq: u32,
    end_reason: Option<EndReason>,
}

pub struct Dialog {
    pub call_id: String,
    pub local_tag: String,
    pub invite: SipMessage,
    pub src: SocketAddr,
    pub rtp_port: u16,
    pub codec: CodecKind,
    pub session_id: u64,
    pub cancel: CancellationToken,
    media_tx: mpsc::UnboundedSender<MediaCtl>,
    inner: Mutex<DialogInner>,
}

impl Dialog {
    /// Records why the call ends (first reason wins) and wakes the call task.
    pub fn terminate(&self, reason: EndReason) {
        let mut g = self.inner.lock().unwrap();
        if g.end_reason.is_none() {
            g.end_reason = Some(reason);
        }
        drop(g);
        self.cancel.cancel();
    }

    pub fn end_reason(&self) -> Option<EndReason> {
        self.inner.lock().unwrap().end_reason.clone()
    }
}

pub struct SipServer {
    sock: Arc<UdpSocket>,
    pub cfg: Arc<Config>,
    codecs: Vec<CodecKind>,
    ports: PortPool,
    advertised: SocketAddr,
    dialogs: Mutex<HashMap<String, Arc<Dialog>>>,
    client_tx: Mutex<HashMap<String, oneshot::Sender<u16>>>,
    /// Final responses of server transactions without a dialog, by branch, for retransmits.
    resp_cache: Mutex<HashMap<String, (Instant, Vec<u8>)>>,
}

fn rand_hex(n: usize) -> String {
    (0..n).map(|_| format!("{:x}", rand::random_range(0u8..16))).collect()
}

/// RFC 3581: fill in rport/received on the top Via so responses route back through NAT.
fn fix_via(msg: &mut SipMessage, src: SocketAddr) {
    if let Some((_, via)) = msg.headers.iter_mut().find(|(n, _)| n.eq_ignore_ascii_case("Via")) {
        let (first, rest) = match via.find(',') {
            Some(i) => (via[..i].to_string(), via[i..].to_string()),
            None => (via.clone(), String::new()),
        };
        let mut parts: Vec<String> = first.split(';').map(str::to_string).collect();
        let mut has_rport = false;
        for p in parts.iter_mut().skip(1) {
            if p.trim().eq_ignore_ascii_case("rport") {
                *p = format!("rport={}", src.port());
                has_rport = true;
            }
        }
        if has_rport && param(&first, "received").is_none() {
            parts.push(format!("received={}", src.ip()));
        }
        *via = parts.join(";") + &rest;
    }
}

impl SipServer {
    pub async fn bind(cfg: Arc<Config>, codecs: Vec<CodecKind>) -> Result<Arc<Self>> {
        let sock = UdpSocket::bind(cfg.sip_bind).await?;
        let local = sock.local_addr()?;
        let advertised = SocketAddr::new(cfg.public_ip(), local.port());
        info!(%local, %advertised, "SIP listening (UDP)");
        Ok(Arc::new(SipServer {
            sock: Arc::new(sock),
            ports: PortPool::new(cfg.rtp_port_min, cfg.rtp_port_max),
            cfg,
            codecs,
            advertised,
            dialogs: Mutex::new(HashMap::new()),
            client_tx: Mutex::new(HashMap::new()),
            resp_cache: Mutex::new(HashMap::new()),
        }))
    }

    pub fn active_calls(&self) -> usize {
        self.dialogs.lock().unwrap().len()
    }

    pub fn shutdown(&self) {
        let all: Vec<_> = self.dialogs.lock().unwrap().values().cloned().collect();
        for d in all {
            d.terminate(EndReason::Shutdown);
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut buf = vec![0u8; 65535];
        let gc = self.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(5));
            loop {
                iv.tick().await;
                gc.resp_cache.lock().unwrap().retain(|_, (t, _)| t.elapsed() < TIMER_H);
            }
        });
        loop {
            let (n, src) = match self.sock.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(e) => {
                    // ICMP port unreachable surfaces here on some platforms; keep serving.
                    debug!("sip recv error: {e}");
                    continue;
                }
            };
            let data = &buf[..n];
            if data.iter().all(|b| b.is_ascii_whitespace()) {
                continue; // CRLF keep-alive
            }
            let mut msg = match SipMessage::parse(data) {
                Ok(m) => m,
                Err(e) => {
                    debug!(%src, "unparseable SIP datagram: {e}");
                    continue;
                }
            };
            if msg.status().is_some() {
                self.on_response(msg);
                continue;
            }
            fix_via(&mut msg, src);
            if msg.call_id().is_none() || msg.cseq().is_none() || msg.header("From").is_none() {
                continue;
            }
            if let Err(e) = self.on_request(msg, src).await {
                warn!(%src, "error handling request: {e:#}");
            }
        }
    }

    async fn send(&self, bytes: &[u8], dst: SocketAddr) {
        if let Err(e) = self.sock.send_to(bytes, dst).await {
            warn!(%dst, "sip send failed: {e}");
        }
    }

    fn contact(&self, user: &str) -> String {
        let user = if user.is_empty() { "sipws" } else { user };
        match self.advertised {
            SocketAddr::V4(a) => format!("<sip:{user}@{}:{}>", a.ip(), a.port()),
            SocketAddr::V6(a) => format!("<sip:{user}@[{}]:{}>", a.ip(), a.port()),
        }
    }

    fn build_response(&self, req: &SipMessage, code: u16, reason: &str, to_tag: Option<&str>) -> SipMessage {
        let mut r = SipMessage::response(code, reason);
        for v in req.headers_all("Via") {
            r.push("Via", v);
        }
        r.push("From", req.header("From").unwrap_or_default());
        let to = req.header("To").unwrap_or_default();
        match to_tag {
            Some(tag) if param(to, "tag").is_none() && code != 100 => r.push("To", format!("{to};tag={tag}")),
            _ => r.push("To", to),
        }
        r.push("Call-ID", req.call_id().unwrap_or_default());
        r.push("CSeq", req.header("CSeq").unwrap_or_default());
        if req.method() == Some("INVITE") && (101..300).contains(&code) {
            for rr in req.headers_all("Record-Route") {
                r.push("Record-Route", rr);
            }
            let user = super::message::uri_user(req.uri().unwrap_or_default());
            r.push("Contact", self.contact(user));
        }
        if code == 405 || req.method() == Some("OPTIONS") || (req.method() == Some("INVITE") && code == 200) {
            r.push("Allow", ALLOW);
        }
        r.push("Server", USER_AGENT);
        r
    }

    async fn respond(&self, req: &SipMessage, src: SocketAddr, code: u16, reason: &str, to_tag: Option<&str>) {
        let r = self.build_response(req, code, reason, to_tag);
        let bytes = r.to_bytes();
        if code >= 200
            && let Some(b) = req.top_branch() {
                let key = format!("{b}|{}", req.method().unwrap_or_default());
                self.resp_cache.lock().unwrap().insert(key, (Instant::now(), bytes.clone()));
            }
        self.send(&bytes, src).await;
    }

    fn dialog(&self, call_id: &str) -> Option<Arc<Dialog>> {
        self.dialogs.lock().unwrap().get(call_id).cloned()
    }

    pub fn remove_dialog(&self, call_id: &str) {
        self.dialogs.lock().unwrap().remove(call_id);
    }

    fn on_response(&self, msg: SipMessage) {
        let code = msg.status().unwrap_or(0);
        if code < 200 {
            return;
        }
        if let Some(branch) = msg.top_branch()
            && let Some(tx) = self.client_tx.lock().unwrap().remove(&branch) {
                let _ = tx.send(code);
            }
    }

    async fn on_request(self: &Arc<Self>, req: SipMessage, src: SocketAddr) -> Result<()> {
        let method = req.method().unwrap_or_default().to_ascii_uppercase();
        let call_id = req.call_id().unwrap_or_default().to_string();

        // Retransmission of a request we already answered with a final response?
        if method != "ACK"
            && let Some(b) = req.top_branch() {
                let key = format!("{b}|{}", req.method().unwrap_or_default());
                let cached = self.resp_cache.lock().unwrap().get(&key).map(|(_, b)| b.clone());
                if let Some(bytes) = cached {
                    self.send(&bytes, src).await;
                    return Ok(());
                }
            }

        let dialog = self.dialog(&call_id);
        match method.as_str() {
            "INVITE" => match dialog {
                Some(d) => self.on_invite_in_dialog(d, req, src).await,
                None => self.on_initial_invite(req, src).await,
            },
            "ACK" => {
                if let (Some(d), Some((n, _))) = (dialog, req.cseq()) {
                    let mut g = d.inner.lock().unwrap();
                    g.acked_cseq = g.acked_cseq.max(n);
                }
                Ok(())
            }
            "CANCEL" => {
                let Some(d) = dialog else {
                    self.respond(&req, src, 481, "Call/Transaction Does Not Exist", None).await;
                    return Ok(());
                };
                self.respond(&req, src, 200, "OK", Some(&d.local_tag)).await;
                if self.reject(&d, 487, "Request Terminated").await {
                    d.terminate(EndReason::Cancelled);
                }
                Ok(())
            }
            "BYE" => {
                match dialog {
                    Some(d) => {
                        self.respond(&req, src, 200, "OK", Some(&d.local_tag)).await;
                        info!(call_id, "remote hangup");
                        d.terminate(EndReason::RemoteBye);
                    }
                    None => self.respond(&req, src, 481, "Call/Transaction Does Not Exist", None).await,
                }
                Ok(())
            }
            "OPTIONS" => {
                let tag = rand_hex(8);
                let mut r = self.build_response(&req, 200, "OK", Some(&tag));
                r.push("Accept", "application/sdp");
                self.send(&r.to_bytes(), src).await;
                Ok(())
            }
            "INFO" => {
                let Some(d) = dialog else {
                    self.respond(&req, src, 481, "Call/Transaction Does Not Exist", None).await;
                    return Ok(());
                };
                if let Some((digit, dur)) = parse_info_dtmf(&req) {
                    let _ = d.media_tx.send(MediaCtl::Dtmf { digit, duration_ms: dur });
                }
                self.respond(&req, src, 200, "OK", Some(&d.local_tag)).await;
                Ok(())
            }
            "UPDATE" => {
                let Some(d) = dialog else {
                    self.respond(&req, src, 481, "Call/Transaction Does Not Exist", None).await;
                    return Ok(());
                };
                if req.body.is_empty() {
                    self.respond(&req, src, 200, "OK", Some(&d.local_tag)).await;
                } else {
                    match self.renegotiate(&d, &req) {
                        Ok(sdp) => {
                            let r = self
                                .build_response(&req, 200, "OK", Some(&d.local_tag))
                                .with_body("application/sdp", sdp.into_bytes());
                            self.send(&r.to_bytes(), src).await;
                        }
                        Err(_) => self.respond(&req, src, 488, "Not Acceptable Here", Some(&d.local_tag)).await,
                    }
                }
                Ok(())
            }
            "PRACK" => {
                self.respond(&req, src, 200, "OK", dialog.as_ref().map(|d| d.local_tag.as_str())).await;
                Ok(())
            }
            _ => {
                self.respond(&req, src, 405, "Method Not Allowed", Some(&rand_hex(8))).await;
                Ok(())
            }
        }
    }

    async fn on_initial_invite(self: &Arc<Self>, req: SipMessage, src: SocketAddr) -> Result<()> {
        let call_id = req.call_id().unwrap_or_default().to_string();
        let tag = rand_hex(10);
        if param(req.header("To").unwrap_or_default(), "tag").is_some() {
            self.respond(&req, src, 481, "Call/Transaction Does Not Exist", None).await;
            return Ok(());
        }
        if self.active_calls() >= self.cfg.max_calls {
            self.respond(&req, src, 486, "Busy Here", Some(&tag)).await;
            return Ok(());
        }
        if req.body.is_empty() {
            // Delayed offer (offer in 200 / answer in ACK) is not supported.
            self.respond(&req, src, 488, "Not Acceptable Here", Some(&tag)).await;
            return Ok(());
        }
        let offer = match Sdp::parse(&req.body) {
            Ok(o) => o,
            Err(e) => {
                warn!(call_id, "bad SDP: {e}");
                self.respond(&req, src, 400, "Bad SDP", Some(&tag)).await;
                return Ok(());
            }
        };
        let neg = match offer.negotiate(&self.codecs) {
            Ok(n) => n,
            Err(e) => {
                info!(call_id, "rejecting call: {e}");
                self.respond(&req, src, 488, "Not Acceptable Here", Some(&tag)).await;
                return Ok(());
            }
        };
        let rtp = match self.ports.bind(self.cfg.rtp_bind_ip).await {
            Ok(s) => s,
            Err(e) => {
                warn!(call_id, "RTP bind failed: {e}");
                self.respond(&req, src, 503, "Service Unavailable", Some(&tag)).await;
                return Ok(());
            }
        };

        let trying = self.build_response(&req, 100, "Trying", None).to_bytes();
        let (media_tx, media_rx) = mpsc::unbounded_channel();
        let dialog = Arc::new(Dialog {
            call_id: call_id.clone(),
            local_tag: tag,
            src,
            rtp_port: rtp.port,
            codec: neg.codec,
            session_id: rand::random_range(1_000_000..u32::MAX as u64),
            cancel: CancellationToken::new(),
            media_tx,
            inner: Mutex::new(DialogInner {
                state: InviteState::Proceeding,
                last_invite_resp: trying.clone(),
                last_invite_cseq: req.cseq().map(|c| c.0).unwrap_or(0),
                acked_cseq: 0,
                sdp_version: 1,
                local_cseq: rand::random_range(1..10_000),
                end_reason: None,
            }),
            invite: req,
        });
        self.dialogs.lock().unwrap().insert(call_id.clone(), dialog.clone());
        self.send(&trying, src).await;

        info!(
            call_id,
            from = addr_uri(dialog.invite.header("From").unwrap_or_default()),
            to = addr_uri(dialog.invite.header("To").unwrap_or_default()),
            codec = neg.codec.name(),
            remote = %SocketAddr::new(neg.remote_ip, neg.remote_port),
            rtp_port = rtp.port,
            "incoming call"
        );
        let answer_sdp = build_answer(
            &offer,
            &neg,
            self.cfg.public_ip(),
            rtp.port,
            self.cfg.ptime,
            dialog.session_id,
            1,
        );
        let server = self.clone();
        tokio::spawn(async move {
            call::run_call(server.clone(), dialog.clone(), neg, rtp, answer_sdp, media_rx).await;
            server.finish(&dialog).await;
        });
        Ok(())
    }

    async fn on_invite_in_dialog(self: &Arc<Self>, d: Arc<Dialog>, req: SipMessage, src: SocketAddr) -> Result<()> {
        let cseq = req.cseq().map(|c| c.0).unwrap_or(0);
        let (state, last_cseq, last) = {
            let g = d.inner.lock().unwrap();
            (g.state, g.last_invite_cseq, g.last_invite_resp.clone())
        };
        if cseq <= last_cseq {
            // Retransmission of the INVITE (or re-INVITE) we're handling.
            self.send(&last, src).await;
            return Ok(());
        }
        if state != InviteState::Answered {
            self.respond(&req, src, 491, "Request Pending", Some(&d.local_tag)).await;
            return Ok(());
        }
        // re-INVITE (hold/resume, session refresh, address change).
        let sdp = if req.body.is_empty() {
            Ok(self.current_answer(&d))
        } else {
            self.renegotiate(&d, &req)
        };
        let sdp = match sdp {
            Ok(s) => s,
            Err(_) => {
                self.respond(&req, src, 488, "Not Acceptable Here", Some(&d.local_tag)).await;
                return Ok(());
            }
        };
        let ok = self
            .build_response(&req, 200, "OK", Some(&d.local_tag))
            .with_body("application/sdp", sdp.into_bytes())
            .to_bytes();
        {
            let mut g = d.inner.lock().unwrap();
            g.last_invite_cseq = cseq;
            g.last_invite_resp = ok.clone();
        }
        self.send(&ok, src).await;
        self.spawn_2xx_retransmit(d, cseq, src);
        Ok(())
    }

    /// Builds an answer to a new offer inside an existing dialog. The codec cannot change mid-call.
    fn renegotiate(&self, d: &Dialog, req: &SipMessage) -> Result<String> {
        let offer = Sdp::parse(&req.body)?;
        let neg = offer.negotiate(&[d.codec])?;
        let _ = d.media_tx.send(MediaCtl::Remote {
            addr: SocketAddr::new(neg.remote_ip, neg.remote_port),
            direction: neg.direction,
        });
        let version = {
            let mut g = d.inner.lock().unwrap();
            g.sdp_version += 1;
            g.sdp_version
        };
        Ok(build_answer(&offer, &neg, self.cfg.public_ip(), d.rtp_port, self.cfg.ptime, d.session_id, version))
    }

    fn current_answer(&self, d: &Dialog) -> String {
        // Re-offer our side for an offerless re-INVITE (session refresh); answer in ACK is ignored.
        let ip = self.cfg.public_ip();
        let ipver = if ip.is_ipv4() { "IP4" } else { "IP6" };
        let (name, rate, ch) = d.codec.rtpmap();
        let pt = match d.codec {
            CodecKind::Pcmu => 0,
            CodecKind::Pcma => 8,
            CodecKind::Opus => 111,
        };
        let enc = if ch > 1 { format!("{name}/{rate}/{ch}") } else { format!("{name}/{rate}") };
        let version = {
            let mut g = d.inner.lock().unwrap();
            g.sdp_version += 1;
            g.sdp_version
        };
        format!(
            "v=0\r\no=sipws {sid} {version} IN {ipver} {ip}\r\ns=sipws\r\nc=IN {ipver} {ip}\r\nt=0 0\r\n\
             m=audio {port} RTP/AVP {pt}\r\na=rtpmap:{pt} {enc}\r\na=ptime:{ptime}\r\na=sendrecv\r\n",
            sid = d.session_id,
            port = d.rtp_port,
            ptime = self.cfg.ptime
        )
    }

    /// Sends 200 OK to the initial INVITE. Returns false if the call was cancelled meanwhile.
    pub async fn answer(&self, d: &Arc<Dialog>, sdp: String) -> bool {
        let (ok, cseq) = {
            let mut g = d.inner.lock().unwrap();
            if g.state != InviteState::Proceeding {
                return false;
            }
            g.state = InviteState::Answered;
            let ok = self
                .build_response(&d.invite, 200, "OK", Some(&d.local_tag))
                .with_body("application/sdp", sdp.into_bytes())
                .to_bytes();
            g.last_invite_resp = ok.clone();
            (ok, g.last_invite_cseq)
        };
        self.send(&ok, d.src).await;
        self.spawn_2xx_retransmit(d.clone(), cseq, d.src);
        true
    }

    fn spawn_2xx_retransmit(&self, d: Arc<Dialog>, cseq: u32, dst: SocketAddr) {
        let sock = self.sock.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            let mut wait = T1;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = d.cancel.cancelled() => return,
                }
                let resend = {
                    let g = d.inner.lock().unwrap();
                    if g.acked_cseq >= cseq || g.last_invite_cseq != cseq {
                        return;
                    }
                    g.last_invite_resp.clone()
                };
                if start.elapsed() >= TIMER_H {
                    warn!(call_id = d.call_id, "no ACK for 200 OK, ending call");
                    d.terminate(EndReason::AckTimeout);
                    return;
                }
                let _ = sock.send_to(&resend, dst).await;
                wait = (wait * 2).min(T2);
            }
        });
    }

    /// Final non-2xx response to a pending initial INVITE. Returns false if already answered.
    pub async fn reject(&self, d: &Dialog, code: u16, reason: &str) -> bool {
        {
            let mut g = d.inner.lock().unwrap();
            if g.state != InviteState::Proceeding {
                return false;
            }
            g.state = InviteState::Terminated;
        }
        self.respond(&d.invite, d.src, code, reason, Some(&d.local_tag)).await;
        true
    }

    /// Cleanup after the call task ends: BYE if needed, then forget the dialog.
    /// Ends the SIP side of an answered call now (BYE in the background if needed), while
    /// the call task may still be busy with the WebSocket (stt.finalize). Idempotent.
    pub fn release_answered(self: &Arc<Self>, d: &Arc<Dialog>) {
        let reason = d.end_reason().unwrap_or(EndReason::Error("unknown".into()));
        {
            let mut g = d.inner.lock().unwrap();
            if g.state != InviteState::Answered {
                return;
            }
            g.state = InviteState::Terminated;
        }
        d.cancel.cancel(); // stops 2xx retransmission
        if reason.needs_bye() {
            let (srv, d) = (self.clone(), d.clone());
            tokio::spawn(async move {
                if let Err(e) = srv.send_bye(&d).await {
                    warn!(call_id = d.call_id, "BYE failed: {e:#}");
                }
            });
        }
    }

    async fn finish(&self, d: &Arc<Dialog>) {
        let reason = d.end_reason().unwrap_or(EndReason::Error("unknown".into()));
        let state = d.inner.lock().unwrap().state;
        match state {
            InviteState::Proceeding => {
                self.reject(d, 480, "Temporarily Unavailable").await;
            }
            InviteState::Answered if reason.needs_bye() => {
                if let Err(e) = self.send_bye(d).await {
                    warn!(call_id = d.call_id, "BYE failed: {e:#}");
                }
            }
            _ => {}
        }
        d.inner.lock().unwrap().state = InviteState::Terminated;
        d.cancel.cancel();
        self.remove_dialog(&d.call_id);
        info!(call_id = d.call_id, reason = reason.as_str(), "call ended");
    }

    async fn send_bye(&self, d: &Dialog) -> Result<()> {
        let inv = &d.invite;
        let target = inv
            .header("Contact")
            .map(addr_uri)
            .unwrap_or_else(|| addr_uri(inv.header("From").unwrap_or_default()))
            .to_string();
        let routes: Vec<String> = inv
            .headers_all("Record-Route")
            .flat_map(|v| v.split(',').map(|s| s.trim().to_string()).collect::<Vec<_>>())
            .collect();
        let cseq = {
            let mut g = d.inner.lock().unwrap();
            g.local_cseq += 1;
            g.local_cseq
        };
        let branch = format!("z9hG4bK{}", rand_hex(16));
        let mut bye = SipMessage::request("BYE", &target);
        let via_host = match self.advertised {
            SocketAddr::V4(a) => a.to_string(),
            SocketAddr::V6(a) => a.to_string(),
        };
        bye.push("Via", format!("SIP/2.0/UDP {via_host};branch={branch};rport"));
        bye.push("Max-Forwards", "70");
        for r in &routes {
            bye.push("Route", r.as_str());
        }
        let to = inv.header("To").unwrap_or_default();
        let from = if param(to, "tag").is_some() { to.to_string() } else { format!("{to};tag={}", d.local_tag) };
        bye.push("From", from);
        bye.push("To", inv.header("From").unwrap_or_default());
        bye.push("Call-ID", d.call_id.as_str());
        bye.push("CSeq", format!("{cseq} BYE"));
        bye.push("User-Agent", USER_AGENT);
        let bytes = bye.to_bytes();

        // Next hop: first Record-Route if any, otherwise where the INVITE came from (NAT-safe).
        let mut dst = d.src;
        if let Some(first) = routes.first() {
            let (host, port) = uri_hostport(addr_uri(first));
            if let Ok(mut it) = tokio::net::lookup_host((host.as_str(), port)).await
                && let Some(a) = it.next() {
                    dst = a;
                }
        }

        let (tx, mut rx) = oneshot::channel();
        self.client_tx.lock().unwrap().insert(branch.clone(), tx);
        let mut wait = T1;
        let deadline = Instant::now() + Duration::from_secs(8);
        let result = loop {
            self.send(&bytes, dst).await;
            tokio::select! {
                code = &mut rx => break code.ok(),
                _ = tokio::time::sleep(wait) => {}
            }
            if Instant::now() >= deadline {
                break None;
            }
            wait = (wait * 2).min(T2);
        };
        self.client_tx.lock().unwrap().remove(&branch);
        debug!(call_id = d.call_id, ?result, "BYE completed");
        Ok(())
    }
}

/// SIP INFO DTMF: application/dtmf-relay ("Signal=5\r\nDuration=160") or application/dtmf ("5").
fn parse_info_dtmf(req: &SipMessage) -> Option<(char, u32)> {
    let ct = req.header("Content-Type")?.to_ascii_lowercase();
    let body = std::str::from_utf8(&req.body).ok()?;
    if ct.starts_with("application/dtmf-relay") {
        let mut digit = None;
        let mut dur = 100;
        for line in body.lines() {
            if let Some((k, v)) = line.split_once('=') {
                match k.trim().to_ascii_lowercase().as_str() {
                    "signal" => digit = v.trim().chars().next(),
                    "duration" => dur = v.trim().parse().unwrap_or(100),
                    _ => {}
                }
            }
        }
        digit.map(|c| (c, dur))
    } else if ct.starts_with("application/dtmf") {
        body.trim().chars().next().map(|c| (c, 100))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn via_rport() {
        let raw = "OPTIONS sip:x SIP/2.0\r\nVia: SIP/2.0/UDP 10.0.0.1:5060;rport;branch=z9hG4bKa, SIP/2.0/UDP p\r\nCall-ID: 1\r\nCSeq: 1 OPTIONS\r\nFrom: <sip:a@b>;tag=1\r\nTo: <sip:x>\r\n\r\n";
        let mut m = SipMessage::parse(raw.as_bytes()).unwrap();
        fix_via(&mut m, "203.0.113.5:40000".parse().unwrap());
        assert_eq!(
            m.header("Via").unwrap(),
            "SIP/2.0/UDP 10.0.0.1:5060;rport=40000;branch=z9hG4bKa;received=203.0.113.5, SIP/2.0/UDP p"
        );
    }
}

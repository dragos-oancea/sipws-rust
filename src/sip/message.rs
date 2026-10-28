//! Minimal SIP message model: parsing, header access and serialisation.
//! Enough of RFC 3261 for a UDP UAS that also sends in-dialog BYEs.

use std::fmt::Write as _;

#[derive(Debug, Clone, PartialEq)]
pub enum StartLine {
    Request { method: String, uri: String },
    Response { code: u16, reason: String },
}

#[derive(Debug, Clone)]
pub struct SipMessage {
    pub start: StartLine,
    /// Headers in wire order; names are normalised to their long form.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("message is not valid UTF-8 in header section")]
    Utf8,
    #[error("missing header/body separator")]
    NoSeparator,
    #[error("bad start line: {0}")]
    StartLine(String),
    #[error("bad header line: {0}")]
    Header(String),
}

fn expand_compact(name: &str) -> String {
    let long = match name.to_ascii_lowercase().as_str() {
        "v" => "Via",
        "f" => "From",
        "t" => "To",
        "i" => "Call-ID",
        "m" => "Contact",
        "l" => "Content-Length",
        "c" => "Content-Type",
        "k" => "Supported",
        "s" => "Subject",
        "e" => "Content-Encoding",
        _ => return name.to_string(),
    };
    long.to_string()
}

impl SipMessage {
    pub fn parse(buf: &[u8]) -> Result<Self, ParseError> {
        let sep = find_subslice(buf, b"\r\n\r\n").ok_or(ParseError::NoSeparator)?;
        let head = std::str::from_utf8(&buf[..sep]).map_err(|_| ParseError::Utf8)?;
        let mut body = buf[sep + 4..].to_vec();

        // Unfold continuation lines (RFC 3261 7.3.1).
        let mut lines: Vec<String> = Vec::new();
        for line in head.split("\r\n") {
            if (line.starts_with(' ') || line.starts_with('\t')) && !lines.is_empty() {
                let last = lines.last_mut().unwrap();
                last.push(' ');
                last.push_str(line.trim());
            } else {
                lines.push(line.to_string());
            }
        }
        let mut it = lines.into_iter();
        let first = it.next().ok_or_else(|| ParseError::StartLine(String::new()))?;
        let start = if let Some(rest) = first.strip_prefix("SIP/2.0 ") {
            let (code, reason) = rest.split_once(' ').unwrap_or((rest, ""));
            let code = code
                .parse()
                .map_err(|_| ParseError::StartLine(first.clone()))?;
            StartLine::Response { code, reason: reason.to_string() }
        } else {
            let mut parts = first.split_whitespace();
            let (Some(method), Some(uri), Some("SIP/2.0")) = (parts.next(), parts.next(), parts.next())
            else {
                return Err(ParseError::StartLine(first));
            };
            StartLine::Request { method: method.to_string(), uri: uri.to_string() }
        };

        let mut headers = Vec::new();
        for line in it {
            if line.is_empty() {
                continue;
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| ParseError::Header(line.clone()))?;
            headers.push((expand_compact(name.trim()), value.trim().to_string()));
        }
        let mut msg = SipMessage { start, headers, body: Vec::new() };
        if let Some(len) = msg.header("Content-Length").and_then(|v| v.parse::<usize>().ok()) {
            body.truncate(len);
        }
        msg.body = body;
        Ok(msg)
    }

    pub fn request(method: &str, uri: &str) -> Self {
        SipMessage {
            start: StartLine::Request { method: method.into(), uri: uri.into() },
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn response(code: u16, reason: &str) -> Self {
        SipMessage {
            start: StartLine::Response { code, reason: reason.into() },
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn method(&self) -> Option<&str> {
        match &self.start {
            StartLine::Request { method, .. } => Some(method),
            _ => None,
        }
    }

    pub fn uri(&self) -> Option<&str> {
        match &self.start {
            StartLine::Request { uri, .. } => Some(uri),
            _ => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match &self.start {
            StartLine::Response { code, .. } => Some(*code),
            _ => None,
        }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn headers_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.headers
            .iter()
            .filter(move |(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn push(&mut self, name: &str, value: impl Into<String>) {
        self.headers.push((name.to_string(), value.into()));
    }

    pub fn set(&mut self, name: &str, value: impl Into<String>) {
        let value = value.into();
        if let Some(h) = self.headers.iter_mut().find(|(n, _)| n.eq_ignore_ascii_case(name)) {
            h.1 = value;
        } else {
            self.push(name, value);
        }
    }

    pub fn call_id(&self) -> Option<&str> {
        self.header("Call-ID")
    }

    /// Returns (sequence number, method) from CSeq.
    pub fn cseq(&self) -> Option<(u32, String)> {
        let v = self.header("CSeq")?;
        let (n, m) = v.split_once(char::is_whitespace)?;
        Some((n.trim().parse().ok()?, m.trim().to_string()))
    }

    /// Branch parameter of the topmost Via.
    pub fn top_branch(&self) -> Option<String> {
        let via = self.header("Via")?;
        let first = via.split(',').next()?;
        param(first, "branch").map(str::to_string)
    }

    pub fn with_body(mut self, content_type: &str, body: Vec<u8>) -> Self {
        self.set("Content-Type", content_type);
        self.body = body;
        self
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut s = String::with_capacity(512);
        match &self.start {
            StartLine::Request { method, uri } => {
                let _ = write!(s, "{method} {uri} SIP/2.0\r\n");
            }
            StartLine::Response { code, reason } => {
                let _ = write!(s, "SIP/2.0 {code} {reason}\r\n");
            }
        }
        for (n, v) in &self.headers {
            if n.eq_ignore_ascii_case("Content-Length") {
                continue;
            }
            let _ = write!(s, "{n}: {v}\r\n");
        }
        let _ = write!(s, "Content-Length: {}\r\n\r\n", self.body.len());
        let mut out = s.into_bytes();
        out.extend_from_slice(&self.body);
        out
    }
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Looks up `;name=value` (or bare `;name`) in a header value. Returns "" for bare params.
pub fn param<'a>(value: &'a str, name: &str) -> Option<&'a str> {
    // Only look at params after the URI part (outside <...>).
    let tail = match value.rfind('>') {
        Some(i) => &value[i + 1..],
        None => value,
    };
    for p in tail.split(';').skip(1) {
        let (k, v) = p.split_once('=').unwrap_or((p, ""));
        if k.trim().eq_ignore_ascii_case(name) {
            return Some(v.trim());
        }
    }
    None
}

/// Extracts the URI out of a name-addr (`"Bob" <sip:bob@x>;tag=1`) or addr-spec.
pub fn addr_uri(value: &str) -> &str {
    if let (Some(a), Some(b)) = (value.find('<'), value.find('>'))
        && a < b {
            return &value[a + 1..b];
        }
    value.split(';').next().unwrap_or(value).trim()
}

/// User part of a SIP URI (`sip:alice@host` -> `alice`).
pub fn uri_user(uri: &str) -> &str {
    let no_scheme = uri.split_once(':').map(|(_, r)| r).unwrap_or(uri);
    match no_scheme.split_once('@') {
        Some((user, _)) => user.split(';').next().unwrap_or(user),
        None => "",
    }
}

/// host[:port] of a SIP URI, with default port 5060.
pub fn uri_hostport(uri: &str) -> (String, u16) {
    let no_scheme = uri.split_once(':').map(|(_, r)| r).unwrap_or(uri);
    let after_user = no_scheme.rsplit_once('@').map(|(_, h)| h).unwrap_or(no_scheme);
    let hp = after_user.split([';', '?']).next().unwrap_or(after_user);
    if let Some(rest) = hp.strip_prefix('[') {
        // IPv6 reference
        if let Some((host, tail)) = rest.split_once(']') {
            let port = tail.strip_prefix(':').and_then(|p| p.parse().ok()).unwrap_or(5060);
            return (host.to_string(), port);
        }
    }
    match hp.rsplit_once(':') {
        Some((h, p)) if p.parse::<u16>().is_ok() => (h.to_string(), p.parse().unwrap()),
        _ => (hp.to_string(), 5060),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INVITE: &str = "INVITE sip:bot@10.0.0.1 SIP/2.0\r\n\
v: SIP/2.0/UDP 10.0.0.2:5060;branch=z9hG4bK776;rport\r\n\
Via: SIP/2.0/UDP 10.0.0.9;branch=z9hG4bKproxy\r\n\
f: \"Alice\" <sip:alice@example.com>;tag=1928\r\n\
To: <sip:bot@10.0.0.1>\r\n\
i: a84b4c76e66710\r\n\
CSeq: 314159 INVITE\r\n\
Contact: <sip:alice@10.0.0.2:5062;transport=udp>\r\n\
X-Custom: hello\r\n\
\tworld\r\n\
Content-Length: 4\r\n\r\nabcdEXTRA";

    #[test]
    fn parses_request() {
        let m = SipMessage::parse(INVITE.as_bytes()).unwrap();
        assert_eq!(m.method(), Some("INVITE"));
        assert_eq!(m.call_id(), Some("a84b4c76e66710"));
        assert_eq!(m.cseq(), Some((314159, "INVITE".into())));
        assert_eq!(m.headers_all("Via").count(), 2);
        assert_eq!(m.top_branch().as_deref(), Some("z9hG4bK776"));
        assert_eq!(m.header("X-Custom"), Some("hello world"));
        assert_eq!(m.body, b"abcd");
        let from = m.header("From").unwrap();
        assert_eq!(param(from, "tag"), Some("1928"));
        assert_eq!(addr_uri(from), "sip:alice@example.com");
        assert_eq!(uri_user(addr_uri(from)), "alice");
        let c = addr_uri(m.header("Contact").unwrap());
        assert_eq!(uri_hostport(c), ("10.0.0.2".to_string(), 5062));
    }

    #[test]
    fn roundtrip() {
        let m = SipMessage::parse(INVITE.as_bytes()).unwrap();
        let again = SipMessage::parse(&m.to_bytes()).unwrap();
        assert_eq!(again.headers.len(), m.headers.len());
        assert_eq!(again.body, m.body);
    }
}

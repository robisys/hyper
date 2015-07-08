//! Pieces pertaining to the HTTP message protocol.
use std::borrow::Cow;
use std::fmt;
use std::io::{self, Read, Write};

use header::Connection;
use header::ConnectionOption::{KeepAlive, Close};
use header::Headers;
use method::Method;
use net::Transport;
use status::StatusCode;
use uri::RequestUri;
use version::HttpVersion;
use version::HttpVersion::{Http10, Http11};

#[cfg(feature = "serde-serialization")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub use self::conn::{Conn, MessageHandler, MessageHandlerFactory, Next};

mod buffer;
mod conn;
mod h1;
//mod h2;

pub struct Decoder<'a, T: Read + 'a>(DecoderImpl<'a, T>);
pub struct Encoder<'a, T: Transport + 'a>(EncoderImpl<'a, T>);

enum DecoderImpl<'a, T: Read + 'a> {
    H1(&'a mut h1::Decoder, Trans<'a, T>),
}

enum Trans<'a, T: Read + 'a> {
    Port(&'a mut T),
    Buf(self::buffer::BufReader<'a, T>)
}

impl<'a, T: Read + 'a> Read for Trans<'a, T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Trans::Port(ref mut t) => t.read(buf),
            Trans::Buf(ref mut b) => b.read(buf)
        }
    }
}

enum EncoderImpl<'a, T: Transport + 'a> {
    H1(&'a mut h1::Encoder, &'a mut T),
}

impl<'a, T: Read> Decoder<'a, T> {
    fn h1(decoder: &'a mut h1::Decoder, transport: Trans<'a, T>) -> Decoder<'a, T> {
        Decoder(DecoderImpl::H1(decoder, transport))
    }
}

impl<'a, T: Transport> Encoder<'a, T> {
    fn h1(encoder: &'a mut h1::Encoder, transport: &'a mut T) -> Encoder<'a, T> {
        Encoder(EncoderImpl::H1(encoder, transport))
    }
}

impl<'a, T: Read> Read for Decoder<'a, T> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.0 {
            DecoderImpl::H1(ref mut decoder, ref mut transport) => {
                decoder.decode(transport, buf)
            }
        }
    }
}

impl<'a, T: Transport> Write for Encoder<'a, T> {
    #[inline]
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        match self.0 {
            EncoderImpl::H1(ref mut encoder, ref mut transport) => {
                encoder.encode(*transport, data)
                //transport.write_atomic(&[b"foo", b"bar"])
            }
        }
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        match self.0 {
            EncoderImpl::H1(_, ref mut transport) => {
                transport.flush()
            }
        }
    }
}

/// Because privacy rules. Reasons.
/// https://github.com/rust-lang/rust/issues/30905
mod internal {
    use std::io::{self, Write};

    #[derive(Debug, Clone)]
    pub struct WriteBuf<T: AsRef<[u8]>> {
        pub bytes: T,
        pub pos: usize,
    }

    pub trait AtomicWrite {
        fn write_atomic(&mut self, data: &[&[u8]]) -> io::Result<usize>;
    }

    #[cfg(not(windows))]
    impl<T: Write + ::vecio::Writev> AtomicWrite for T {

        fn write_atomic(&mut self, bufs: &[&[u8]]) -> io::Result<usize> {
            use vecio::Rawv;
            self.writev(bufs)
        }

    }

    #[cfg(windows)]
    impl<T: Write> AtomicWrite for T {
        fn write_atomic(&mut self, bufs: &[&[u8]]) -> io::Result<usize> {
            let cap = bufs.iter().map(|buf| buf.len()).fold(0, |total, next| total + next);
            let mut vec = Vec::with_capacity(cap);
            for &buf in bufs {
                vec.extend(buf);
            }
            self.write(&vec)
        }
    }
}

/// An Incoming Message head. Includes request/status line, and headers.
#[derive(Debug, Default)]
pub struct MessageHead<S> {
    /// HTTP version of the message.
    pub version: HttpVersion,
    /// Subject (request line or status line) of Incoming message.
    pub subject: S,
    /// Headers of the Incoming message.
    pub headers: Headers
}

/// An incoming request message.
pub type RequestHead = MessageHead<RequestLine>;

#[derive(Debug, Default)]
pub struct RequestLine(pub Method, pub RequestUri);

impl fmt::Display for RequestLine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.0, self.1)
    }
}

/// An incoming response message.
pub type ResponseHead = MessageHead<RawStatus>;

impl<S> MessageHead<S> {
    pub fn should_keep_alive(&self) -> bool {
        should_keep_alive(self.version, &self.headers)
    }
}

/// The raw status code and reason-phrase.
#[derive(Clone, PartialEq, Debug)]
pub struct RawStatus(pub u16, pub Cow<'static, str>);

impl fmt::Display for RawStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.0, self.1)
    }
}

impl From<StatusCode> for RawStatus {
    fn from(status: StatusCode) -> RawStatus {
        RawStatus(status.to_u16(), Cow::Borrowed(status.canonical_reason().unwrap_or("")))
    }
}

impl Default for RawStatus {
    fn default() -> RawStatus {
        RawStatus(200, Cow::Borrowed("OK"))
    }
}

#[cfg(feature = "serde-serialization")]
impl Serialize for RawStatus {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        (self.0, &self.1).serialize(serializer)
    }
}

#[cfg(feature = "serde-serialization")]
impl Deserialize for RawStatus {
    fn deserialize<D>(deserializer: &mut D) -> Result<RawStatus, D::Error> where D: Deserializer {
        let representation: (u16, String) = try!(Deserialize::deserialize(deserializer));
        Ok(RawStatus(representation.0, Cow::Owned(representation.1)))
    }
}

/// Checks if a connection should be kept alive.
#[inline]
pub fn should_keep_alive(version: HttpVersion, headers: &Headers) -> bool {
    trace!("should_keep_alive( {:?}, {:?} )", version, headers.get::<Connection>());
    match (version, headers.get::<Connection>()) {
        (Http10, None) => false,
        (Http10, Some(conn)) if !conn.contains(&KeepAlive) => false,
        (Http11, Some(conn)) if conn.contains(&Close)  => false,
        _ => true
    }
}
pub type ParseResult<T> = ::Result<Option<(MessageHead<T>, usize)>>;

pub fn parse<T: Http1Message<Incoming=I>, I>(rdr: &[u8]) -> ParseResult<I> {
    h1::parse::<T, I>(rdr)
}

pub enum ServerMessage {}
pub enum ClientMessage {}

pub trait Http1Message {
    type Incoming;
    type Outgoing: Default;
    //TODO: replace with associated const when stable
    fn initial_interest() -> Next;
    fn parse(bytes: &[u8]) -> ParseResult<Self::Incoming>;
    fn decoder(head: &MessageHead<Self::Incoming>) -> ::Result<h1::Decoder>;
    fn encode<W: io::Write>(head: MessageHead<Self::Outgoing>, dst: &mut W) -> h1::Encoder;

}

#[test]
fn test_should_keep_alive() {
    let mut headers = Headers::new();

    assert!(!should_keep_alive(Http10, &headers));
    assert!(should_keep_alive(Http11, &headers));

    headers.set(Connection::close());
    assert!(!should_keep_alive(Http10, &headers));
    assert!(!should_keep_alive(Http11, &headers));

    headers.set(Connection::keep_alive());
    assert!(should_keep_alive(Http10, &headers));
    assert!(should_keep_alive(Http11, &headers));
}

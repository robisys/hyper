//! A collection of traits abstracting over Listeners and Streams.
use std::io::{self, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};

use mio::tcp::{TcpStream, TcpListener};
use mio::{Selector, Token, Evented, EventSet, PollOpt, TryAccept};

#[cfg(feature = "openssl")]
pub use self::openssl::Openssl;

#[cfg(not(windows))]
pub trait Transport: Read + Write + Evented + ::vecio::Writev {}

#[cfg(not(windows))]
impl<T: Read + Write + Evented + ::vecio::Writev> Transport for T {}

#[cfg(windows)]
pub trait Transport: Read + Write + Evented {}

#[cfg(windows)]
impl<T: Read + Write + Evented> Transport for T {}

/// A connector creates a NetworkStream.
pub trait Connect {
    /// Type of Stream to create
    type Output: Transport;
    /// Connect to a remote address.
    fn connect(&self, host: &str, port: u16, scheme: &str) -> ::Result<Self::Output>;
}

/// An alias to `mio::tcp::TcpStream`.
#[derive(Debug)]
pub struct HttpStream(pub TcpStream);

impl Read for HttpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for HttpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Evented for HttpStream {
    #[inline]
    fn register(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.0.register(selector, token, interest, opts)
    }

    #[inline]
    fn reregister(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.0.reregister(selector, token, interest, opts)
    }

    #[inline]
    fn deregister(&self, selector: &mut Selector) -> io::Result<()> {
        self.0.deregister(selector)
    }
}

impl ::vecio::Writev for HttpStream {
    fn writev(&mut self, bufs: &[&[u8]]) -> io::Result<usize> {
        use ::vecio::Rawv;
        self.0.writev(bufs)
    }
}

/// An alias to `mio::tcp::TcpListener`.
pub struct HttpListener(pub TcpListener);


impl TryAccept for HttpListener {
    type Output = HttpStream;

    #[inline]
    fn accept(&self) -> io::Result<Option<HttpStream>> {
        TryAccept::accept(&self.0).map(|ok| ok.map(HttpStream))
    }
}

impl Evented for HttpListener {
    #[inline]
    fn register(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.0.register(selector, token, interest, opts)
    }

    #[inline]
    fn reregister(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.0.reregister(selector, token, interest, opts)
    }

    #[inline]
    fn deregister(&self, selector: &mut Selector) -> io::Result<()> {
        self.0.deregister(selector)
    }
}


/// A connector that will produce HttpStreams.
#[derive(Debug, Clone, Default)]
pub struct HttpConnector;

impl Connect for HttpConnector {
    type Output = HttpStream;

    fn connect(&self, host: &str, port: u16, scheme: &str) -> ::Result<HttpStream> {
        let addr = (host, port).to_socket_addrs().unwrap().next().unwrap();
        Ok(try!(match scheme {
            "http" => {
                debug!("http scheme");
                Ok(HttpStream(try!(TcpStream::connect(&addr))))
            },
            _ => {
                Err(io::Error::new(io::ErrorKind::InvalidInput,
                                "Invalid scheme for Http"))
            }
        }))
    }
}

/// A closure as a connector used to generate TcpStreams per request
///
/// # Example
///
/// Basic example:
///
/// ```norun
/// Client::with_connector(|addr: &str, port: u16, scheme: &str| {
///     TcpStream::connect(&(addr, port))
/// });
/// ```
///
/// Example using TcpBuilder from the net2 crate if you want to configure your source socket:
///
/// ```norun
/// Client::with_connector(|addr: &str, port: u16, scheme: &str| {
///     let b = try!(TcpBuilder::new_v4());
///     try!(b.bind("127.0.0.1:0"));
///     b.connect(&(addr, port))
/// });
/// ```
impl<F> Connect for F where F: Fn(&str, u16, &str) -> io::Result<TcpStream> {
    type Output = HttpStream;

    fn connect(&self, host: &str, port: u16, scheme: &str) -> ::Result<HttpStream> {
        Ok(HttpStream(try!((*self)(host, port, scheme))))
    }
}

/// An abstraction to allow any SSL implementation to be used with HttpsStreams.
pub trait Ssl {
    /// The protected stream.
    type Stream: Transport;
    /// Wrap a client stream with SSL.
    fn wrap_client(&self, stream: TcpStream, host: &str) -> ::Result<Self::Stream>;
    /// Wrap a server stream with SSL.
    fn wrap_server(&self, stream: TcpStream) -> ::Result<Self::Stream>;
}

/// A stream over the HTTP protocol, possibly protected by SSL.
#[derive(Debug)]
pub enum HttpsStream<S: Transport> {
    /// A plain text stream.
    Http(HttpStream),
    /// A stream protected by SSL.
    Https(S)
}

impl<S: Transport> Read for HttpsStream<S> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            HttpsStream::Http(ref mut s) => s.read(buf),
            HttpsStream::Https(ref mut s) => s.read(buf)
        }
    }
}

impl<S: Transport> Write for HttpsStream<S> {
    #[inline]
    fn write(&mut self, msg: &[u8]) -> io::Result<usize> {
        match *self {
            HttpsStream::Http(ref mut s) => s.write(msg),
            HttpsStream::Https(ref mut s) => s.write(msg)
        }
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        match *self {
            HttpsStream::Http(ref mut s) => s.flush(),
            HttpsStream::Https(ref mut s) => s.flush()
        }
    }
}

impl<S: Transport> ::vecio::Writev for HttpsStream<S> {
    fn writev(&mut self, bufs: &[&[u8]]) -> io::Result<usize> {
        match *self {
            HttpsStream::Http(ref mut s) => s.writev(bufs),
            HttpsStream::Https(ref mut s) => s.writev(bufs)
        }
    }
}


#[cfg(unix)]
impl ::std::os::unix::io::AsRawFd for HttpStream {
    fn as_raw_fd(&self) -> ::std::os::unix::io::RawFd {
        self.0.as_raw_fd()
    }
}

#[cfg(unix)]
impl<S: Transport + ::std::os::unix::io::AsRawFd> ::std::os::unix::io::AsRawFd for HttpsStream<S> {
    fn as_raw_fd(&self) -> ::std::os::unix::io::RawFd {
        match *self {
            HttpsStream::Http(ref s) => s.as_raw_fd(),
            HttpsStream::Https(ref s) => s.as_raw_fd(),
        }
    }
}

impl<S: Transport> Evented for HttpsStream<S> {
    #[inline]
    fn register(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        match *self {
            HttpsStream::Http(ref s) => s.register(selector, token, interest, opts),
            HttpsStream::Https(ref s) => s.register(selector, token, interest, opts),
        }
    }

    #[inline]
    fn reregister(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        match *self {
            HttpsStream::Http(ref s) => s.reregister(selector, token, interest, opts),
            HttpsStream::Https(ref s) => s.reregister(selector, token, interest, opts),
        }
    }

    #[inline]
    fn deregister(&self, selector: &mut Selector) -> io::Result<()> {
        match *self {
            HttpsStream::Http(ref s) => s.deregister(selector),
            HttpsStream::Https(ref s) => s.deregister(selector),
        }
    }
}

/// A Http Listener over SSL.
pub struct HttpsListener<S: Ssl> {
    listener: TcpListener,
    ssl: S,
}

impl<S: Ssl> HttpsListener<S> {
    /// Start listening to an address over HTTPS.
    #[inline]
    pub fn new/*<To: ToSocketAddrs>(addr: To,*/(addr: &SocketAddr, ssl: S) -> io::Result<HttpsListener<S>> {
        TcpListener::bind(addr).map(|l| HttpsListener {
            listener: l,
            ssl: ssl
        })
    }

    /// Construct an HttpsListener from a bound `TcpListener`.
    pub fn with_listener(listener: TcpListener, ssl: S) -> HttpsListener<S> {
        HttpsListener {
            listener: listener,
            ssl: ssl
        }
    }
}

impl<S: Ssl> TryAccept for HttpsListener<S> {
    type Output = S::Stream;

    #[inline]
    fn accept(&self) -> io::Result<Option<S::Stream>> {
        self.listener.accept().and_then(|s| match s {
            Some((s, _)) => self.ssl.wrap_server(s).map(Some).map_err(|e| {
                match e {
                    ::Error::Io(e) => e,
                    _ => io::Error::new(io::ErrorKind::Other, e),

                }
            }),
            None => Ok(None),
        })
    }
}

impl<S: Ssl> Evented for HttpsListener<S> {
    #[inline]
    fn register(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.listener.register(selector, token, interest, opts)
    }

    #[inline]
    fn reregister(&self, selector: &mut Selector, token: Token, interest: EventSet, opts: PollOpt) -> io::Result<()> {
        self.listener.reregister(selector, token, interest, opts)
    }

    #[inline]
    fn deregister(&self, selector: &mut Selector) -> io::Result<()> {
        self.listener.deregister(selector)
    }
}

/// A connector that can protect HTTP streams using SSL.
#[derive(Debug, Default)]
pub struct HttpsConnector<S: Ssl> {
    ssl: S
}

impl<S: Ssl> HttpsConnector<S> {
    /// Create a new connector using the provided SSL implementation.
    pub fn new(s: S) -> HttpsConnector<S> {
        HttpsConnector { ssl: s }
    }
}

fn _assert_transport() {
    fn _assert<T: Transport>() {}
    _assert::<HttpsStream<HttpStream>>();
}

/*
impl<S: Ssl> Connect for HttpsConnector<S> {
    type Stream = HttpsStream<<S as Ssl>::Stream>;

    fn connect(&self, host: &str, port: u16, scheme: &str) -> ::Result<Self::Stream> {
        let addr = (host, port).to_socket_addrs().unwrap().next().unwrap();
        if scheme == "https" {
            debug!("https scheme");
            let stream = try!(TcpStream::connect(&addr));
            self.ssl.wrap_client(stream, host).map(HttpsStream::Https)
        } else {
            HttpConnector.connect(host, port, scheme).map(HttpsStream::Http)
        }
    }
}
*/


#[cfg(not(feature = "openssl"))]
#[doc(hidden)]
pub type DefaultConnector = HttpConnector;

#[cfg(feature = "openssl")]
#[doc(hidden)]
pub type DefaultConnector = HttpsConnector<self::openssl::Openssl>;

#[cfg(feature = "openssl")]
mod openssl {
    use std::io;
    use std::net::{SocketAddr, Shutdown};
    use std::path::Path;
    use std::sync::Arc;

    use mio::tcp::TcpStream;

    use openssl::ssl::{Ssl, SslContext, SslStream, SslMethod, SSL_VERIFY_NONE};
    use openssl::ssl::error::StreamError as SslIoError;
    use openssl::ssl::error::SslError;
    use openssl::x509::X509FileType;

    /// An implementation of `Ssl` for OpenSSL.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use hyper::Server;
    /// use hyper::net::Openssl;
    ///
    /// let ssl = Openssl::with_cert_and_key("/home/foo/cert", "/home/foo/key").unwrap();
    /// Server::https("0.0.0.0:443", ssl).unwrap();
    /// ```
    ///
    /// For complete control, create a `SslContext` with the options you desire
    /// and then create `Openssl { context: ctx }
    #[derive(Debug, Clone)]
    pub struct Openssl {
        /// The `SslContext` from openssl crate.
        pub context: Arc<SslContext>
    }

    impl Default for Openssl {
        fn default() -> Openssl {
            Openssl {
                context: Arc::new(SslContext::new(SslMethod::Sslv23).unwrap_or_else(|e| {
                    // if we cannot create a SslContext, that's because of a
                    // serious problem. just crash.
                    panic!("{}", e)
                }))
            }
        }
    }

    impl Openssl {
        /// Ease creating an `Openssl` with a certificate and key.
        pub fn with_cert_and_key<C, K>(cert: C, key: K) -> Result<Openssl, SslError>
        where C: AsRef<Path>, K: AsRef<Path> {
            let mut ctx = try!(SslContext::new(SslMethod::Sslv23));
            try!(ctx.set_cipher_list("DEFAULT"));
            try!(ctx.set_certificate_file(cert.as_ref(), X509FileType::PEM));
            try!(ctx.set_private_key_file(key.as_ref(), X509FileType::PEM));
            ctx.set_verify(SSL_VERIFY_NONE, None);
            Ok(Openssl { context: Arc::new(ctx) })
        }
    }

    impl super::Ssl for Openssl {
        type Stream = SslStream<TcpStream>;

        fn wrap_client(&self, stream: TcpStream, host: &str) -> ::Result<Self::Stream> {
            let ssl = try!(Ssl::new(&self.context));
            try!(ssl.set_hostname(host));
            SslStream::connect(ssl, stream).map_err(From::from)
        }

        fn wrap_server(&self, stream: TcpStream) -> ::Result<Self::Stream> {
            match SslStream::accept(&*self.context, stream) {
                Ok(ssl_stream) => Ok(ssl_stream),
                Err(SslIoError(e)) => {
                    Err(io::Error::new(io::ErrorKind::ConnectionAborted, e).into())
                },
                Err(e) => Err(e.into())
            }
        }
    }

}

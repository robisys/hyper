//! HTTP Server
//!
//! # Server
//!
//! A `Server` is created to listen on port, parse HTTP requests, and hand
//! them off to a `Handler`.
//!
//! # Handling requests
//!
//! You must pass a `Handler` to the Server that will handle requests. There is
//! a default implementation for `fn`s and closures, allowing you pass one of
//! those easily.
//!
//!
//! ```no_run
//! use hyper::server::{Server, Request, Response};
//!
//! fn hello(req: Request, res: Response) {
//!     // handle things here
//! }
//!
//! Server::http("0.0.0.0:0").unwrap().handle(hello).unwrap();
//! ```
//!
//! As with any trait, you can also define a struct and implement `Handler`
//! directly on your own type, and pass that to the `Server` instead.
//!
//! ```no_run
//! use std::sync::Mutex;
//! use std::sync::mpsc::{channel, Sender};
//! use hyper::server::{Handler, Server, Request, Response};
//!
//! struct SenderHandler {
//!     sender: Mutex<Sender<&'static str>>
//! }
//!
//! impl Handler for SenderHandler {
//!     fn handle(&self, req: Request, res: Response) {
//!         self.sender.lock().unwrap().send("start").unwrap();
//!     }
//! }
//!
//!
//! let (tx, rx) = channel();
//! Server::http("0.0.0.0:0").unwrap().handle(SenderHandler {
//!     sender: Mutex::new(tx)
//! }).unwrap();
//! ```
//!
//! # The `Request` and `Response` pair
//!
//! A `Handler` receives a pair of arguments, a `Request` and a `Response`. The
//! `Request` includes access to the `method`, `uri`, and `headers` of the
//! incoming HTTP request. It also implements `std::io::Read`, in order to
//! read any body, such as with `POST` or `PUT` messages.
//!
//! Likewise, the `Response` includes ways to set the `status` and `headers`,
//! and implements `std::io::Write` to allow writing the response body.
//!
//! ```no_run
//! use std::io;
//! use hyper::server::{Server, Request, Response};
//! use hyper::status::StatusCode;
//!
//! Server::http("0.0.0.0:0").unwrap().handle(|mut req: Request, mut res: Response| {
//!     match req.method {
//!         hyper::Post => {
//!             io::copy(&mut req, &mut res.start().unwrap()).unwrap();
//!         },
//!         _ => *res.status_mut() = StatusCode::MethodNotAllowed
//!     }
//! }).unwrap();
//! ```
use std::error::Error as StdError;
use std::fmt;
use std::marker::PhantomData;
use std::net::{SocketAddr/*, ToSocketAddrs*/};
use std::sync::mpsc;
use std::thread;

use std::time::Duration;

//use num_cpus;

use mio::{TryAccept, Evented, EventSet, PollOpt};
use rotor::{self, Scope};

pub use self::request::Request;
pub use self::response::Response;

use http::{self, Next};
//use net::{HttpsListener, Ssl, HttpsStream};
use net::{HttpListener, HttpStream, Transport};


mod request;
mod response;
mod message;

/// A server can listen on a TCP socket.
///
/// Once listening, it will create a `Request`/`Response` pair for each
/// incoming connection, and hand them to the provided handler.
#[derive(Debug)]
pub struct Server<T: TryAccept + Evented> {
    listener: T,
    timeouts: Timeouts,
}

#[derive(Clone, Copy, Debug, Default)]
struct Timeouts {
    read: Option<Duration>,
    write: Option<Duration>,
    keep_alive: Option<Duration>,
}

macro_rules! try_option(
    ($e:expr) => {{
        match $e {
            Some(v) => v,
            None => return None
        }
    }}
);

impl<T> Server<T> where T: TryAccept + Evented, <T as TryAccept>::Output: Transport {
    /// Creates a new server with the provided handler.
    #[inline]
    pub fn new(listener: T) -> Server<T> {
        Server {
            listener: listener,
            timeouts: Timeouts::default()
        }
    }

    /// Controls keep-alive for this server.
    ///
    /// The timeout duration passed will be used to determine how long
    /// to keep the connection alive before dropping it.
    ///
    /// Passing `None` will disable keep-alive.
    ///
    /// Default is enabled with a 5 second timeout.
    #[inline]
    pub fn keep_alive(&mut self, timeout: Option<Duration>) {
        self.timeouts.keep_alive = timeout;
    }

    /// Sets the read timeout for all Request reads.
    pub fn set_read_timeout(&mut self, dur: Option<Duration>) {
        self.timeouts.read = dur;
    }

    /// Sets the write timeout for all Response writes.
    pub fn set_write_timeout(&mut self, dur: Option<Duration>) {
        self.timeouts.write = dur;
    }
}

impl Server<HttpListener> {
    pub fn http(addr: &str) -> ::Result<Server<HttpListener>> {
        use ::mio::tcp::TcpListener;
        TcpListener::bind(&addr.parse().unwrap())
            .map(HttpListener)
            .map(Server::new)
            .map_err(From::from)
    }
}


/*
impl<S: Ssl> Server<HttpsStream<S::Stream>> {
    /// Creates a new server that will handle `HttpStream`s over SSL.
    ///
    /// You can use any SSL implementation, as long as implements `hyper::net::Ssl`.
    pub fn https(addr: &SocketAddr, ssl: S) -> ::Result<Server<HttpsListener<S>>> {
        HttpsListener::new(addr, ssl).map(Server::new)
    }
}
*/


//impl<T: Transport> Server<T> {
impl Server<HttpListener> {
    /// Binds to a socket and starts handling connections.
    pub fn handle<H>(self, mut factory: H) -> ::Result<Listening>
    where H: HandlerFactory<HttpStream> {
        let addr = try!(self.listener.0.local_addr());
        let (notifier_tx, notifier_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let listener = self.listener;
        let handle = try!(thread::Builder::new().name("hyper-server".to_owned()).spawn(move || {
            let mut loop_ = rotor::Loop::new(&rotor::Config::new()).unwrap();
            loop_.add_machine_with(move |scope| {
                try!(notifier_tx.send(scope.notifier()));
                try!(scope.register(&listener, EventSet::readable(), PollOpt::level()));
                Ok(ServerFsm::Listener::<HttpListener, _, H>(listener, shutdown_rx, PhantomData))
            }).unwrap();
            loop_.run(move || {
                message::Message::new(factory.create())
            }).unwrap();
        }));

        let notifier = notifier_rx.recv().unwrap();

        Ok(Listening {
            addr: addr,
            shutdown: (shutdown_tx, notifier),
            handle: Some(handle),
        })
    }
}

enum ServerFsm<A, F, H>
where A: TryAccept + rotor::Evented,
      A::Output: Transport,
      F: http::MessageHandlerFactory<A::Output, Output=message::Message<H::Output, A::Output>>,
      H: HandlerFactory<A::Output> {
    Listener(A, mpsc::Receiver<Shutdown>, PhantomData<F>),
    Conn(http::Conn<A::Output, message::Message<H::Output, A::Output>>)
}

impl<A, F, H> rotor::Machine for ServerFsm<A, F, H>
where A: TryAccept + rotor::Evented,
      A::Output: Transport,
      F: http::MessageHandlerFactory<A::Output, Output=message::Message<H::Output, A::Output>>,
      H: HandlerFactory<A::Output> {
    type Context = F;
    type Seed = A::Output;

    fn create(seed: Self::Seed, scope: &mut Scope<F>) -> Result<Self, Box<StdError>> {
        try!(scope.register(&seed, EventSet::readable(), PollOpt::level()));
        Ok(ServerFsm::Conn(http::Conn::new(seed)))
    }

    fn ready(self, events: EventSet, scope: &mut Scope<F>) -> rotor::Response<Self, Self::Seed> {
        match self {
            ServerFsm::Listener(listener, rx, m) => {
                match listener.accept() {
                    Ok(Some(conn)) => {
                        rotor::Response::spawn(ServerFsm::Listener(listener, rx, m), conn)
                    },
                    Ok(None) => rotor::Response::ok(ServerFsm::Listener(listener, rx, m)),
                    Err(e) => {
                        error!("listener accept error {}", e);
                        // usually fine, just keep listening
                        rotor::Response::ok(ServerFsm::Listener(listener, rx, m))
                    }
                }
            },
            ServerFsm::Conn(conn) => {
                if events.is_error() {
                    error!("Conn error event");
                }

                match conn.ready(events, scope) {
                    Some(conn) => rotor::Response::ok(ServerFsm::Conn(conn)),
                    None => rotor::Response::done()
                }
            }
        }
    }

    fn spawned(self, _scope: &mut Scope<F>) -> rotor::Response<Self, Self::Seed> {
        match self {
            ServerFsm::Listener(listener, rx, m) => {
                match listener.accept() {
                    Ok(Some(conn)) => {
                        rotor::Response::spawn(ServerFsm::Listener(listener, rx, m), conn)
                    },
                    Ok(None) => rotor::Response::ok(ServerFsm::Listener(listener, rx, m)),
                    Err(e) => {
                        error!("listener accept error {}", e);
                        // usually fine, just keep listening
                        rotor::Response::ok(ServerFsm::Listener(listener, rx, m))
                    }
                }
            },
            sock => rotor::Response::ok(sock)
        }

    }

    fn timeout(self, _scope: &mut Scope<F>) -> rotor::Response<Self, Self::Seed> {
        unimplemented!("timeout")
    }

    fn wakeup(self, scope: &mut Scope<F>) -> rotor::Response<Self, Self::Seed> {
        match self {
            ServerFsm::Listener(lst, shutdown, m) => {
                if shutdown.try_recv().is_ok() {
                    let _ = scope.deregister(&lst);
                    scope.shutdown_loop();
                    rotor::Response::done()
                } else {
                    rotor::Response::ok(ServerFsm::Listener(lst, shutdown, m))
                }
            },
            conn => rotor::Response::ok(conn)
        }
    }
}

#[derive(Debug)]
struct Shutdown;

/// A handle of the running server.
pub struct Listening {
    /// The address this server is listening on.
    pub addr: SocketAddr,
    shutdown: (mpsc::Sender<Shutdown>, rotor::Notifier),
    handle: Option<::std::thread::JoinHandle<()>>,
}

impl fmt::Debug for Listening {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Listening")
            .field("addr", &self.addr)
            .finish()
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        self.handle.take().map(|handle| {
            handle.join().unwrap();
        });
    }
}

impl Listening {
    /// Starts the Server, blocking until it is shutdown.
    pub fn forever(self) {

    }
    /// Stop the server from listening to its socket address.
    pub fn close(self) {
        debug!("closing server");
        self.shutdown.0.send(Shutdown).unwrap();
        self.shutdown.1.wakeup().unwrap();
    }
}

pub trait Handler<T: Transport>: Send + 'static {
    fn on_request(&mut self, request: Request) -> Next;
    fn on_request_readable(&mut self, request: &mut http::Decoder<T>) -> Next;
    fn on_response(&mut self, response: &mut Response) -> Next;
    fn on_response_writable(&mut self, response: &mut http::Encoder<T>) -> Next;
}


pub trait HandlerFactory<T: Transport>: Send + 'static {
    type Output: Handler<T>;
    fn create(&mut self) -> Self::Output;
}

impl<F, H, T> HandlerFactory<T> for F
where F: FnMut() -> H + Send + 'static, H: Handler<T>, T: Transport {
    type Output = H;
    fn create(&mut self) -> H {
        self()
    }
}

#[cfg(test)]
mod tests {
    /*
    use header::Headers;
    use method::Method;
    use mock::MockStream;
    use status::StatusCode;
    use uri::RequestUri;

    use super::{Request, Response, Fresh, Handler, Worker};

    #[test]
    fn test_check_continue_default() {
        let mut mock = MockStream::with_input(b"\
            POST /upload HTTP/1.1\r\n\
            Host: example.domain\r\n\
            Expect: 100-continue\r\n\
            Content-Length: 10\r\n\
            \r\n\
            1234567890\
        ");

        fn handle(_: Request, res: Response<Fresh>) {
            res.start().unwrap().end().unwrap();
        }

        Worker::new(handle, Default::default()).handle_connection(&mut mock);
        let cont = b"HTTP/1.1 100 Continue\r\n\r\n";
        assert_eq!(&mock.write[..cont.len()], cont);
        let res = b"HTTP/1.1 200 OK\r\n";
        assert_eq!(&mock.write[cont.len()..cont.len() + res.len()], res);
    }

    #[test]
    fn test_check_continue_reject() {
        struct Reject;
        impl Handler for Reject {
            fn handle(&self, _: Request, res: Response<Fresh>) {
                res.start().unwrap().end().unwrap();
            }

            fn check_continue(&self, _: (&Method, &RequestUri, &Headers)) -> StatusCode {
                StatusCode::ExpectationFailed
            }
        }

        let mut mock = MockStream::with_input(b"\
            POST /upload HTTP/1.1\r\n\
            Host: example.domain\r\n\
            Expect: 100-continue\r\n\
            Content-Length: 10\r\n\
            \r\n\
            1234567890\
        ");

        Worker::new(Reject, Default::default()).handle_connection(&mut mock);
        assert_eq!(mock.write, &b"HTTP/1.1 417 Expectation Failed\r\n\r\n"[..]);
    }
    */
}

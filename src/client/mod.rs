//! HTTP Client
use std::default::Default;
use std::error::Error as StdError;
use std::io::{Read};
use std::marker::PhantomData;
use std::sync::mpsc;
use std::thread;

use rotor::{self, Scope, EventSet, PollOpt};

use url::ParseError as UrlError;

use header::Host;
use http::{self, Next, RequestHead};
use net::{Transport, Connect, DefaultConnector};
use uri::RequestUri;
use {Url};
use Error;

pub use self::request::Request;
pub use self::response::Response;

//mod pool;
mod request;
mod response;

/// A Client to use additional features with Requests.
///
/// Clients can handle things such as: redirect policy, connection pooling.
pub struct Client {
    handle: Option<thread::JoinHandle<()>>,
    notifier: (rotor::Notifier, mpsc::Sender<Notify>),
}


impl Client {
    /// Create a new Client.
    pub fn new<F>(factory: F) -> ::Result<Client>
    where F: HandlerFactory<<DefaultConnector as Connect>::Output>{
        Client::with_connector(DefaultConnector::default(), factory)
    }

    /// Create a new client with a specific connector.
    pub fn with_connector<F, C>(connector: C, factory: F) -> ::Result<Client>
    where C: Connect + Send + 'static,
          C::Output: Transport + Send + 'static,
          F: HandlerFactory<C::Output> {
        let mut loop_ = try!(rotor::Loop::new(&rotor::Config::new()));
        let (tx, rx) = mpsc::channel();
        let mut notifier = None;
        {
            let not = &mut notifier;
            loop_.add_machine_with(move |scope| {
                *not = Some(scope.notifier());
                Ok(ClientFsm::Connector(connector, rx, PhantomData))
            }).unwrap();
        }

        let notifier = notifier.expect("loop.add_machine_with failed");
        let handle = try!(thread::Builder::new().name("hyper-client".to_owned()).spawn(move || {
            loop_.run(Context {
                factory: factory,
                queue: Vec::new(),
                _marker: PhantomData,
            }).unwrap()
        }));

        Ok(Client {
            handle: Some(handle),
            notifier: (notifier, tx),
        })
    }

    /// Build a new request using this Client.
    pub fn request(&self, url: Url) {
        self.notifier.1.send(Notify::Connect(url))
            .expect("event loop has panicked");
        self.notifier.0.wakeup()
            .expect("event loop notify queue is full, cannot recover");
    }

    /// Close the Client loop.
    pub fn close(self) {
        // Most errors mean that the Receivers are already dead, which would
        // imply the EventLoop panicked.
        let _ = self.notifier.1.send(Notify::Shutdown);
        let _ = self.notifier.0.wakeup();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.handle.take().map(|handle| handle.join());
    }
}

pub trait Handler<T: Transport>: Send + 'static {
    fn on_request(&mut self, request: &mut Request) -> http::Next;
    fn on_request_writable(&mut self, request: &mut http::Encoder<T>) -> http::Next;
    fn on_response(&mut self, response: Response) -> http::Next;
    fn on_response_readable(&mut self, response: &mut http::Decoder<T>) -> http::Next;

    fn on_error(self, err: ::Error) where Self: Sized {
        debug!("default Handler.on_error({:?})", err);
    }
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

struct UrlParts {
    host: String,
    port: u16,
    path: RequestUri,
}

struct Message<H: Handler<T>, T: Transport> {
    handler: H,
    url: Option<UrlParts>,
    _marker: PhantomData<T>,
}

impl<H: Handler<T>, T: Transport> http::MessageHandler<T> for Message<H, T> {
    type Message = http::ClientMessage;

    fn on_outgoing(&mut self, head: &mut RequestHead) -> Next {
        let url = self.url.take().expect("Message.url is missing");
        head.headers.set(Host {
            hostname: url.host,
            port: Some(url.port),
        });
        head.subject.1 = url.path;
        let mut req = self::request::new(head);
        self.handler.on_request(&mut req)
    }

    fn on_encode(&mut self, transport: &mut http::Encoder<T>) -> Next {
        self.handler.on_request_writable(transport)
    }

    fn on_incoming(&mut self, head: http::ResponseHead) -> Next {
        trace!("on_incoming {:?}", head);
        let resp = response::new(head);
        self.handler.on_response(resp)
    }

    fn on_decode(&mut self, transport: &mut http::Decoder<T>) -> Next {
        self.handler.on_response_readable(transport)
    }
}

struct Context<F: HandlerFactory<T>, T: Transport> {
    queue: Vec<UrlParts>,
    factory: F,
    _marker: PhantomData<T>,
}

impl<F: HandlerFactory<T>, T: Transport> http::MessageHandlerFactory<T> for Context<F, T> {
    type Output = Message<F::Output, T>;

    fn create(&mut self) -> Self::Output {
        Message {
            handler: self.factory.create(),
            url: Some(self.queue.remove(0)),
            _marker: PhantomData,
        }
    }
}

enum Notify {
    Connect(Url),
    Shutdown,
}

enum ClientFsm<C, F, H>
where C: Connect,
      C::Output: Transport,
      F: HandlerFactory<C::Output, Output=H>,
      H: Handler<C::Output> {
    Connector(C, mpsc::Receiver<Notify>, PhantomData<F>),
    Socket(http::Conn<C::Output, Message<H, C::Output>>)
}

impl<C, F, H> rotor::Machine for ClientFsm<C, F, H>
where C: Connect,
      C::Output: Transport,
      F: HandlerFactory<C::Output, Output=H>,
      H: Handler<C::Output> {
    type Context = Context<F, C::Output>;
    type Seed = C::Output;

    fn create(seed: Self::Seed, scope: &mut Scope<Self::Context>) -> Result<Self, Box<StdError>> {
        try!(scope.register(&seed, EventSet::writable(), PollOpt::level()));
        Ok(ClientFsm::Socket(http::Conn::new(seed)))
    }

    fn ready(self, events: EventSet, scope: &mut Scope<Self::Context>) -> rotor::Response<Self, Self::Seed> {
        match self {
            ClientFsm::Connector(..) => {
                unreachable!()
            },
            ClientFsm::Socket(conn) => {
                if events.is_error() {
                    error!("Socket error event");
                }

                match conn.ready(events, scope) {
                    Some(conn) => rotor::Response::ok(ClientFsm::Socket(conn)),
                    None => rotor::Response::done()
                }
            }
        }
    }

    fn spawned(self, _: &mut Scope<Self::Context>) -> rotor::Response<Self, Self::Seed> {
        rotor::Response::ok(self)
    }

    fn timeout(self, _: &mut Scope<Self::Context>) -> rotor::Response<Self, Self::Seed> {
        unimplemented!("timeout")
    }

    fn wakeup(self, scope: &mut Scope<Self::Context>) -> rotor::Response<Self, Self::Seed> {
        match self {
            ClientFsm::Connector(connector, rx, m) => {
                match rx.try_recv() {
                    Ok(Notify::Connect(url)) => {
                        // TODO: check pool for sockets to this domain
                        let (host, port) = match get_host_and_port(&url) {
                            Ok(v) => v,
                            Err(e) => {
                                scope.factory.create().on_error(e.into());
                                return rotor::Response::ok(ClientFsm::Connector(connector, rx, m));
                            }
                        };
                        let socket = match connector.connect(&host, port, &url.scheme) {
                            Ok(v) => v,
                            Err(e) => {
                                scope.factory.create().on_error(e.into());
                                return rotor::Response::ok(ClientFsm::Connector(connector, rx, m));
                            }
                        };
                        scope.queue.push(UrlParts {
                            host: host,
                            port: port,
                            path: RequestUri::AbsolutePath(url.serialize_path().unwrap())
                        });
                        rotor::Response::spawn(ClientFsm::Connector(connector, rx, m), socket)
                    }
                    Ok(Notify::Shutdown) => {
                        scope.shutdown_loop();
                        rotor::Response::done()
                    },
                    Err(mpsc::TryRecvError::Disconnected) => {
                        unimplemented!("Connector notifier disconnected");
                    }
                    Err(mpsc::TryRecvError::Empty) => {
                        // spurious wakeup
                        rotor::Response::ok(ClientFsm::Connector(connector, rx, m))
                    }
                }
            },
            other => rotor::Response::ok(other)
        }
    }
}

/// A helper trait to convert common objects into a Url.
pub trait IntoUrl {
    /// Consumes the object, trying to return a Url.
    fn into_url(self) -> Result<Url, UrlError>;
}

impl IntoUrl for Url {
    fn into_url(self) -> Result<Url, UrlError> {
        Ok(self)
    }
}

impl<'a> IntoUrl for &'a str {
    fn into_url(self) -> Result<Url, UrlError> {
        Url::parse(self)
    }
}

impl<'a> IntoUrl for &'a String {
    fn into_url(self) -> Result<Url, UrlError> {
        Url::parse(self)
    }
}

fn get_host_and_port(url: &Url) -> ::Result<(String, u16)> {
    let host = match url.serialize_host() {
        Some(host) => host,
        None => return Err(Error::Uri(UrlError::EmptyHost))
    };
    trace!("host={:?}", host);
    let port = match url.port_or_default() {
        Some(port) => port,
        None => return Err(Error::Uri(UrlError::InvalidPort))
    };
    trace!("port={:?}", port);
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    /*
    use std::io::Read;
    use header::Server;
    use super::{Client};
    use super::pool::Pool;
    use url::Url;

    mock_connector!(Issue640Connector {
        b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n",
        b"GET",
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n",
        b"HEAD",
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n",
        b"POST"
    });

    // see issue #640
    #[test]
    fn test_head_response_body_keep_alive() {
        let client = Client::with_connector(Pool::with_connector(Default::default(), Issue640Connector));

        let mut s = String::new();
        client.get("http://127.0.0.1").send().unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "GET");

        let mut s = String::new();
        client.head("http://127.0.0.1").send().unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "");

        let mut s = String::new();
        client.post("http://127.0.0.1").send().unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "POST");
    }
    */
}

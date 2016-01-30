use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::time::Duration;

use rotor::{EventSet, PollOpt, Scope};

use http::{self, h1, Http1Message, Encoder, Decoder};
use http::write_buf::WriteBuf;
use http::buffer::Buffer;
use net::Transport;

const MAX_BUFFER_SIZE: usize = 8192 + 4096 * 100;

/// This handles a connection, which will have been established over a
/// Transport (like a socket), and will likely include multiple
/// `Message`s over HTTP.
///
/// The connection will determine when a message begins and ends, creating
/// a new message `MessageHandler` for each one, as well as determine if this
/// connection can be kept alive after the message, or if it is complete.
pub struct Conn<T: Transport, H: MessageHandler<T>> {
    buf: Buffer,
    state: State<H, T>,
    transport: T,
}

impl<T: Transport, H: MessageHandler<T>> Conn<T, H> {
    pub fn new(transport: T) -> Conn<T, H> {
        Conn {
            buf: Buffer::new(),
            state: State::Init,
            transport: transport,
        }
    }

    fn interest(&mut self) -> Reg {
        match self.state {
            State::Closed => Reg::Remove,
            State::Init => {
                <H as MessageHandler>::Message::initial_interest().interest()
            }
            State::Http1(Http1 { reading: Reading::Closed, writing: Writing::Closed, .. }) => {
                Reg::Remove
            }
            State::Http1(Http1 { ref reading, ref writing, .. }) => {
                let read = match *reading {
                    Reading::Parse |
                    Reading::Body(..) => Reg::Read,
                    _ => Reg::Wait
                };

                let write = match *writing {
                    Writing::Head |
                    Writing::Chunk(..) |
                    Writing::Ready(..) => Reg::Write,
                    _ => Reg::Wait
                };

                match (read, write) {
                    (Reg::Read, Reg::Write) => Reg::ReadWrite,
                    (Reg::Read, Reg::Wait) => Reg::Read,
                    (Reg::Wait, Reg::Write) => Reg::Write,
                    (Reg::Wait, Reg::Wait) => Reg::Wait,
                    _ => unreachable!()
                }
            }
            //_ => Next_::ReadWrite,
        }
    }

    fn read<F: MessageHandlerFactory<T, Output=H>>(&mut self, factory: &mut F, state: State<H, T>) -> State<H, T> {
         match state {
            State::Init => {
                match self.buf.read_from(&mut self.transport) {
                    Ok(0) => {
                        trace!("on_readable Init eof");
                        return State::Closed;
                    }
                    Ok(_) => {},
                    Err(e) => match e.kind() {
                        io::ErrorKind::WouldBlock |
                        io::ErrorKind::Interrupted => return State::Init,
                        _ => {
                            debug!("io error trying to parse {:?}", e);
                            return State::Closed;
                        }
                    }
                }
                match http::parse::<<H as MessageHandler<T>>::Message, _>(self.buf.bytes()) {
                    Ok(Some((head, len))) => {
                        trace!("parsed {} bytes out of {}", len, self.buf.len());
                        self.buf.consume(len);
                        match <<H as MessageHandler<T>>::Message as Http1Message>::decoder(&head) {
                            Ok(decoder) => {
                                let req_keep_alive = head.should_keep_alive();
                                let mut handler = factory.create();
                                let next = handler.on_incoming(head);
                                trace!("handler.on_incoming() -> {:?}", next);
                                match next.interest {
                                    Next_::Read => self.read(factory, State::Http1(Http1 {
                                        handler: handler,
                                        reading: Reading::Body(decoder),
                                        writing: Writing::Init,
                                        keep_alive: req_keep_alive,
                                        _marker: PhantomData,
                                    })),
                                    Next_::Write => State::Http1(Http1 {
                                        handler: handler,
                                        reading: if decoder.is_eof() {
                                            if req_keep_alive {
                                                Reading::KeepAlive
                                            } else {
                                                Reading::Closed
                                            }
                                        } else {
                                            Reading::Wait(decoder)
                                        },
                                        writing: Writing::Head,
                                        keep_alive: req_keep_alive,
                                        _marker: PhantomData,
                                    }),
                                    Next_::ReadWrite => self.read(factory, State::Http1(Http1 {
                                        handler: handler,
                                        reading: Reading::Body(decoder),
                                        writing: Writing::Head,
                                        keep_alive: req_keep_alive,
                                        _marker: PhantomData,
                                    })),
                                    Next_::Wait => unimplemented!("parsed and now Wait"),
                                    Next_::End |
                                    Next_::Remove => State::Closed,
                                }
                            },
                            Err(e) => {
                                debug!("error creating decoder: {:?}", e);
                                //TODO: respond with 400
                                State::Closed
                            }
                        }
                    },
                    Ok(None) => {
                        if self.buf.len() >= MAX_BUFFER_SIZE {
                            //TODO: Handler.on_too_large_error()
                            debug!("MAX_BUFFER_SIZE reached, closing");
                            State::Closed
                        } else {
                            State::Init
                        }
                    },
                    Err(e) => {
                        trace!("parsing error: {:?}", e);
                        State::Closed
                        /* TODO: h2
                        let h2_init = b"PRI * HTTP/2";
                        if self.buf.bytes().starts_with(h2_init) {
                            trace!("HTTP/2 request!");
                            self.state = State::Http2(h2::conn());
                            Some(Reading::Closed)
                        } else {
                            //TODO: match on error to send proper response
                            //TODO: have Handler.on_parse_error() or something
                            Some(Reading::Closed)
                        }
                        */
                    }
                }
            },
            State::Http1(mut http1) => {
                let next = match http1.reading {
                    Reading::Body(ref mut decoder) => {
                        let wrapped = if !self.buf.is_empty() {
                            super::Trans::Buf(self.buf.wrap(&mut self.transport))
                        } else {
                            super::Trans::Port(&mut self.transport)
                        };

                        http1.handler.on_decode(&mut Decoder::h1(decoder, wrapped))
                    },
                    _ => unimplemented!("Conn.on_readable State::Http1(reading = {:?})", http1.reading)
                };
                let mut s = State::Http1(http1);
                s.update(next);
                s
            },
            State::Closed => {
                error!("on_readable State::Closed");
                State::Closed
            }

        }
    }

    fn write<F: MessageHandlerFactory<T, Output=H>>(&mut self, _factory: &mut F, mut state: State<H, T>) -> State<H, T> {
        let next = match state {
            State::Init => {
                // this could be a Client request, which writes first, so pay
                // attention to the version written here, which will adjust
                // our internal state to Http1 or Http2
                unimplemented!("Conn.on_writable State::Init")
            }
            State::Http1(Http1 { ref mut handler, ref mut writing, ref mut keep_alive, .. }) => {
                match *writing {
                    Writing::Init => {
                        unimplemented!("Conn.on_writable Http1::Writing::Init");
                    }
                    Writing::Head => {
                        let mut head = http::MessageHead::default();
                        let interest = handler.on_outgoing(&mut head);
                        // if the request wants to close, server cannot stop it
                        if *keep_alive {
                            // if the request wants to stay alive, then it depends
                            // on the server to agree
                            *keep_alive = head.should_keep_alive();
                        }
                        let mut buf = Vec::new();
                        let mut encoder = <<H as MessageHandler<T>>::Message as Http1Message>::encode(head, &mut buf);
                        *writing = match interest.interest {
                            // user wants to write some data right away
                            // try to write the headers and the first chunk
                            // together, so they are in the same packet
                            Next_::Write |
                            Next_::ReadWrite => {
                                encoder.prefix(WriteBuf {
                                    bytes: buf,
                                    pos: 0
                                });
                                Writing::Ready(encoder)
                            },
                            _ => Writing::Chunk(Chunk {
                                buf: buf,
                                pos: 0,
                                next: (encoder, interest.clone())
                            })
                        };
                        Some(interest)
                    },
                    Writing::Chunk(ref mut chunk) => {
                        match self.transport.write(&chunk.buf[chunk.pos..]) {
                            Ok(n) => {
                                chunk.pos += n;
                                if chunk.pos >= chunk.buf.len() {
                                    Some(chunk.next.1.clone())
                                } else {
                                    None
                                }
                            },
                            Err(e) => match e.kind() {
                                io::ErrorKind::WouldBlock |
                                io::ErrorKind::Interrupted => None,
                                _ => {
                                    error!("io error writing chunk: {}", e);
                                    return State::Closed;
                                }
                            }
                        }
                    },
                    Writing::Ready(ref mut encoder) => {
                        Some(handler.on_encode(&mut Encoder::h1(encoder, &mut self.transport)))
                        //TODO: if encoder.chunked() { *writing = Chunk }
                    },
                    Writing::Wait(..) => {
                        unimplemented!("Conn.on_writable Http1::Writing::Wait");
                    }
                    Writing::KeepAlive => {
                        unimplemented!("Conn.on_writable Http1::Writing::KeepAlive");
                    }
                    Writing::Closed => {
                        error!("on_writable Http1::Writing::Closed");
                        None
                    }
                }
            },
            State::Closed => {
                error!("on_writable State::Closed");
                None
            }
        };

        if let Some(next) = next {
            state.update(next);
        }
        state
    }

    pub fn ready<F>(mut self, events: EventSet, scope: &mut Scope<F>) -> Option<Self>
    where F: MessageHandlerFactory<T, Output=H> {
        if events.is_readable() {
            self.on_readable(scope);
        }

        if events.is_writable() {
            self.on_writable(scope);
        }

        let events = match self.interest() {
            Reg::Read => EventSet::readable(),
            Reg::Write => EventSet::writable(),
            Reg::ReadWrite => EventSet::readable() | EventSet::writable(),
            Reg::Wait => EventSet::none(),
            Reg::Remove => {
                let _ = scope.deregister(&self.transport);
                return None;
            },
        };
        match scope.reregister(&self.transport, events, PollOpt::level()) {
            Ok(..) => Some(self),
            Err(e) => {
                error!("error reregistering: {:?}", e);
                None
            }
        }
    }

    fn on_readable<F>(&mut self, scope: &mut Scope<F>)
    where F: MessageHandlerFactory<T, Output=H> {
        trace!("on_readable -> {:?}", self.state);
        let state = mem::replace(&mut self.state, State::Closed);
        self.state = self.read(&mut **scope, state);
        trace!("on_readable <- {:?}", self.state);
    }

    fn on_writable<F>(&mut self, scope: &mut Scope<F>)
    where F: MessageHandlerFactory<T, Output=H> {
        trace!("on_writable -> {:?}", self.state);
        let state = mem::replace(&mut self.state, State::Closed);
        self.state = self.write(&mut **scope, state);
        trace!("on_writable <- {:?}", self.state);
    }
}

enum State<H: MessageHandler<T>, T: Transport> {
    Init,
    /// Http1 will only ever use a connection to send and receive a single
    /// message at a time. Once a H1 status has been determined, we will either
    /// be reading or writing an H1 message, and optionally multiple if
    /// keep-alive is true.
    Http1(Http1<H, T>),
    /// Http2 allows multiplexing streams over a single connection. So even
    /// when we've identified a certain message, we must always parse frame
    /// head to determine if the incoming frame is part of a current message,
    /// or a new one. This also means we could have multiple messages at once.
    //Http2 {},
    Closed,
}

impl<H: MessageHandler<T>, T: Transport> fmt::Debug for State<H, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            State::Init => f.write_str("Init"),
            State::Http1(ref h1) => f.debug_tuple("Http1")
                .field(h1)
                .finish(),
            State::Closed => f.write_str("Closed")
        }
    }
}

impl<H: MessageHandler<T>, T: Transport> State<H, T> {
    fn update(&mut self, next: Next) {
        let state = mem::replace(self, State::Closed);
        let new_state = match (state, next.interest) {
            (_, Next_::Remove) => State::Closed,
            (State::Closed, _) => State::Closed,
            (State::Init, _) => State::Init,
            (State::Http1(http1), Next_::End) => {
                match (http1.reading, http1.writing) {
                    (Reading::KeepAlive, Writing::KeepAlive) => State::Init,
                    (Reading::KeepAlive, Writing::Ready(ref encoder)) if encoder.is_eof() => State::Init,
                    (Reading::Body(ref decoder), Writing::KeepAlive) if decoder.is_eof() => State::Init,
                    (Reading::Body(ref decoder), Writing::Ready(ref encoder)) if encoder.is_eof() && decoder.is_eof() => State::Init,
                    _ => State::Closed
                }
            },
            (State::Http1(mut http1), Next_::Read) => {
                http1.reading = match http1.reading {
                    Reading::Wait(decoder) => Reading::Body(decoder),
                    same => same
                };

                http1.writing = match http1.writing {
                    Writing::Ready(encoder) => if encoder.is_eof() {
                        if http1.keep_alive {
                            Writing::KeepAlive
                        } else {
                            Writing::Closed
                        }
                    } else {
                        Writing::Wait(encoder)
                    },
                    same => same
                };

                State::Http1(http1)
            },
            (State::Http1(mut http1), Next_::Write) => {
                http1.writing = match http1.writing {
                    Writing::Wait(encoder) => Writing::Ready(encoder),
                    same => same
                };

                http1.reading = match http1.reading {
                    Reading::Body(decoder) => if decoder.is_eof() {
                        if http1.keep_alive {
                            Reading::KeepAlive
                        } else {
                            Reading::Closed
                        }
                    } else {
                        Reading::Wait(decoder)
                    },
                    same => same
                };
                State::Http1(http1)
            },
            (State::Http1(mut http1), Next_::ReadWrite) => {
                http1.reading = match http1.reading {
                    Reading::Wait(decoder) => Reading::Body(decoder),
                    same => same
                };
                http1.writing = match http1.writing {
                    Writing::Wait(encoder) => Writing::Ready(encoder),
                    same => same
                };
                State::Http1(http1)
            }
            (state, Next_::Wait) => state
        };
        mem::replace(self, new_state);
    }
}

// These Reading and Writing stuff should probably get moved into h1/message.rs

struct Http1<H, T> {
    handler: H,
    reading: Reading,
    writing: Writing,
    keep_alive: bool,
    _marker: PhantomData<T>,
}

impl<H, T> fmt::Debug for Http1<H, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Http1")
            .field("reading", &self.reading)
            .field("writing", &self.writing)
            .field("keep_alive", &self.keep_alive)
            .finish()
    }
}

#[derive(Debug)]
enum Reading {
    Init,
    Parse,
    Body(h1::Decoder),
    Wait(h1::Decoder),
    KeepAlive,
    Closed
}

#[derive(Debug)]
enum Writing {
    Init,
    Head,
    Chunk(Chunk) ,
    Ready(h1::Encoder),
    Wait(h1::Encoder),
    KeepAlive,
    Closed
}

#[derive(Debug)]
struct Chunk {
    buf: Vec<u8>,
    pos: usize,
    next: (h1::Encoder, Next),
}

pub trait MessageHandler<T: Transport> {
    type Message: Http1Message;
    fn on_incoming(&mut self, head: http::MessageHead<<Self::Message as Http1Message>::Incoming>) -> Next;
    fn on_outgoing(&mut self, head: &mut http::MessageHead<<Self::Message as Http1Message>::Outgoing>) -> Next;
    fn on_decode(&mut self, &mut http::Decoder<T>) -> Next;
    fn on_encode(&mut self, &mut http::Encoder<T>) -> Next;
}

pub trait MessageHandlerFactory<T: Transport> {
    type Output: MessageHandler<T>;

    fn create(&mut self) -> Self::Output;
}

impl<F, H, T> MessageHandlerFactory<T> for F
where F: FnMut() -> H, H: MessageHandler<T>, T: Transport {
    type Output = H;
    fn create(&mut self) -> H {
        self()
    }
}

#[must_use]
#[derive(Debug, Clone)]
pub struct Next {
    interest: Next_
}

#[derive(Debug, Clone, Copy)]
enum Next_ {
    Read,
    Write,
    ReadWrite,
    Wait,
    End,
    Remove,
}

#[derive(Debug, Clone, Copy)]
enum Reg {
    Read,
    Write,
    ReadWrite,
    Wait,
    Remove
}

impl Next {
    fn new(interest: Next_) -> Next {
        Next {
            interest: interest
        }
    }

    fn interest(&self) -> Reg {
        match self.interest {
            Next_::Read => Reg::Read,
            Next_::Write => Reg::Write,
            Next_::ReadWrite => Reg::ReadWrite,
            Next_::Wait => Reg::Wait,
            Next_::End => Reg::Remove,
            Next_::Remove => Reg::Remove,
        }
    }

    pub fn read() -> Next {
        Next::new(Next_::Read)
    }

    pub fn write() -> Next {
        Next::new(Next_::Write)
    }

    pub fn read_and_write() -> Next {
        Next::new(Next_::ReadWrite)
    }

    pub fn end() -> Next {
        Next::new(Next_::End)
    }

    pub fn wait() -> Next {
        Next::new(Next_::Wait)
    }

    /*
    pub fn remove() -> Next {
        Next::new(Next_:Remove)
    }
    */

    pub fn timeout(self, _dur: Duration) -> Next {
        unimplemented!("Next.timeout()")
    }
}



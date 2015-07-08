extern crate hyper;

use std::net::{TcpStream, SocketAddr};
use std::io::{Read, Write};
use std::sync::mpsc;

use hyper::http::{Next, Encoder, Decoder};
use hyper::net::HttpStream;
use hyper::server::{Server, Handler, Request, Response};

struct Serve {
    addr: SocketAddr,
    msg_rx: mpsc::Receiver<Msg>,
    close: mpsc::Sender<()>
}

impl Serve {
    fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /*
    fn head(&self) -> Request {
        unimplemented!()
    }
    */

    fn body(&self) -> Vec<u8> {
        let mut buf = vec![];
        while let Ok(Msg::Chunk(msg)) = self.msg_rx.try_recv() {
            buf.extend(&msg);
        }
        buf
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        self.close.send(()).unwrap();
    }
}

struct TestHandler {
    tx: mpsc::Sender<Msg>
}

enum Msg {
    //Head(Request),
    Chunk(Vec<u8>),
}

impl Handler<HttpStream> for TestHandler {
    fn on_request(&mut self, _req: Request) -> Next {
        //self.tx.send(Msg::Head(req)).unwrap();
        Next::read()
    }

    fn on_request_readable(&mut self, decoder: &mut Decoder<HttpStream>) -> Next {
        let mut vec = vec![0; 1024];
        match decoder.read(&mut vec) {
            Ok(0) => {
                Next::write()
            }
            Ok(n) => {
                vec.truncate(n);
                self.tx.send(Msg::Chunk(vec)).unwrap();
                Next::read()
            }
            Err(e) => {
                panic!("test error: {}", e);
            }
        }
    }

    fn on_response(&mut self, _res: &mut Response) -> Next {
        Next::end()
    }

    fn on_response_writable(&mut self, _encoder: &mut Encoder<HttpStream>) -> Next {
        Next::end()
    }
}

fn serve() -> Serve {
    use std::thread;

    let (msg_tx, msg_rx) = mpsc::channel();
    let (close_tx, close_rx) = mpsc::channel();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let handle = Server::http("127.0.0.1:0").unwrap()
            .handle(move || TestHandler { tx: msg_tx.clone() }).unwrap();
        tx.send(handle.addr).unwrap();

        close_rx.recv().unwrap();
        handle.close();
    });

    Serve {
        addr: rx.recv().unwrap(),
        close: close_tx,
        msg_rx: msg_rx,
    }
}

#[test]
fn server_get_should_ignore_body() {
    let server = serve();

    let mut req = TcpStream::connect(server.addr()).unwrap();
    req.write_all(b"\
        GET / HTTP/1.1\r\n\
        Host: example.domain\r\n\
        \r\n\
        I shouldn't be read.\r\n\
    ").unwrap();
    req.read(&mut [0; 256]).unwrap();

    assert_eq!(server.body(), b"");
}

#[test]
fn server_get_with_body() {
    let server = serve();
    let mut req = TcpStream::connect(server.addr()).unwrap();
    req.write_all(b"\
        GET / HTTP/1.1\r\n\
        Host: example.domain\r\n\
        Content-Length: 19\r\n\
        \r\n\
        I'm a good request.\r\n\
    ").unwrap();
    req.read(&mut [0; 256]).unwrap();

    // note: doesnt include trailing \r\n, cause Content-Length wasn't 21
    assert_eq!(server.body(), b"I'm a good request.");
}

#[test]
fn server_post_with_chunked_body() {
    let server = serve();
    let mut req = TcpStream::connect(server.addr()).unwrap();
    req.write_all(b"\
        POST / HTTP/1.1\r\n\
        Host: example.domain\r\n\
        Transfer-Encoding: chunked\r\n\
        \r\n\
        1\r\n\
        q\r\n\
        2\r\n\
        we\r\n\
        2\r\n\
        rt\r\n\
        0\r\n\
        \r\n
    ").unwrap();
    req.read(&mut [0; 256]).unwrap();

    assert_eq!(server.body(), b"qwert");
}

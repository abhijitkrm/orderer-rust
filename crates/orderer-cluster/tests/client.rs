//! The client protocol over real sockets: connection reuse, frames that
//! arrive in slow pieces, malformed input, and leader-following.

mod common;

use common::*;
use orderer::journal::crc32c;
use orderer_cluster::client::{Client, ClientError, ClientServer, Conn};
use orderer_cluster::codec::encode_batch;
use orderer_core::*;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

fn batch(id: u64, n: u64) -> Vec<u8> {
    let cmds: Vec<_> = (0..n)
        .map(|k| {
            (
                1u32,
                Command::new(id * 1000 + k, Side::Bid, 10_000, 1, Tif::Gtc),
            )
        })
        .collect();
    let mut p = Vec::new();
    encode_batch(&cmds, &mut p);
    p
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut b = vec![kind];
    b.extend_from_slice(body);
    let mut f = (b.len() as u32).to_le_bytes().to_vec();
    f.extend_from_slice(&crc32c(&b).to_le_bytes());
    f.extend_from_slice(&b);
    f
}

fn read_reply(s: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut head = [0u8; 8];
    s.read_exact(&mut head).unwrap();
    let len = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).unwrap();
    (body[0], body[1..].to_vec())
}

impl Served {
    fn pairs(&self) -> Vec<(u8, SocketAddr)> {
        self.addrs
            .iter()
            .copied()
            .enumerate()
            .map(|(i, a)| (i as u8, a))
            .collect()
    }
}

struct Served {
    c: Cluster,
    addrs: Vec<SocketAddr>,
    _servers: Vec<ClientServer>,
}

fn served(name: &str, n: usize) -> Served {
    let c = Cluster::new(name, n, |_| {});
    c.wait_leader(Duration::from_secs(5));
    let addrs = free_addrs(n);
    let servers = (0..n)
        .map(|i| ClientServer::start(addrs[i], c.handle(i).unwrap()).unwrap())
        .collect();
    Served {
        c,
        addrs,
        _servers: servers,
    }
}

#[test]
fn one_connection_serves_many_requests() {
    let s = served("client-reuse", 3);
    let mut client = Client::new(s.pairs());
    for i in 0..60 {
        client
            .propose(&batch(i, 5))
            .unwrap_or_else(|e| panic!("request {i}: {e}"));
    }
    s.c.wait_applied(&[0, 1, 2], 60, Duration::from_secs(10));
}

#[test]
fn frames_split_across_the_server_read_timeout_are_reassembled() {
    let s = served("client-split", 1);
    let mut sock = TcpStream::connect(s.addrs[0]).unwrap();
    sock.set_nodelay(true).unwrap();
    for i in 0..3u64 {
        let f = frame(1, &batch(i, 4));
        // Dribble the frame: header, then body in two pieces, with pauses
        // longer than the server's 200 ms read timeout.
        sock.write_all(&f[..5]).unwrap();
        std::thread::sleep(Duration::from_millis(350));
        sock.write_all(&f[5..20]).unwrap();
        std::thread::sleep(Duration::from_millis(350));
        sock.write_all(&f[20..]).unwrap();
        let (kind, body) = read_reply(&mut sock);
        assert_eq!(kind, 1, "expected OK, got reply {kind}: {body:?}");
    }
}

#[test]
fn status_and_hash_requests_work_on_the_same_connection() {
    let s = served("client-status", 3);
    let mut client = Client::new(s.pairs());
    client.propose(&batch(1, 3)).unwrap();
    let mut conn = Conn::connect(s.addrs[0]).unwrap();
    for _ in 0..3 {
        let st = conn.status().unwrap();
        assert!(st.contains("role=") && st.contains("commit="), "{st}");
    }
    // The recorder state machine supports digests.
    assert!(conn.hash().unwrap().is_some());
}

#[test]
fn malformed_frames_close_the_connection_but_not_the_server() {
    let s = served("client-bad", 1);
    for junk in [
        vec![0xffu8; 40],             // absurd length
        vec![0, 0, 0, 0, 0, 0, 0, 0], // zero length
        {
            let mut f = frame(1, b"hello");
            let n = f.len();
            f[n - 1] ^= 1; // bad checksum
            f
        },
        vec![], // immediate close
    ] {
        let mut sock = TcpStream::connect(s.addrs[0]).unwrap();
        let _ = sock.write_all(&junk);
        drop(sock);
    }
    let mut client = Client::new(s.pairs());
    client.propose(&batch(9, 2)).unwrap();
}

#[test]
fn an_unknown_request_gets_an_error_reply() {
    let s = served("client-unknown", 1);
    let mut sock = TcpStream::connect(s.addrs[0]).unwrap();
    sock.write_all(&frame(99, b"?")).unwrap();
    let (kind, _) = read_reply(&mut sock);
    assert_eq!(kind, 5);
}

#[test]
fn the_client_follows_the_leader_and_reports_no_leader() {
    let s = served("client-follow", 3);
    // Point the client at the followers first; it must find the leader.
    let l = s.c.leader().unwrap();
    let mut order: Vec<(u8, SocketAddr)> = (0..3)
        .filter(|&i| i != l)
        .map(|i| (i as u8, s.addrs[i]))
        .collect();
    order.push((l as u8, s.addrs[l]));
    let mut client = Client::new(order);
    for i in 0..10 {
        client.propose(&batch(i, 2)).unwrap();
    }
    // Nothing listening anywhere: a bounded failure, nothing could commit.
    let nowhere: Vec<(u8, SocketAddr)> = free_addrs(2)
        .into_iter()
        .enumerate()
        .map(|(i, a)| (i as u8, a))
        .collect();
    let mut lost = Client::new(nowhere);
    assert_eq!(lost.propose(&batch(1, 1)), Err(ClientError::NoLeader));
}

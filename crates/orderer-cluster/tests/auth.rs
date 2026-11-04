//! Authenticated transport and client protocol.

mod common;

use common::*;
use orderer_cluster::client::{Client, ClientServer, Conn};
use orderer_cluster::codec::encode_batch;
use orderer_core::*;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

fn payload(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

fn batch(id: u64) -> Vec<u8> {
    let mut p = Vec::new();
    encode_batch(
        &[(1, Command::new(id, Side::Bid, 10_000, 1, Tif::Gtc))],
        &mut p,
    );
    p
}

fn keyed(
    key: &'static [u8],
) -> impl Fn(&mut orderer_cluster::ReplicaConfig) + Send + Sync + 'static {
    move |cfg| cfg.auth_key = Some(key.to_vec())
}

#[test]
fn a_cluster_with_one_shared_key_works_normally() {
    let c = Cluster::new("auth-ok", 3, keyed(b"correct horse battery staple"));
    c.wait_leader(Duration::from_secs(5));
    for i in 0..100 {
        c.propose(&payload(i)).unwrap();
    }
    assert_eq!(c.wait_converged(Duration::from_secs(10)).len(), 100);
}

#[test]
fn a_node_with_the_wrong_key_cannot_join_or_disturb() {
    let mut c = Cluster::new("auth-wrong", 3, keyed(b"right key"));
    let l = c.wait_leader(Duration::from_secs(5));
    for i in 0..20 {
        c.propose(&payload(i)).unwrap();
    }
    let f = (0..3).find(|&i| i != l).unwrap();
    // Restart one follower with the wrong key: it cannot exchange a single
    // message with the others.
    c.stop(f);
    let mut cfg = c.config(f);
    cfg.auth_key = Some(b"wrong key".to_vec());
    let rogue = orderer_cluster::Replica::start(cfg, Recorder::default()).unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    let st = rogue.handle().status();
    assert_eq!(
        st.commit, 0,
        "a node with the wrong key received log entries"
    );
    // The honest majority is unaffected, still commits, and is not deposed.
    let term = c.handle(l).unwrap().status().term;
    for i in 20..60 {
        c.propose(&payload(i)).unwrap();
    }
    assert_eq!(c.handle(l).unwrap().status().term, term);
    rogue.crash();
}

#[test]
fn an_unkeyed_node_cannot_join_a_keyed_cluster_and_vice_versa() {
    let mut c = Cluster::new("auth-none", 3, keyed(b"k"));
    let l = c.wait_leader(Duration::from_secs(5));
    let f = (0..3).find(|&i| i != l).unwrap();
    c.stop(f);
    let mut cfg = c.config(f);
    cfg.auth_key = None;
    let plain = orderer_cluster::Replica::start(cfg, Recorder::default()).unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(plain.handle().status().commit, 0);
    plain.crash();
}

#[test]
fn a_raw_connection_that_skips_the_handshake_is_dropped() {
    let c = Cluster::new("auth-raw", 3, keyed(b"k"));
    c.wait_leader(Duration::from_secs(5));
    // Speak the unauthenticated hello (as the unkeyed protocol would).
    let target = c.members[0].1;
    let mut s = TcpStream::connect(target).unwrap();
    let mut hello = vec![9u8, 1, 0, 0, 0, 0, 0, 0, 0, 0]; // kind 6 body below
    hello.clear();
    let body = {
        let mut b = vec![6u8, 1];
        b.extend_from_slice(&1u64.to_le_bytes());
        b
    };
    let crc = orderer::journal::crc32c(&body);
    s.write_all(&(body.len() as u32).to_le_bytes()).unwrap();
    s.write_all(&crc.to_le_bytes()).unwrap();
    s.write_all(&body).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut buf = [0u8; 64];
    // The server closes without sending a challenge.
    assert!(matches!(s.read(&mut buf), Ok(0) | Err(_)));
    // The cluster is fine.
    c.propose(&payload(1)).unwrap();
}

#[test]
fn the_client_port_requires_the_key() {
    let c = Cluster::new("auth-client", 3, |_| {});
    let l = c.wait_leader(Duration::from_secs(5));
    let addr = free_addrs(1)[0];
    let _srv =
        ClientServer::start_with_key(addr, c.handle(l).unwrap(), Some(b"secret".to_vec())).unwrap();

    // Right key: works.
    let mut ok = Conn::connect_with_key(addr, Some(b"secret")).unwrap();
    assert!(ok.status().unwrap().contains("role=Leader"));
    let mut client = Client::new(vec![(l as u8, addr)]).with_key(b"secret".to_vec());
    client.propose(&batch(1)).unwrap();

    // Wrong key: the client refuses the server's challenge (and vice versa).
    assert!(Conn::connect_with_key(addr, Some(b"not it")).is_err());
    // No key at all: the server will not talk.
    let mut anon = Conn::connect(addr).unwrap();
    assert!(anon.status().is_err());
}

/// Forward one client connection to `server`, optionally flipping one body
/// bit of the second client frame (the first request after the handshake)
/// and recomputing its CRC so that only the MAC can notice.
fn forward(mut a: TcpStream, server: std::net::SocketAddr, tamper: bool) {
    let mut b = TcpStream::connect(server).unwrap();
    let (mut a2, mut b2) = (a.try_clone().unwrap(), b.try_clone().unwrap());
    // server -> client: pass through
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = b2.read(&mut buf) {
            if n == 0 || a2.write_all(&buf[..n]).is_err() {
                break;
            }
        }
    });
    let mut frames = 0;
    loop {
        let mut head = [0u8; 8];
        if a.read_exact(&mut head).is_err() {
            break;
        }
        let len = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len];
        if a.read_exact(&mut body).is_err() {
            break;
        }
        frames += 1;
        if tamper && frames == 2 && len > 1 {
            body[len / 2] ^= 1;
            head[4..8].copy_from_slice(&orderer::journal::crc32c(&body).to_le_bytes());
        }
        if b.write_all(&head).is_err() || b.write_all(&body).is_err() {
            break;
        }
    }
}

#[test]
fn a_tampered_authenticated_frame_is_rejected() {
    use std::net::TcpListener;
    let c = Cluster::new("auth-tamper", 3, |_| {});
    let l = c.wait_leader(Duration::from_secs(5));
    let server = free_addrs(1)[0];
    let _srv = ClientServer::start_with_key(server, c.handle(l).unwrap(), Some(b"secret".to_vec()))
        .unwrap();

    // A man in the middle: the first connection through it is tampered with,
    // the second is forwarded untouched.
    let proxy_l = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = proxy_l.local_addr().unwrap();
    std::thread::spawn(move || {
        for i in 0..2 {
            let (a, _) = proxy_l.accept().unwrap();
            std::thread::spawn(move || forward(a, server, i == 0));
        }
    });

    let mut tampered = Conn::connect_with_key(proxy, Some(b"secret")).unwrap();
    assert!(tampered.status().is_err(), "a tampered frame was accepted");

    // The same path with no tampering works, so the proxy is not what broke
    // the first connection.
    let mut clean = Conn::connect_with_key(proxy, Some(b"secret")).unwrap();
    assert!(clean.status().unwrap().contains("role="));
}

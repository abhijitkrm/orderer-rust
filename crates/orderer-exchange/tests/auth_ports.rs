//! Authentication on the gateway port and the exchange port: the right key and
//! config work; a wrong key, no key, a different config, a replayed recording, a
//! session of the other port and a broken handshake do not, and none of them hurts
//! the server.

use orderer::journal::crc32c;
use orderer::{PartitionMap, Pipeline};
use orderer_cluster::auth::{ct_eq, handshake, nonce, Session, MAC_LEN, NONCE_LEN};
use orderer_cluster::{Replica, ReplicaConfig, ReplicaHandle};
use orderer_consensus::Config as RaftConfig;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::*;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

fn temp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("orderer-auth-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn free() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn replica(dir: &std::path::Path, name: &str, sm: impl orderer_cluster::StateMachine) -> Replica {
    let mut c = ReplicaConfig::new(0, vec![(0, free())], dir.join(name));
    c.tick = Duration::from_millis(5);
    c.consensus = RaftConfig {
        election_ticks: 10,
        heartbeat_ticks: 2,
        max_batch: 256,
        ..RaftConfig::default()
    };
    c.fsync = false;
    c.propose_timeout = Duration::from_secs(5);
    Replica::start(c, sm).unwrap()
}

const KEY: &[u8] = b"a shared secret";

fn cfg_a() -> [u8; 32] {
    ExchangeConfig::demo().checksum()
}

fn cfg_b() -> [u8; 32] {
    ExchangeConfig::parse(
        "ledger_shards = 1\nmatching_groups = 1\npartitions = 4\nfee_ppm = 501\nsymbol = 10 base=1 quote=2 lot=1000\n",
    )
    .unwrap()
    .checksum()
}

fn auth(key: Option<&[u8]>, cfg: [u8; 32]) -> Auth {
    Auth::new(key.map(|k| k.to_vec()), cfg)
}

struct Gw {
    _r: Replica,
    srv: GatewayServer,
    addr: SocketAddr,
}

fn gateway(dir: &std::path::Path, a: Auth) -> Gw {
    let (sm, view) = LedgerNodeSm::new(demo_config(0));
    let r = replica(dir, "ledger", sm);
    let srv =
        GatewayServer::start_with("127.0.0.1:0".parse().unwrap(), r.handle(), view, a.clone())
            .unwrap();
    let addr = srv.addr;
    // wait until the single replica leads
    let mut c = GatewayClient::with_auth(vec![addr], a);
    c.budget = Duration::from_secs(10);
    c.state(true, &[]).expect("the replica becomes leader");
    Gw { _r: r, srv, addr }
}

fn client(addr: SocketAddr, a: Auth) -> GatewayClient {
    let mut c = GatewayClient::with_auth(vec![addr], a);
    c.budget = Duration::from_millis(600);
    c
}

fn works(c: &mut GatewayClient) -> bool {
    c.state(true, &[]).is_ok()
}

#[test]
fn the_gateway_port_accepts_the_right_key_and_refuses_the_rest() {
    let dir = temp("gw");
    let gw = gateway(&dir, auth(Some(KEY), cfg_a()));
    assert!(works(&mut client(gw.addr, auth(Some(KEY), cfg_a()))));
    assert!(
        !works(&mut client(gw.addr, auth(Some(b"another key"), cfg_a()))),
        "wrong key"
    );
    assert!(!works(&mut client(gw.addr, auth(None, cfg_a()))), "no key");
    assert!(
        !works(&mut client(gw.addr, auth(Some(KEY), cfg_b()))),
        "other config"
    );
    // the server is unharmed
    assert!(works(&mut client(gw.addr, auth(Some(KEY), cfg_a()))));
    gw.srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_keyed_client_does_not_talk_to_an_unkeyed_server() {
    let dir = temp("gw2");
    let gw = gateway(&dir, auth(None, cfg_a()));
    assert!(works(&mut client(gw.addr, auth(None, cfg_a()))));
    assert!(!works(&mut client(gw.addr, auth(Some(KEY), cfg_a()))));
    gw.srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- raw sockets: an attacker, or a broken client ------------------------------------

fn put(s: &mut TcpStream, kind: u8, body: &[u8], sess: Option<&mut Session>) -> Vec<u8> {
    let mac = sess.map(|x| x.seal(kind, body));
    let mut b = vec![kind];
    b.extend_from_slice(body);
    if let Some(m) = mac {
        b.extend_from_slice(&m);
    }
    let mut out = (b.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&crc32c(&b).to_le_bytes());
    out.extend_from_slice(&b);
    s.write_all(&out).unwrap();
    out
}

fn get(s: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut h = [0u8; 8];
    s.read_exact(&mut h)?;
    let len = u32::from_le_bytes(h[..4].try_into().unwrap()) as usize;
    let mut b = vec![0u8; len];
    s.read_exact(&mut b)?;
    let kind = b.remove(0);
    Ok((kind, b))
}

/// The client half of the handshake by hand. `Err(text)` if the server refused or
/// failed the challenge.
fn raw_connect(
    addr: SocketAddr,
    key: &[u8],
    cfg: [u8; 32],
    label: &[u8],
) -> Result<(TcpStream, Session), String> {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let nc = nonce();
    let mut hello = nc.to_vec();
    hello.extend_from_slice(&cfg);
    put(&mut s, 0xF0, &hello, None);
    let (kind, body) = get(&mut s).map_err(|e| e.to_string())?;
    if kind == 0xF2 {
        return Err(String::from_utf8_lossy(&body).into_owned());
    }
    assert_eq!(kind, 0xF1);
    let (ns, mac) = body.split_at(NONCE_LEN);
    let chal = [label, b"chal"].concat();
    let sess = [label, b"sess"].concat();
    let want = handshake(key, &chal, &[&nc, ns, &cfg]);
    if !ct_eq(&want[..MAC_LEN], mac) {
        return Err("challenge does not verify".into());
    }
    Ok((s, Session::new(handshake(key, &sess, &[&nc, ns, &cfg]), 0)))
}

const Q_STATE: u8 = 3;

fn state_req() -> Vec<u8> {
    let mut b = vec![1u8]; // lin
    b.extend_from_slice(&0u32.to_le_bytes());
    b
}

#[test]
fn a_config_mismatch_names_itself() {
    let dir = temp("cfg");
    let gw = gateway(&dir, auth(Some(KEY), cfg_a()));
    let e = raw_connect(gw.addr, KEY, cfg_b(), b"gway")
        .map(|_| ())
        .unwrap_err();
    assert!(e.contains("config mismatch"), "{e}");
    gw.srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_recorded_session_cannot_be_replayed() {
    let dir = temp("replay");
    let gw = gateway(&dir, auth(Some(KEY), cfg_a()));
    // a legitimate exchange, recorded
    let (mut s, mut sess) = raw_connect(gw.addr, KEY, cfg_a(), b"gway").unwrap();
    let recorded = put(&mut s, Q_STATE, &state_req(), Some(&mut sess));
    let (k, _) = get(&mut s).unwrap();
    assert_eq!(k, 2, "a STATE reply");
    // replayed on the same connection (stale sequence number): the server hangs up
    s.write_all(&recorded).unwrap();
    assert!(get(&mut s).is_err(), "no answer to a replayed frame");
    // replayed on a new connection (different session key): hung up as well
    let (mut s2, _sess2) = raw_connect(gw.addr, KEY, cfg_a(), b"gway").unwrap();
    s2.write_all(&recorded).unwrap();
    assert!(
        get(&mut s2).is_err(),
        "no answer to a frame from another session"
    );
    // a frame with a flipped bit is refused
    let (mut s3, mut sess3) = raw_connect(gw.addr, KEY, cfg_a(), b"gway").unwrap();
    let mut frame = put(&mut s3, Q_STATE, &state_req(), Some(&mut sess3));
    let _ = get(&mut s3); // the honest copy was answered
    let last = frame.len() - 1;
    frame[last] ^= 1;
    // fix the crc so only the MAC is wrong
    let crc = crc32c(&frame[8..]);
    frame[4..8].copy_from_slice(&crc.to_le_bytes());
    let (mut s4, _) = raw_connect(gw.addr, KEY, cfg_a(), b"gway").unwrap();
    s4.write_all(&frame).unwrap();
    assert!(get(&mut s4).is_err());
    // the server still serves honest clients
    assert!(works(&mut client(gw.addr, auth(Some(KEY), cfg_a()))));
    gw.srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_session_of_the_other_port_does_not_verify() {
    let dir = temp("label");
    let gw = gateway(&dir, auth(Some(KEY), cfg_a()));
    let e = raw_connect(gw.addr, KEY, cfg_a(), b"xchg")
        .map(|_| ())
        .unwrap_err();
    assert!(e.contains("does not verify"), "{e}");
    gw.srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_broken_handshake_does_not_hurt_the_server() {
    let dir = temp("broken");
    let gw = gateway(&dir, auth(Some(KEY), cfg_a()));
    // half a header, then silence, then close
    let mut s = TcpStream::connect(gw.addr).unwrap();
    s.write_all(&[9, 0, 0]).unwrap();
    drop(s);
    // a hello of the wrong length
    let mut s = TcpStream::connect(gw.addr).unwrap();
    put(&mut s, 0xF0, &[1, 2, 3], None);
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert!(get(&mut s).is_err());
    // a request frame instead of a hello
    let mut s = TcpStream::connect(gw.addr).unwrap();
    put(&mut s, Q_STATE, &state_req(), None);
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert!(get(&mut s).is_err());
    // a hello, then the connection drops before the first request
    let (s, _) = raw_connect(gw.addr, KEY, cfg_a(), b"gway").unwrap();
    drop(s);
    // random bytes
    let mut s = TcpStream::connect(gw.addr).unwrap();
    s.write_all(&[0xff; 64]).unwrap();
    drop(s);
    assert!(works(&mut client(gw.addr, auth(Some(KEY), cfg_a()))));
    gw.srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- the exchange port ---------------------------------------------------------------

fn tier(dir: &std::path::Path, a: Auth) -> (Replica, ExchangeServer, ReplicaHandle) {
    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(2).unwrap(),
        TierConfig {
            source_base: 0,
            retain: 1 << 12,
        },
        |init, plugs| {
            let mut b = Pipeline::<FifoCore>::builder()
                .partitions(2)
                .egress(plugs.digest)
                .egress(plugs.archive);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().unwrap()
        },
    );
    let r = replica(dir, "tier", sm);
    let h = r.handle();
    let srv =
        ExchangeServer::start_with("127.0.0.1:0".parse().unwrap(), r.handle(), view, a.clone())
            .unwrap();
    (r, srv, h)
}

#[test]
fn the_exchange_port_checks_key_and_config_too() {
    let dir = temp("xchg");
    let (_r, srv, _h) = tier(&dir, auth(Some(KEY), cfg_a()));
    let ok = ExchangeClient::connect_with(srv.addr, &auth(Some(KEY), cfg_a()));
    let mut ok = ok.expect("right key and config");
    // wait for the single replica to lead, then a real request over the secured link
    let end = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match ok.cursor(0).unwrap() {
            Ok(c) => {
                assert_eq!(c, 0);
                break;
            }
            Err(_) if std::time::Instant::now() < end => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    let wrong = ExchangeClient::connect_with(srv.addr, &auth(Some(b"nope"), cfg_a()));
    assert!(wrong.is_err());
    let mismatch = ExchangeClient::connect_with(srv.addr, &auth(Some(KEY), cfg_b()))
        .err()
        .expect("config mismatch")
        .to_string();
    assert!(mismatch.contains("config mismatch"), "{mismatch}");
    // no key: the handshake passes (nothing to check) but the first request is refused
    let mut nokey = ExchangeClient::connect_with(srv.addr, &auth(None, cfg_a())).unwrap();
    assert!(nokey.cursor(0).is_err());
    // the replica key binds the config
    let k = |c: [u8; 32]| auth(Some(KEY), c).replica_key();
    assert_eq!(k(cfg_a()), k(cfg_a()));
    assert_ne!(k(cfg_a()), k(cfg_b()));
    assert_ne!(k(cfg_a()), Some(KEY.to_vec()));
    srv.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

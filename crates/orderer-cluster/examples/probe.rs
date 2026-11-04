use orderer_cluster::client::Client;
use orderer_cluster::codec::encode_batch;
use orderer_core::*;
use std::time::Instant;
fn main() {
    let addrs: Vec<std::net::SocketAddr> = std::env::args()
        .nth(1)
        .unwrap()
        .split(',')
        .map(|a| a.parse().unwrap())
        .collect();
    let mut c = Client::new(
        addrs
            .into_iter()
            .enumerate()
            .map(|(i, a)| (i as u8, a))
            .collect(),
    );
    for k in 0..6u64 {
        let cmds: Vec<_> = (0..10)
            .map(|i| {
                (
                    1u32,
                    Command::new(1000 + k * 100 + i, Side::Bid, 10_000, 1, Tif::Gtc),
                )
            })
            .collect();
        let mut p = Vec::new();
        encode_batch(&cmds, &mut p);
        let t = Instant::now();
        let r = c.propose(&p);
        println!("proposal {k}: {r:?} in {:?}", t.elapsed());
    }
}

//! The README quick start, compiled and run by `cargo run --example quickstart`.

use orderer::*;
use orderer_core::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("orderer-quickstart");
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(BookConfig::default())
        .partitions(2)
        .journal(JournalConfig::new(&dir, JournalFormat::Binary)) // durable, fsync every 1024
        .egress(collect) // or Acks, Metrics, Callback, your own Egress
        .build()?;

    let h = p.handle(); // cloneable; publish from any thread
    h.publish(7, Command::new(1, Side::Ask, 100, 10, Tif::Gtc))?;
    h.publish(7, Command::new(2, Side::Bid, 100, 4, Tif::Gtc))?;
    h.publish(9, Command::new(1, Side::Bid, 50, 1, Tif::Gtc))?;

    p.drain()?; // everything published so far is applied and delivered
    let snap = p.snapshot()?; // consistent cut across partitions
    snap.write(dir.join("books.snap"))?; // matcher-snap/1 body + .meta sidecar
    p.shutdown()?;

    print!("{}", String::from_utf8(events.listing())?);
    Ok(())
}

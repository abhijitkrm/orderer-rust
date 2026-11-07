//! Shared test model: reference data, a PRNG and a price-time matching group.
#![allow(dead_code)]

use orderer_ledger::*;
use std::collections::BTreeMap;

pub const BTC: u32 = 1;
pub const USD: u32 = 2;
pub const ETH: u32 = 3;
pub const SYMS: [u32; 2] = [10, 11];
pub const ACCOUNTS: u64 = 6;

pub struct Rng(pub u64);
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub fn config(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        SYMS[0],
        Symbol {
            base: BTC,
            quote: USD,
            lot: 1_000,
        },
    );
    symbols.insert(
        SYMS[1],
        Symbol {
            base: ETH,
            quote: USD,
            lot: 10,
        },
    );
    let mut fees = BTreeMap::new();
    fees.insert(SYMS[1], (5_000, 25_000)); // maker pays less than taker on one symbol
    Config {
        shard,
        fee_ppm: 15_000,
        symbols,
        fees,
    }
}

#[derive(Clone)]
pub struct Rest {
    pub id: u64,
    pub side: Side,
    pub price: u64,
    pub remaining: u64,
    pub seq: u64,
}

/// One matching group: price-time books; every 7th order id is immediate-or-cancel.
#[derive(Default)]
pub struct Matcher {
    pub books: BTreeMap<u32, Vec<Rest>>,
    seq: u64,
    pub events: Vec<Event>,
    pub intake: Intake,
}

impl Matcher {
    pub fn submit(&mut self, order: u64, symbol: u32, side: Side, price: u64, qty: u64) {
        // every 7th order id is immediate-or-cancel, whatever the ledger asked for
        self.submit_tif(order, symbol, side, price, qty, order % 7 == 0)
    }

    pub fn submit_tif(
        &mut self,
        order: u64,
        symbol: u32,
        side: Side,
        price: u64,
        qty: u64,
        ioc: bool,
    ) {
        let book = self.books.entry(symbol).or_default();
        let mut rem = qty;
        while rem > 0 {
            let best = book
                .iter()
                .enumerate()
                .filter(|(_, r)| {
                    r.side != side
                        && match side {
                            Side::Buy => r.price <= price,
                            Side::Sell => r.price >= price,
                        }
                })
                .min_by_key(|(_, r)| {
                    let p = if side == Side::Buy {
                        r.price as i64
                    } else {
                        -(r.price as i64)
                    };
                    (p, r.seq)
                })
                .map(|(i, _)| i);
            let Some(i) = best else { break };
            let q = rem.min(book[i].remaining);
            let (buy, sell) = if side == Side::Buy {
                (order, book[i].id)
            } else {
                (book[i].id, order)
            };
            self.events.push(Event::Trade {
                buy,
                sell,
                price: book[i].price,
                qty: q,
                taker_is_buy: side == Side::Buy,
            });
            book[i].remaining -= q;
            rem -= q;
            if book[i].remaining == 0 {
                book.remove(i);
            }
        }
        if rem > 0 {
            if ioc {
                self.events.push(Event::Closed { order });
            } else {
                self.seq += 1;
                book.push(Rest {
                    id: order,
                    side,
                    price,
                    remaining: rem,
                    seq: self.seq,
                });
            }
        }
    }

    pub fn cancel(&mut self, order: u64, symbol: u32) {
        let book = self.books.entry(symbol).or_default();
        if let Some(i) = book.iter().position(|r| r.id == order) {
            book.remove(i);
            self.events.push(Event::Closed { order });
        }
    }

    /// Accept an outbox entry exactly once per (shard, index), however often it is sent.
    pub fn deliver(&mut self, shard: u16, idx: u64, m: OutMsg) -> Accept {
        let a = self.intake.accept(shard, idx);
        if a != Accept::Applied {
            return a;
        }
        match m {
            OutMsg::Submit {
                order,
                symbol,
                side,
                price,
                qty,
            } => self.submit(order, symbol, side, price, qty),
            OutMsg::SubmitIoc {
                order,
                symbol,
                side,
                price,
                qty,
            } => self.submit_tif(order, symbol, side, price, qty, true),
            OutMsg::Cancel { order, symbol } => self.cancel(order, symbol),
            // phase 2 messages concern the credit slice, not the books
            OutMsg::Grant { .. } | OutMsg::Recall { .. } => {}
        }
        a
    }

    pub fn resting(&self) -> usize {
        self.books.values().map(|b| b.len()).sum()
    }
}

/// `c` (a `Reserve`) as an immediate-or-cancel reserve when `ioc`.
pub fn maybe_ioc(c: Cmd, ioc: bool) -> Cmd {
    match c {
        Cmd::Reserve {
            account,
            symbol,
            side,
            price,
            qty,
        } if ioc => Cmd::ReserveIoc {
            account,
            symbol,
            side,
            price,
            qty,
        },
        c => c,
    }
}

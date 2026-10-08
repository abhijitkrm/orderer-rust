//! Thin multi-symbol router. One book per symbol — the exchange partitioning
//! model (each symbol its own single-writer domain).

use std::collections::HashMap;

use crate::book::OrderBook;
use crate::sink::Sink;
use crate::types::*;

pub struct Engine {
    default_cfg: BookConfig,
    books: HashMap<Symbol, OrderBook>,
}

impl Engine {
    pub fn new(default_cfg: BookConfig) -> Engine {
        Engine {
            default_cfg,
            books: HashMap::new(),
        }
    }

    /// The config given to lazily-created books.
    pub fn default_cfg(&self) -> BookConfig {
        self.default_cfg
    }

    /// Register a symbol with its own config (else first `submit` creates it
    /// with the engine default).
    pub fn add_symbol(&mut self, sym: Symbol, cfg: BookConfig) {
        self.books.insert(sym, OrderBook::new(cfg));
    }

    pub fn book(&self, sym: Symbol) -> Option<&OrderBook> {
        self.books.get(&sym)
    }

    /// Iterate all live `(symbol, book)` pairs (unordered).
    pub fn books_iter(&self) -> impl Iterator<Item = (Symbol, &OrderBook)> + '_ {
        self.books.iter().map(|(s, b)| (*s, b))
    }

    pub fn book_mut(&mut self, sym: Symbol) -> Option<&mut OrderBook> {
        self.books.get_mut(&sym)
    }

    /// Route a command to `sym`'s book; events flow to `sink`.
    pub fn submit<S: Sink>(&mut self, sym: Symbol, cmd: Command, sink: &mut S) {
        let cfg = self.default_cfg;
        self.books
            .entry(sym)
            .or_insert_with(|| OrderBook::new(cfg))
            .apply(cmd, sink);
    }

    /// `submit` with symbol-tagged delivery: `f(symbol, seq, event)`.
    pub fn submit_tagged<F>(&mut self, sym: Symbol, cmd: Command, f: &mut F)
    where
        F: FnMut(Symbol, u64, &Event),
    {
        struct Adaptor<'a, F> {
            sym: Symbol,
            f: &'a mut F,
        }
        impl<'a, F: FnMut(Symbol, u64, &Event)> Sink for Adaptor<'a, F> {
            #[inline]
            fn on_event(&mut self, seq: u64, ev: &Event) {
                (self.f)(self.sym, seq, ev)
            }
        }
        let cfg = self.default_cfg;
        self.books
            .entry(sym)
            .or_insert_with(|| OrderBook::new(cfg))
            .apply(cmd, &mut Adaptor { sym, f });
    }
}

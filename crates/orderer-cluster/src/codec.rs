//! Binary encodings: protocol messages, WAL frames and command batches.
//! Little-endian throughout. Decoders never panic on malformed input.

use orderer::journal::{decode_cmd, encode_cmd, CMD_RECORD};
use orderer_consensus::{Entry, Index, Msg, NodeId, Persist, Term};
use orderer_core::{Command, Symbol};

#[derive(Debug, PartialEq, Eq)]
pub enum CodecError {
    Short,
    Bad(&'static str),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::Short => write!(f, "truncated input"),
            CodecError::Bad(w) => write!(f, "malformed: {w}"),
        }
    }
}

impl std::error::Error for CodecError {}

type R<T> = Result<T, CodecError>;

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
    pub fn bytes(&mut self, n: usize) -> R<&'a [u8]> {
        if self.remaining() < n {
            return Err(CodecError::Short);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> R<u8> {
        Ok(self.bytes(1)?[0])
    }
    pub fn u32(&mut self) -> R<u32> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("4 bytes"),
        ))
    }
    pub fn u64(&mut self) -> R<u64> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("8 bytes"),
        ))
    }
    pub fn finish(&self) -> R<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(CodecError::Bad("trailing bytes"))
        }
    }
}

/// Upper bound on a single entry's payload, to stop hostile length fields
/// from allocating (a batch entry is far smaller).
pub const MAX_ENTRY: usize = 16 << 20;

pub fn put_entry(out: &mut Vec<u8>, e: &Entry) {
    out.extend_from_slice(&e.term.to_le_bytes());
    out.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
    out.extend_from_slice(&e.data);
}

pub fn get_entry(r: &mut Reader) -> R<Entry> {
    let term = r.u32()?;
    let n = r.u32()? as usize;
    if n > MAX_ENTRY {
        return Err(CodecError::Bad("entry too large"));
    }
    Ok(Entry {
        term,
        data: r.bytes(n)?.to_vec(),
    })
}

fn get_entries(r: &mut Reader) -> R<Vec<Entry>> {
    let n = r.u32()? as usize;
    // Each entry is at least 8 bytes; reject counts the input cannot hold.
    if n > r.remaining() / 8 {
        return Err(CodecError::Bad("entry count"));
    }
    (0..n).map(|_| get_entry(r)).collect()
}

fn put_entries(out: &mut Vec<u8>, entries: &[Entry]) {
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        put_entry(out, e);
    }
}

pub fn encode_msg(m: &Msg, out: &mut Vec<u8>) {
    match m {
        Msg::Vote {
            term,
            last_index,
            last_term,
            pre,
        } => {
            out.push(1);
            out.extend_from_slice(&term.to_le_bytes());
            out.extend_from_slice(&last_index.to_le_bytes());
            out.extend_from_slice(&last_term.to_le_bytes());
            out.push(u8::from(*pre));
        }
        Msg::VoteReply { term, granted, pre } => {
            out.push(2);
            out.extend_from_slice(&term.to_le_bytes());
            out.push(u8::from(*granted));
            out.push(u8::from(*pre));
        }
        Msg::Append {
            term,
            prev_index,
            prev_term,
            entries,
            commit,
        } => {
            out.push(3);
            out.extend_from_slice(&term.to_le_bytes());
            out.extend_from_slice(&prev_index.to_le_bytes());
            out.extend_from_slice(&prev_term.to_le_bytes());
            out.extend_from_slice(&commit.to_le_bytes());
            put_entries(out, entries);
        }
        Msg::AppendReply {
            term,
            success,
            index,
        } => {
            out.push(4);
            out.extend_from_slice(&term.to_le_bytes());
            out.push(u8::from(*success));
            out.extend_from_slice(&index.to_le_bytes());
        }
    }
}

fn flag(r: &mut Reader) -> R<bool> {
    match r.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::Bad("flag")),
    }
}

pub fn decode_msg(r: &mut Reader) -> R<Msg> {
    Ok(match r.u8()? {
        1 => Msg::Vote {
            term: r.u32()?,
            last_index: r.u64()?,
            last_term: r.u32()?,
            pre: flag(r)?,
        },
        2 => Msg::VoteReply {
            term: r.u32()?,
            granted: flag(r)?,
            pre: flag(r)?,
        },
        3 => Msg::Append {
            term: r.u32()?,
            prev_index: r.u64()?,
            prev_term: r.u32()?,
            commit: r.u64()?,
            entries: get_entries(r)?,
        },
        4 => Msg::AppendReply {
            term: r.u32()?,
            success: flag(r)?,
            index: r.u64()?,
        },
        _ => return Err(CodecError::Bad("message tag")),
    })
}

/// A WAL frame body: one atomic write (the token is not stored).
pub fn encode_persist(p: &Persist, out: &mut Vec<u8>) {
    out.extend_from_slice(&p.term.to_le_bytes());
    out.push(p.voted_for.unwrap_or(NodeId::MAX));
    match p.compact {
        Some((i, t)) => {
            out.push(1);
            out.extend_from_slice(&i.to_le_bytes());
            out.extend_from_slice(&t.to_le_bytes());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&p.from.to_le_bytes());
    put_entries(out, &p.entries);
}

pub fn decode_persist(buf: &[u8]) -> R<Persist> {
    let mut r = Reader::new(buf);
    let term: Term = r.u32()?;
    let voted_for = match r.u8()? {
        NodeId::MAX => None,
        v => Some(v),
    };
    let compact: Option<(Index, Term)> = match r.u8()? {
        0 => None,
        1 => Some((r.u64()?, r.u32()?)),
        _ => return Err(CodecError::Bad("compact flag")),
    };
    let from = r.u64()?;
    if from == 0 {
        return Err(CodecError::Bad("write starts at index 0"));
    }
    let entries = get_entries(&mut r)?;
    r.finish()?;
    Ok(Persist {
        token: 0,
        term,
        voted_for,
        from,
        entries,
        compact,
    })
}

// ---- command batches (the payload of a log entry) ----------------------------

/// Entry payload tag: a batch of commands.
pub const TAG_BATCH: u8 = 1;
/// One command on the wire: symbol, cmd, side, otype, tif, order_id, price,
/// qty — the journal record without its `iseq`, checksum or padding.
pub const CMD_BYTES: usize = 32;

pub fn encode_batch(cmds: &[(Symbol, Command)], out: &mut Vec<u8>) {
    out.push(TAG_BATCH);
    out.extend_from_slice(&(cmds.len() as u32).to_le_bytes());
    out.reserve(cmds.len() * CMD_BYTES);
    let mut rec = [0u8; CMD_RECORD];
    for (sym, cmd) in cmds {
        encode_cmd(0, *sym, cmd, &mut rec);
        out.extend_from_slice(&rec[8..8 + CMD_BYTES]);
    }
}

/// Decode a batch payload. An empty payload (a leader's no-op) is an empty
/// batch.
pub fn decode_batch(data: &[u8]) -> R<Vec<(Symbol, Command)>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut r = Reader::new(data);
    if r.u8()? != TAG_BATCH {
        return Err(CodecError::Bad("entry tag"));
    }
    let n = r.u32()? as usize;
    if n != r.remaining() / CMD_BYTES || r.remaining() % CMD_BYTES != 0 {
        return Err(CodecError::Bad("batch length"));
    }
    let mut out = Vec::with_capacity(n);
    let mut rec = [0u8; CMD_RECORD];
    for _ in 0..n {
        rec[8..8 + CMD_BYTES].copy_from_slice(r.bytes(CMD_BYTES)?);
        let (_, sym, cmd) = decode_cmd(&rec).ok_or(CodecError::Bad("command"))?;
        out.push((sym, cmd));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orderer_core::{Side, Tif};

    fn entries() -> Vec<Entry> {
        vec![
            Entry {
                term: 1,
                data: vec![],
            },
            Entry {
                term: 2,
                data: vec![1, 2, 3],
            },
        ]
    }

    #[test]
    fn messages_round_trip() {
        let msgs = vec![
            Msg::Vote {
                term: 9,
                last_index: 1 << 40,
                last_term: 3,
                pre: true,
            },
            Msg::VoteReply {
                term: 9,
                granted: false,
                pre: false,
            },
            Msg::Append {
                term: 4,
                prev_index: 7,
                prev_term: 3,
                entries: entries(),
                commit: 5,
            },
            Msg::AppendReply {
                term: 4,
                success: true,
                index: 99,
            },
        ];
        for m in msgs {
            let mut b = Vec::new();
            encode_msg(&m, &mut b);
            let mut r = Reader::new(&b);
            assert_eq!(decode_msg(&mut r).unwrap(), m);
            r.finish().unwrap();
        }
    }

    #[test]
    fn persist_round_trips() {
        let p = Persist {
            token: 0,
            term: 5,
            voted_for: Some(2),
            from: 11,
            entries: entries(),
            compact: Some((10, 4)),
        };
        let mut b = Vec::new();
        encode_persist(&p, &mut b);
        assert_eq!(decode_persist(&b).unwrap(), p);
    }

    #[test]
    fn truncations_and_garbage_never_panic() {
        let mut b = Vec::new();
        encode_msg(
            &Msg::Append {
                term: 4,
                prev_index: 7,
                prev_term: 3,
                entries: entries(),
                commit: 5,
            },
            &mut b,
        );
        for n in 0..b.len() {
            assert!(
                decode_msg(&mut Reader::new(&b[..n])).is_err(),
                "prefix {n} decoded"
            );
        }
        // Hostile count / length fields must not allocate.
        let mut evil = vec![3u8];
        evil.extend_from_slice(&[0u8; 24]);
        evil.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_msg(&mut Reader::new(&evil)).is_err());
        let mut x = 0x1234_5678_9abc_def0u64;
        for _ in 0..20000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let len = (x % 64) as usize;
            let junk: Vec<u8> = (0..len).map(|i| (x >> (i % 56)) as u8).collect();
            let _ = decode_msg(&mut Reader::new(&junk));
            let _ = decode_persist(&junk);
            let _ = decode_batch(&junk);
        }
    }

    #[test]
    fn batches_round_trip() {
        let cmds = vec![
            (7, Command::new(1, Side::Ask, 100, 10, Tif::Gtc)),
            (9, Command::new(2, Side::Bid, 101, 4, Tif::Ioc)),
            (7, Command::Cancel { order_id: 1 }),
        ];
        let mut b = Vec::new();
        encode_batch(&cmds, &mut b);
        assert_eq!(b.len(), 5 + 3 * CMD_BYTES);
        assert_eq!(decode_batch(&b).unwrap(), cmds);
        assert!(decode_batch(&[]).unwrap().is_empty());
        assert!(decode_batch(&b[..b.len() - 1]).is_err());
        assert!(decode_batch(&[9, 0, 0, 0, 0]).is_err());
    }
}

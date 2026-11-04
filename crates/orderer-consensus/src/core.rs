//! The replication protocol as a pure state machine (`spec/REPLICATION.md`).
//!
//! A [`Node`] performs no I/O and reads no clock. The caller feeds it inputs
//! ([`Node::tick`], [`Node::step`], [`Node::propose`], [`Node::persisted`])
//! and executes the [`Effect`]s it emits. This is what makes the protocol
//! simulable: the same inputs always produce the same effects.
//!
//! Disk contract: a [`Persist`] write is atomic, writes are applied in the
//! order issued, and the node is told of completion with
//! [`Node::persisted`] in that same order. A node never reveals state
//! (votes, append acknowledgements) before the write covering it completes.

use crate::rng::Rng;
use std::collections::VecDeque;

pub type NodeId = u8;
pub type Term = u32;
/// Log position, 1-based. 0 means "before the first entry".
pub type Index = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    /// `pre`: a pre-vote probe. `term` is then the term the sender would
    /// campaign in; nothing changes on the receiver.
    Vote {
        term: Term,
        last_index: Index,
        last_term: Term,
        pre: bool,
    },
    VoteReply {
        term: Term,
        granted: bool,
        pre: bool,
    },
    Append {
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        commit: Index,
    },
    /// On success `index` is the highest index the sender holds durably that
    /// matches the leader. On failure it is a hint: the sender's last index
    /// (or `prev_index - 1`), whichever is smaller.
    AppendReply {
        term: Term,
        success: bool,
        index: Index,
    },
}

impl Msg {
    pub fn term(&self) -> Term {
        match self {
            Msg::Vote { term, .. }
            | Msg::VoteReply { term, .. }
            | Msg::Append { term, .. }
            | Msg::AppendReply { term, .. } => *term,
        }
    }
}

/// One atomic disk write: set the hard state, truncate the log to
/// `from - 1` entries, then append `entries`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Persist {
    pub token: u64,
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub from: Index,
    pub entries: Vec<Entry>,
    /// First discard the log prefix up to and including this `(index, term)`
    /// (a snapshot now covers it). The host stores the snapshot data itself,
    /// **before** issuing this write.
    pub compact: Option<(Index, Term)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Send {
        to: NodeId,
        msg: Msg,
    },
    Persist(Persist),
    /// A committed entry, delivered exactly once per incarnation, in order.
    Apply {
        index: Index,
        entry: Entry,
    },
    /// A proposal was refused: this node is not the leader.
    Rejected {
        leader: Option<NodeId>,
    },
    /// `to` needs state the log no longer holds. The host should transfer its
    /// stored snapshot at `index` (the log base) to `to`, which installs it
    /// with [`Node::install_snapshot`].
    NeedSnapshot {
        to: NodeId,
        index: Index,
        term: Term,
    },
    /// Replace the state machine with the snapshot at `index`; applying
    /// resumes at `index + 1`.
    Restore {
        index: Index,
        term: Term,
    },
}

/// What survives a crash.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Disk {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    /// Entries `base + 1 ..`; everything up to `base` is in the snapshot.
    pub log: Vec<Entry>,
    pub base: Index,
    pub base_term: Term,
}

impl Disk {
    /// Apply one write. Writes must be applied in issue order.
    pub fn apply(&mut self, p: &Persist) {
        self.term = p.term;
        self.voted_for = p.voted_for;
        if let Some((i, t)) = p.compact {
            let drop = (i.saturating_sub(self.base) as usize).min(self.log.len());
            self.log.drain(..drop);
            if i > self.base {
                self.base = i;
                self.base_term = t;
            }
        }
        let keep = (p.from - 1 - self.base) as usize;
        if keep < self.log.len() {
            self.log.truncate(keep);
        }
        debug_assert!(keep <= self.log.len(), "persist leaves a hole");
        self.log.extend(p.entries.iter().cloned());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mutation {
    None,
    /// Commit as soon as the leader itself has written the entry.
    CommitOnOneAck,
    /// Grant votes without the log up-to-date check.
    SkipUpToDateCheck,
    /// Grant a second vote in the same term.
    IgnoreVotedFor,
    /// Commit earlier-term entries by counting replicas (Raft figure 8).
    CommitOldTerm,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Base election timeout in ticks; the actual one is in `[T, 2T)`.
    pub election_ticks: u32,
    pub heartbeat_ticks: u32,
    /// Max entries per `Append`.
    pub max_batch: usize,
    pub pre_vote: bool,
    /// Honored only when the `mutations` feature is on.
    pub mutation: Mutation,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_batch: 64,
            pre_vote: true,
            mutation: Mutation::None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

struct Write {
    token: u64,
    /// Highest log index this write makes durable.
    upto: Index,
    epoch: u64,
    /// Effects held back until this write is durable.
    gated: Vec<Effect>,
}

pub struct Node {
    id: NodeId,
    peers: Vec<NodeId>,
    cfg: Config,
    rng: Rng,

    term: Term,
    voted_for: Option<NodeId>,
    /// Entries `base + 1 ..`; `base`/`base_term` describe the snapshot.
    log: Vec<Entry>,
    base: Index,
    base_term: Term,

    role: Role,
    leader: Option<NodeId>,
    commit: Index,
    applied: Index,
    /// Highest log index durable on this node.
    durable: Index,
    /// Bumped by every truncation; a completing write from an older epoch
    /// does not advance `durable` (a later write covers it).
    epoch: u64,
    writes: VecDeque<Write>,
    next_token: u64,

    votes: Vec<bool>,
    next_index: Vec<Index>,
    match_index: Vec<Index>,
    heard: Vec<bool>,
    /// Ticks until a snapshot may be requested for the peer again.
    snap_wait: Vec<u32>,

    election_elapsed: u32,
    election_timeout: u32,
    heartbeat_elapsed: u32,

    out: Vec<Effect>,
    fault: Option<&'static str>,
}

impl Node {
    pub fn new(id: NodeId, peers: Vec<NodeId>, cfg: Config, seed: u64) -> Self {
        Self::restart(id, peers, cfg, seed, Disk::default())
    }

    /// Start (or restart after a crash) from what was on disk.
    pub fn restart(id: NodeId, mut peers: Vec<NodeId>, cfg: Config, seed: u64, disk: Disk) -> Self {
        peers.sort_unstable();
        assert!(
            peers.contains(&id),
            "node must be a member of its own cluster"
        );
        let n = peers.len();
        let durable = disk.base + disk.log.len() as Index;
        let mut node = Node {
            id,
            peers,
            cfg,
            rng: Rng::new(seed ^ (u64::from(id) << 56)),
            term: disk.term,
            voted_for: disk.voted_for,
            log: disk.log,
            base: disk.base,
            base_term: disk.base_term,
            role: Role::Follower,
            leader: None,
            // The host restores the snapshot at `base` before replaying.
            commit: disk.base,
            applied: disk.base,
            durable,
            epoch: 0,
            writes: VecDeque::new(),
            next_token: 1,
            votes: vec![false; n],
            next_index: vec![1; n],
            match_index: vec![0; n],
            heard: vec![false; n],
            snap_wait: vec![0; n],
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            out: Vec::new(),
            fault: None,
        };
        node.reset_timeout();
        node
    }

    // ---- observers -------------------------------------------------------

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit_index(&self) -> Index {
        self.commit
    }
    /// Log entries from `base() + 1` on.
    pub fn log(&self) -> &[Entry] {
        &self.log
    }
    pub fn base(&self) -> Index {
        self.base
    }
    pub fn last_index(&self) -> Index {
        self.base + self.log.len() as Index
    }
    pub fn applied_index(&self) -> Index {
        self.applied
    }
    /// Entry at absolute `index`, if still in the log.
    pub fn entry_at(&self, index: Index) -> Option<&Entry> {
        if index > self.base {
            self.log.get((index - self.base - 1) as usize)
        } else {
            None
        }
    }
    /// Set if the node detected a violated protocol invariant. The node
    /// keeps running; a harness should treat this as a failure.
    pub fn fault(&self) -> Option<&'static str> {
        self.fault
    }
    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.out)
    }

    // ---- inputs ----------------------------------------------------------

    pub fn tick(&mut self) {
        self.snap_wait
            .iter_mut()
            .for_each(|w| *w = w.saturating_sub(1));
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
            self.election_elapsed += 1;
            if self.election_elapsed >= self.election_timeout {
                // Check-quorum: a leader that has not heard from a majority
                // for a full timeout steps down (spec §8).
                let me = self.pos(self.id);
                let heard = self
                    .heard
                    .iter()
                    .enumerate()
                    .filter(|&(i, &h)| h || i == me)
                    .count();
                if heard < self.quorum() {
                    self.role = Role::Follower;
                    self.leader = None;
                }
                self.heard.iter_mut().for_each(|h| *h = false);
                self.reset_timeout();
            }
            return;
        }
        self.election_elapsed += 1;
        if self.election_elapsed >= self.election_timeout {
            self.start_election();
        }
    }

    /// Submit client data. Only the leader accepts it.
    pub fn propose(&mut self, data: Vec<u8>) {
        if self.role != Role::Leader {
            self.out.push(Effect::Rejected {
                leader: self.leader,
            });
            return;
        }
        self.leader_append(Entry {
            term: self.term,
            data,
        });
        self.broadcast_append();
    }

    /// The host has stored a snapshot of the state after applying entries up
    /// to `index` (which must be applied). Drops the log prefix it covers.
    pub fn compact(&mut self, index: Index) {
        if index <= self.base || index > self.applied {
            self.fault = Some("compact outside (base, applied]");
            return;
        }
        let term = self.term_at(index);
        let drop = (index - self.base) as usize;
        self.log.drain(..drop);
        self.base = index;
        self.base_term = term;
        let from = self.last_index() + 1;
        self.persist_with(from, Vec::new(), Some((index, term)));
    }

    /// The host received (and durably stored) the leader `from`'s snapshot
    /// at `(index, last_term)`, sent in `term`. Emits [`Effect::Restore`] when
    /// the state machine must be replaced.
    pub fn install_snapshot(&mut self, from: NodeId, term: Term, index: Index, last_term: Term) {
        if term < self.term {
            return;
        }
        self.become_follower(term, Some(from));
        let reply_term = self.term;
        if index <= self.commit {
            // Already covered by what we have committed.
            let have = self.commit;
            self.send_gated(
                from,
                Msg::AppendReply {
                    term: reply_term,
                    success: true,
                    index: have,
                },
            );
            return;
        }
        if index <= self.last_index() && self.term_at(index) == last_term {
            // We hold the entry the snapshot ends at: keep the suffix.
            let drop = (index - self.base) as usize;
            self.log.drain(..drop);
        } else {
            self.log.clear();
            self.durable = index;
            self.epoch += 1;
        }
        self.base = index;
        self.base_term = last_term;
        self.commit = index;
        self.applied = index;
        let from_index = self.last_index() + 1;
        self.persist_with(from_index, Vec::new(), Some((index, last_term)));
        self.out.push(Effect::Restore {
            index,
            term: last_term,
        });
        self.send_gated(
            from,
            Msg::AppendReply {
                term: reply_term,
                success: true,
                index,
            },
        );
    }

    /// The disk finished the write with `token` (and all earlier ones).
    pub fn persisted(&mut self, token: u64) {
        while self.writes.front().is_some_and(|w| w.token <= token) {
            let w = self.writes.pop_front().expect("checked non-empty");
            if w.epoch == self.epoch {
                self.durable = self.durable.max(w.upto).min(self.last_index());
            }
            self.out.extend(w.gated);
        }
        if self.role == Role::Leader {
            self.try_commit();
        }
    }

    pub fn step(&mut self, from: NodeId, msg: Msg) {
        match msg {
            Msg::Vote {
                term,
                last_index,
                last_term,
                pre,
            } => self.on_vote(from, term, last_index, last_term, pre),
            Msg::VoteReply { term, granted, pre } => self.on_vote_reply(from, term, granted, pre),
            Msg::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            } => self.on_append(from, term, prev_index, prev_term, entries, commit),
            Msg::AppendReply {
                term,
                success,
                index,
            } => self.on_append_reply(from, term, success, index),
        }
    }

    // ---- helpers ---------------------------------------------------------

    fn pos(&self, id: NodeId) -> usize {
        self.peers
            .iter()
            .position(|&p| p == id)
            .expect("unknown peer")
    }

    fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    fn mutated(&self, m: Mutation) -> bool {
        cfg!(feature = "mutations") && self.cfg.mutation == m
    }

    fn last_term(&self) -> Term {
        self.log.last().map_or(self.base_term, |e| e.term)
    }

    /// Term of the entry at `index`; 0 for "before the log" and for indexes
    /// that were compacted away (callers never ask about those).
    fn term_at(&self, index: Index) -> Term {
        if index == self.base {
            self.base_term
        } else {
            self.entry_at(index).map_or(0, |e| e.term)
        }
    }

    fn reset_timeout(&mut self) {
        self.election_elapsed = 0;
        let t = u64::from(self.cfg.election_ticks);
        self.election_timeout = (t + self.rng.below(t)) as u32;
    }

    fn others(&self) -> Vec<NodeId> {
        self.peers
            .iter()
            .copied()
            .filter(|&p| p != self.id)
            .collect()
    }

    fn send(&mut self, to: NodeId, msg: Msg) {
        self.out.push(Effect::Send { to, msg });
    }

    /// Send once everything written so far is durable.
    fn send_gated(&mut self, to: NodeId, msg: Msg) {
        let eff = Effect::Send { to, msg };
        match self.writes.back_mut() {
            Some(w) => w.gated.push(eff),
            None => self.out.push(eff),
        }
    }

    fn persist(&mut self, from: Index, entries: Vec<Entry>) {
        self.persist_with(from, entries, None);
    }

    fn persist_with(&mut self, from: Index, entries: Vec<Entry>, compact: Option<(Index, Term)>) {
        let token = self.next_token;
        self.next_token += 1;
        let upto = from - 1 + entries.len() as Index;
        self.writes.push_back(Write {
            token,
            upto,
            epoch: self.epoch,
            gated: Vec::new(),
        });
        self.out.push(Effect::Persist(Persist {
            token,
            term: self.term,
            voted_for: self.voted_for,
            from,
            entries,
            compact,
        }));
    }

    fn persist_hard_state(&mut self) {
        let from = self.last_index() + 1;
        self.persist(from, Vec::new());
    }

    fn truncate_from(&mut self, index: Index) {
        if index <= self.commit {
            self.fault = Some("truncating a committed entry");
        }
        self.log.truncate((index - self.base - 1) as usize);
        self.durable = self.durable.min(index - 1);
        self.epoch += 1;
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.persist_hard_state();
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.reset_timeout();
    }

    fn apply_committed(&mut self) {
        while self.applied < self.commit {
            self.applied += 1;
            let entry = self
                .entry_at(self.applied)
                .expect("committed entry is in the log")
                .clone();
            self.out.push(Effect::Apply {
                index: self.applied,
                entry,
            });
        }
    }

    // ---- elections -------------------------------------------------------

    fn start_election(&mut self) {
        if self.cfg.pre_vote && self.peers.len() > 1 {
            self.role = Role::PreCandidate;
            self.leader = None;
            self.votes.iter_mut().for_each(|v| *v = false);
            let me = self.pos(self.id);
            self.votes[me] = true;
            self.reset_timeout();
            let msg = Msg::Vote {
                term: self.term + 1,
                last_index: self.last_index(),
                last_term: self.last_term(),
                pre: true,
            };
            for p in self.others() {
                self.send(p, msg.clone());
            }
        } else {
            self.campaign();
        }
    }

    fn campaign(&mut self) {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.iter_mut().for_each(|v| *v = false);
        let me = self.pos(self.id);
        self.votes[me] = true;
        self.persist_hard_state();
        self.reset_timeout();
        let msg = Msg::Vote {
            term: self.term,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre: false,
        };
        for p in self.others() {
            self.send_gated(p, msg.clone());
        }
        if self.votes.iter().filter(|&&v| v).count() >= self.quorum() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        let next = self.last_index() + 1;
        self.next_index.iter_mut().for_each(|n| *n = next);
        self.match_index.iter_mut().for_each(|m| *m = 0);
        self.heard.iter_mut().for_each(|h| *h = false);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        // The no-op both commits earlier-term entries and establishes the
        // new term's authority (spec §5).
        self.leader_append(Entry {
            term: self.term,
            data: Vec::new(),
        });
        self.broadcast_append();
    }

    fn up_to_date(&self, last_index: Index, last_term: Term) -> bool {
        self.mutated(Mutation::SkipUpToDateCheck)
            || (last_term, last_index) >= (self.last_term(), self.last_index())
    }

    fn on_vote(&mut self, from: NodeId, term: Term, last_index: Index, last_term: Term, pre: bool) {
        let up_to_date = self.up_to_date(last_index, last_term);
        if pre {
            // Grant only if we have no live leader and the log is current.
            // Never changes any state.
            let live_leader = self.role == Role::Leader
                || (self.leader.is_some() && self.election_elapsed < self.election_timeout);
            let granted = term > self.term && up_to_date && !live_leader;
            let reply_term = if granted { term } else { self.term };
            self.send(
                from,
                Msg::VoteReply {
                    term: reply_term,
                    granted,
                    pre: true,
                },
            );
            return;
        }
        if term < self.term {
            self.send(
                from,
                Msg::VoteReply {
                    term: self.term,
                    granted: false,
                    pre: false,
                },
            );
            return;
        }
        if term > self.term {
            self.become_follower(term, None);
        }
        let free = self.mutated(Mutation::IgnoreVotedFor)
            || self.voted_for.is_none()
            || self.voted_for == Some(from);
        let granted = free && up_to_date;
        if granted {
            if self.voted_for != Some(from) {
                self.voted_for = Some(from);
                self.persist_hard_state();
            }
            self.reset_timeout();
        }
        let term = self.term;
        self.send_gated(
            from,
            Msg::VoteReply {
                term,
                granted,
                pre: false,
            },
        );
    }

    fn on_vote_reply(&mut self, from: NodeId, term: Term, granted: bool, pre: bool) {
        if pre {
            if self.role != Role::PreCandidate {
                return;
            }
            if !granted {
                if term > self.term {
                    self.become_follower(term, None);
                }
                return;
            }
            if term != self.term + 1 {
                return;
            }
            let i = self.pos(from);
            self.votes[i] = true;
            if self.votes.iter().filter(|&&v| v).count() >= self.quorum() {
                self.campaign();
            }
            return;
        }
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Candidate || term < self.term || !granted {
            return;
        }
        let i = self.pos(from);
        self.votes[i] = true;
        if self.votes.iter().filter(|&&v| v).count() >= self.quorum() {
            self.become_leader();
        }
    }

    // ---- replication -----------------------------------------------------

    fn leader_append(&mut self, entry: Entry) {
        self.log.push(entry.clone());
        let index = self.last_index();
        self.persist(index, vec![entry]);
    }

    fn broadcast_append(&mut self) {
        for p in self.others() {
            self.send_append(p);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let i = self.pos(to);
        let next = self.next_index[i].max(1);
        if next <= self.base {
            // The entries this peer needs are inside the snapshot.
            if self.snap_wait[i] == 0 {
                self.snap_wait[i] = self.cfg.election_ticks * 2;
                self.out.push(Effect::NeedSnapshot {
                    to,
                    index: self.base,
                    term: self.base_term,
                });
            }
            return;
        }
        let prev_index = next - 1;
        let start = (prev_index - self.base) as usize;
        let end = self.log.len().min(start + self.cfg.max_batch);
        let entries = self.log[start..end].to_vec();
        let msg = Msg::Append {
            term: self.term,
            prev_index,
            prev_term: self.term_at(prev_index),
            entries,
            commit: self.commit,
        };
        self.send(to, msg);
    }

    fn on_append(
        &mut self,
        from: NodeId,
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) {
        if term < self.term {
            self.send(
                from,
                Msg::AppendReply {
                    term: self.term,
                    success: false,
                    index: 0,
                },
            );
            return;
        }
        // A current-term Append: `from` is the leader.
        self.become_follower(term, Some(from));

        // Entries inside our snapshot are committed, hence identical.
        let (mut prev_index, mut prev_term, mut entries) = (prev_index, prev_term, entries);
        if prev_index < self.base {
            let skip = ((self.base - prev_index) as usize).min(entries.len());
            entries.drain(..skip);
            prev_index += skip as Index;
            if prev_index < self.base {
                // Everything sent is already covered; just confirm our base.
                let term = self.term;
                let index = self.base;
                self.send_gated(
                    from,
                    Msg::AppendReply {
                        term,
                        success: true,
                        index,
                    },
                );
                return;
            }
            prev_term = self.base_term;
        }

        if prev_index > self.last_index() || self.term_at(prev_index) != prev_term {
            let hint = self.last_index().min(prev_index.saturating_sub(1));
            let term = self.term;
            self.send_gated(
                from,
                Msg::AppendReply {
                    term,
                    success: false,
                    index: hint,
                },
            );
            return;
        }

        let mut idx = prev_index;
        let mut first_new = None;
        for (k, e) in entries.iter().enumerate() {
            idx += 1;
            if idx <= self.last_index() {
                if self.term_at(idx) != e.term {
                    self.truncate_from(idx);
                    first_new = Some(k);
                    break;
                }
            } else {
                first_new = Some(k);
                break;
            }
        }
        if let Some(k) = first_new {
            let from_index = prev_index + 1 + k as Index;
            let new = entries[k..].to_vec();
            self.log.extend(new.iter().cloned());
            self.persist(from_index, new);
        }

        let matched = prev_index + entries.len() as Index;
        if leader_commit > self.commit {
            self.commit = leader_commit.min(matched);
            self.apply_committed();
        }
        let term = self.term;
        self.send_gated(
            from,
            Msg::AppendReply {
                term,
                success: true,
                index: matched,
            },
        );
    }

    fn on_append_reply(&mut self, from: NodeId, term: Term, success: bool, index: Index) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Leader || term < self.term {
            return;
        }
        let i = self.pos(from);
        self.heard[i] = true;
        if success {
            self.match_index[i] = self.match_index[i].max(index);
            self.next_index[i] = self.next_index[i].max(index + 1);
            self.try_commit();
            if self.next_index[i] <= self.last_index() {
                self.send_append(from);
            }
        } else {
            self.next_index[i] = (self.next_index[i] - 1).min(index + 1).max(1);
            self.send_append(from);
        }
    }

    fn try_commit(&mut self) {
        let me = self.pos(self.id);
        let mut m: Vec<Index> = (0..self.peers.len())
            .map(|i| {
                if i == me {
                    self.durable
                } else {
                    self.match_index[i]
                }
            })
            .collect();
        m.sort_unstable_by(|a, b| b.cmp(a));
        let one_ack = self.mutated(Mutation::CommitOnOneAck);
        let idx = if one_ack { m[0] } else { m[self.quorum() - 1] };
        // Only entries of the current term are committed by counting
        // (earlier ones ride along); this is the Raft figure-8 rule.
        let old_ok = self.mutated(Mutation::CommitOldTerm);
        if idx > self.commit && (one_ack || old_ok || self.term_at(idx) == self.term) {
            self.commit = idx;
            self.apply_committed();
        }
    }
}

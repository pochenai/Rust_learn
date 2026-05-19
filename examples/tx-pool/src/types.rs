use std::cmp::Ordering;

use tokio::sync::broadcast::{self, Sender};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SenderId(u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct TxId {
    sender: SenderId,
    nonce: u64,
}

#[derive(Debug, Clone)]
pub struct Tx {
    id: TxId,
    priority: u64,
}

impl From<u64> for SenderId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl TxId {
    pub fn new(sender: SenderId, nonce: u64) -> Self {
        Self { sender, nonce }
    }

    /// Smallest possible `TxId` for the given sender — useful as a range
    /// lower bound when iterating over a sender's txs.
    pub fn min_for_sender(sender: SenderId) -> Self {
        Self { sender, nonce: 0 }
    }

    pub fn sender(&self) -> SenderId {
        self.sender
    }
}

impl Tx {
    pub fn new(sender: impl Into<SenderId>, nonce: u64, gp: u64) -> Self {
        Self { id: TxId::new(sender.into(), nonce), priority: gp }
    }
    pub fn txid(&self) -> TxId {
        self.id
    }

    pub fn sender_id(&self) -> SenderId {
        self.id.sender
    }

    pub fn nonce(&self) -> u64 {
        self.id.nonce
    }

    pub fn unlocks(&self) -> TxId {
        TxId { sender: self.sender_id(), nonce: self.nonce() + 1 }
    }
}

impl Eq for Tx {}

impl PartialEq for Tx {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for Tx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Tx {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary key is priority (higher = better for block building). The
        // (sender, nonce) tie-breaker exists because `BTreeSet` uses `Ord` —
        // not `Eq` — for dedup: any `cmp` returning `Equal` makes `insert`
        // silently drop the new value. Without the tie-breaker, two txs at
        // the same priority would collapse to whichever landed first.
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.id.sender.cmp(&other.id.sender))
            .then_with(|| self.id.nonce.cmp(&other.id.nonce))
    }
}

/// Represents a changed account
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangedAccount {
    /// The address of the account.
    pub address: SenderId,
    /// Account nonce_next.
    pub nonce_next: u64,
}

pub type StateUpdate = Vec<ChangedAccount>;

#[derive(Clone, Debug)]
pub enum CanonStateNotification {
    Commit(StateUpdate),
}

// =========== Node ================//
/// Source of canonical chain notifications the pool must follow.
pub trait NodeTypes {
    fn chain_events(&self) -> BroadcastStream<CanonStateNotification>;
}

#[derive(Clone)]
pub struct Node<S> {
    tx: Sender<S>,
}

impl<S: Clone> Node<S> {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self { tx }
    }

    /// Broadcast an event to every current subscriber. `Err` from `send` only
    /// means "no live receivers right now" — fine to ignore for a demo node.
    pub fn emit(&self, ev: S) {
        let _ = self.tx.send(ev);
    }
}

impl NodeTypes for Node<CanonStateNotification> {
    fn chain_events(&self) -> BroadcastStream<CanonStateNotification> {
        // Each call hands out a fresh receiver subscribed to the same sender,
        // so the maintenance task and any other consumer get parallel views.
        BroadcastStream::new(self.tx.subscribe())
    }
}

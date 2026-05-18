use std::cmp::Ordering;

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

impl Tx {
    pub fn new(sender: impl Into<SenderId>, nonce: u64, gp: u64) -> Self {
        Self { id: TxId { sender: sender.into(), nonce }, priority: gp }
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
        TxId { sender: self.sender_id(), nonce: self.nonce() }
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
        self.priority.cmp(&other.priority)
    }
}

use crate::best::*;
use crate::types::*;
use imbl::OrdMap;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::{collections::hash_map::Entry, sync::Arc};

#[derive(Clone)]
pub struct Pool {
    inner: Arc<RwLock<TxPool>>,
}

impl Pool {
    pub fn new() -> Self {
        let pool = TxPool::default();
        Self { inner: Arc::new(RwLock::new(pool)) }
    }

    pub fn add_tx(&self, tx: Tx) {
        let mut pool = self.inner.write();
        pool.add_tx(tx);
    }

    pub fn best_txs(&self) -> Box<dyn Iterator<Item = Tx>> {
        let pool = self.inner.read();
        Box::new(pool.best())
    }

    pub fn on_canonical_state_change(&self, s: StateUpdate) {
        let mut pool = self.inner.write();
        pool.on_canonical_state_change(s);
    }
}

#[derive(Debug, Default)]
pub struct TxPool {
    pending: OrdMap<TxId, Tx>,
    independent_txs: FxHashMap<SenderId, Tx>,
}

impl TxPool {
    pub fn add_tx(&mut self, tx: Tx) {
        self.update_independents(&tx);
        self.pending.insert(tx.txid(), tx);
    }

    pub fn best(&self) -> BestTxs {
        BestTxs {
            all: self.pending.clone(),
            independent_txs: self.independent_txs.values().cloned().collect(),
        }
    }

    /// Apply a canonical state diff: drop transactions whose nonce was just
    /// mined and refresh each affected sender's independent transaction.
    fn on_canonical_state_change(&mut self, state: StateUpdate) {
        for acc in state {
            let sender = acc.address;

            // Collect-then-remove: `OrdMap` iterators borrow the map, so we
            // can't mutate it during iteration.
            let stale: Vec<TxId> = self
                .pending
                .range(TxId::min_for_sender(sender)..TxId::new(sender, acc.nonce_next))
                .map(|(id, _)| *id)
                .collect();
            for id in stale {
                self.pending.remove(&id);
            }

            // Smallest remaining tx for this sender; falls into the next
            // sender's keys (or off the end) when nothing is left.
            let next = self
                .pending
                .range(TxId::new(sender, acc.nonce_next)..)
                .next()
                .filter(|(id, _)| id.sender() == sender)
                .map(|(_, tx)| tx.clone());
            match next {
                Some(tx) => {
                    self.independent_txs.insert(sender, tx);
                }
                None => {
                    self.independent_txs.remove(&sender);
                }
            }
        }
    }

    // private fns
    fn update_independents(&mut self, tx: &Tx) {
        match self.independent_txs.entry(tx.sender_id()) {
            Entry::Occupied(mut e) => {
                if e.get().nonce() > tx.nonce() {
                    *e.get_mut() = tx.clone();
                }
            }
            Entry::Vacant(e) => {
                e.insert(tx.clone());
            }
        };
    }
}

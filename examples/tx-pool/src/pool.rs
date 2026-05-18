use crate::best::*;
use crate::types::*;
use imbl::OrdMap;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::{collections::hash_map::Entry, collections::BTreeSet, sync::Arc};
use thiserror::Error;

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
}

#[derive(Debug, Default)]
pub struct TxPool {
    pending: OrdMap<TxId, Tx>,
    independent_txs: FxHashMap<SenderId, Tx>,
}

#[derive(Debug, Error)]
#[error("[{msg}]")]
pub struct PoolError {
    msg: String,
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

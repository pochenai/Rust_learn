use crate::types::*;
use imbl::OrdMap;
use std::collections::BTreeSet;
pub struct BestTxs {
    pub(crate) all: OrdMap<TxId, Tx>,
    pub(crate) independent_txs: BTreeSet<Tx>,
}

impl BestTxs {
    pub fn next_best(&mut self) -> Option<Tx> {
        loop {
            let best = self.pop_best()?;

            if let Some(unlocked) = self.all.get(&best.unlocks()) {
                self.independent_txs.insert(unlocked.clone());
            }
            return Some(best);
        }
    }

    // private funcitons
    fn pop_best(&mut self) -> Option<Tx> {
        self.independent_txs.pop_last().inspect(|best| {
            self.all.remove(&best.txid());
        })
    }
}

impl Iterator for BestTxs {
    type Item = Tx;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_best()
    }
}

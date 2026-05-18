use crate::{
    pool::{Pool, TxPool},
    types::Tx,
};

mod best;
mod pool;
mod types;

fn test_txpool() {
    let pool = Pool::new();
    let tx = Tx::new(111, 0, 100);
    pool.add_tx(tx);
    let tx = Tx::new(111, 1, 110);
    pool.add_tx(tx);
    let tx = Tx::new(112, 0, 90);
    pool.add_tx(tx);

    let best_txs = pool.best_txs();
    for (id, tx) in best_txs.enumerate() {
        println!("best tx:{id},:{:?}", tx);
    }
}

fn main() {
    test_txpool();
}

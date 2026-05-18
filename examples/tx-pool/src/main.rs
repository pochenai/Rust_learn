mod best;
mod maintain;
mod pool;
mod types;

use crate::{
    maintain::spawn_maintain_tasks,
    pool::{Pool, TransactionPoolTr},
    types::{CanonStateNotification, ChangedAccount, Node, Tx},
};
use std::time::Duration;
use tokio::sync::oneshot;

fn print_best(label: &str, pool: &Pool) {
    println!("--- best txs ({label}) ---");
    for (i, tx) in pool.best_txs().enumerate() {
        println!("  best {i}: {tx:?}");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let pool = Pool::new();
    let node: Node<CanonStateNotification> = Node::new();

    // Seed three pending transactions: two from sender 111, one from 112.
    pool.add_tx(Tx::new(111u64, 0, 100));
    pool.add_tx(Tx::new(111u64, 1, 110));
    pool.add_tx(Tx::new(111u64, 2, 110));
    pool.add_tx(Tx::new(111u64, 3, 110));
    pool.add_tx(Tx::new(112u64, 0, 90));

    print_best("before block 1", &pool);

    // Wire up the maintenance loop. It subscribes to the node's broadcast
    // immediately inside `spawn_maintain_tasks`, so any event emitted after
    // this point is guaranteed to be delivered.
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = spawn_maintain_tasks(node.clone(), pool.clone(), shutdown_rx);

    // "Mine" a block: sender 111's on-chain nonce advances to 2. By Ethereum
    // semantics that means txs with nonces < 2 (i.e. 0 and 1) are mined and
    // should be pruned; nonce 2 is the *next* tx to execute and stays in the
    // pool — it becomes 111's new independent candidate.
    node.emit(CanonStateNotification::Commit(vec![ChangedAccount {
        address: 111.into(),
        nonce_next: 2,
    }]));

    // The apply runs on the blocking pool, so yield long enough for the
    // broadcast to arrive and the blocking task to complete.
    tokio::time::sleep(Duration::from_millis(50)).await;

    print_best("after block 1", &pool);

    node.emit(CanonStateNotification::Commit(vec![
        ChangedAccount { address: 111.into(), nonce_next: 3 },
        ChangedAccount { address: 112.into(), nonce_next: 1 },
    ]));
    tokio::time::sleep(Duration::from_millis(50)).await;

    print_best("after block 2", &pool);

    // Node-stop event: ask the maintenance loop to wind down, then wait for
    // it to actually exit before main returns.
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    println!("--- maintenance task stopped ---");
}

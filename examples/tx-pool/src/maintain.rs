use crate::{
    pool::TransactionPoolTr,
    types::{CanonStateNotification, NodeTypes},
};
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_stream::{Stream, StreamExt};

/// Spawn the pool's long-running maintenance task.
///
/// `shutdown` is the node-wide stop signal. The maintenance loop is meant to
/// live for the entire node lifetime: between blocks the chain-events stream
/// simply parks, and the loop should keep waiting rather than racing to exit
/// once the first notification has been consumed.
pub fn spawn_maintain_tasks<Node, P>(
    node: Node,
    pool: P,
    shutdown: oneshot::Receiver<()>,
) -> JoinHandle<()>
where
    Node: NodeTypes,
    P: TransactionPoolTr,
{
    // Drop broadcast `Lagged` errors here — the maintenance loop only acts on
    // successfully delivered notifications.
    let events = node.chain_events().filter_map(|ev| ev.ok());
    tokio::spawn(maintain_transaction_pool(events, pool, shutdown))
}

/// Apply canonical state updates to the pool until shutdown is signaled.
pub async fn maintain_transaction_pool<St, P>(events: St, pool: P, shutdown: oneshot::Receiver<()>)
where
    St: Stream<Item = CanonStateNotification> + Send + 'static,
    P: TransactionPoolTr,
{
    tokio::pin!(events, shutdown);
    loop {
        tokio::select! {
            // Global stop. Completes whether the sender resolved the channel
            // or just dropped it; either way the node is going down.
            _ = &mut shutdown => break,
            ev = events.next() => {
                let Some(ev) = ev else {
                    // The chain-events producer is gone. Park on shutdown so
                    // we only exit when the rest of the node also stops —
                    // never because a single block's notification was
                    // consumed.
                    let _ = (&mut shutdown).await;
                    break;
                };
                match ev {
                    CanonStateNotification::Commit(state) => {
                        pool.on_canonical_state_change(state);
                    }
                }
            }
        }
    }
}

# Rust Async 多事件常驻服务设计模式

> 来自 reth `transaction-pool` 的 `maintain_transaction_pool` + `spawn_critical_task` 组合。
> 通用形态：长期运行、监听多个异步事件源、在事件之间维护共享状态、不能阻塞主路径。

---

## 1. 核心理念

**"常驻"不靠循环，靠永远 ready 不完的 leaf future。**

| 维度 | 传统线程 | Rust async daemon |
|---|---|---|
| 谁推动执行 | OS 调度器 + 自家 while loop | runtime（tokio）+ waker |
| 空闲时占 CPU | spin 或自己 sleep | 0%，task 从调度队列摘除 |
| "活着"的本质 | 线程没退出 | 最内层 leaf future 永远 `Pending` |
| 唤醒源 | OS 信号、channel、timer 全自己处理 | leaf 注册 waker，事件源调 `wake()` |

业务代码层完全看不到 `Waker` / `Context` / `poll` —— 编译器把 `cx` 顺着 `.await` 自动接力传到 leaf；leaf 库（tokio、futures-channel、tokio-stream）内部统一处理 `cx.waker().clone() → 注册 → 事件来时 wake()`。

---

## 2. 五层洋葱模型

```
┌─────────────────────────────────────────────────────┐
│ 1. Runtime（tokio）                                 │  调度循环 / IO reactor / timer wheel
├─────────────────────────────────────────────────────┤
│ 2. Spawn 包装器（critical / panic / shutdown / 指标）│  一次性 select(shutdown, task)
├─────────────────────────────────────────────────────┤
│ 3. 透传适配器（catch_unwind / map / span / box）    │  Pending/Ready 透传，不改控制流
├─────────────────────────────────────────────────────┤
│ 4. 业务循环（loop { tokio::select! { … } }）        │  daemon 真正的"心脏"
├─────────────────────────────────────────────────────┤
│ 5. Leaf（stream / interval / oneshot / IO）         │  注册 waker，决定何时醒
└─────────────────────────────────────────────────────┘
```

每层职责：

- **第 1 层** 只做调度，不懂业务。
- **第 2 层** 横切关注点：panic 隔离、shutdown 联动、指标 —— "系统服务"特征，复用一次写一次。
- **第 3 层** 纯类型适配。Pending 进 Pending 出，Ready 进 Ready 出。
- **第 4 层** 业务核心：loop 体 = 一轮处理；select! 分支 = 关心的事件源。
- **第 5 层** 事件入口，决定 daemon 被谁唤醒。

---

## 3. 第 2 层：Spawn 包装器（一次性 select）

来自 `reth_tasks::TaskExecutor::spawn_critical_as` —— 仓库路径：
`crates/tasks/src/runtime.rs:540`（reth）。

```rust
fn spawn_critical_as<F>(
    &self,
    name: &'static str,
    fut: F,
    task_kind: TaskKind,
) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    self.0.metrics.inc_critical_tasks();
    let panicked_tasks_tx = self.0.task_events_tx.clone();
    let on_shutdown = self.0.on_shutdown.clone();

    // ① panic 隔离 + 上报：捕获 future 内部 panic，丢一个 PanickedTaskError 到 task_events_tx
    let task = std::panic::AssertUnwindSafe(fut)
        .catch_unwind()
        .map_err(move |error| {
            let task_error = PanickedTaskError::new(name, error);
            error!("{task_error}");
            let _ = panicked_tasks_tx.send(TaskEvent::Panic(task_error));
        })
        .in_current_span();   // ② tracing span 继承

    let finished_critical_tasks_total_metrics =
        self.0.metrics.finished_critical_tasks_total.clone();

    // ③ shutdown 联动 + drop 计数器
    let task = async move {
        let _inc_counter_on_drop = IncCounterOnDrop::new(finished_critical_tasks_total_metrics);
        let task = pin!(task);
        let _ = select(on_shutdown, task).await;   // ★ 一次性 select，不是 loop
    };

    self.spawn_on_rt(task, task_kind)              // ④ tokio::spawn(task)
}
```

**要点**：

- 这一层**没有任何 loop**，只 await 一次。
- 这一次 `.await` 可以**无限期挂起**，因为内部 `task`（包含业务循环）自己永远不 ready。
- 给 daemon 加上两条退出路径：
  - `task` 自然完成（业务 loop break）→ 优雅退出
  - `on_shutdown` 先 ready → `task` 被 drop 取消 → 强制退出
- **shutdown 在外层而不在 select! 分支里** = 业务代码不需要感知 shutdown。关注点分离。

---

## 4. 第 4 层：业务循环（多事件 select!）

来自 `maintain_transaction_pool` —— 仓库路径：
`crates/transaction-pool/src/maintain.rs:125`（reth）。

精简后的骨架：

```rust
pub async fn maintain_transaction_pool<…>(
    client: Client,
    pool: P,
    mut events: St,                        // canonical-state stream
    task_spawner: Runtime,
    config: MaintainPoolConfig,
) where … {
    // 跨轮共享的状态
    let mut blob_store_tracker = BlobStoreCanonTracker::default();
    let mut last_finalized_block = FinalizedBlockTracker::new(...);
    let mut dirty_addresses = HashSet::default();
    let mut maintained_state = MaintainedPoolState::InSync;

    // 跨轮共享的 future（用 Fuse 包，ready 一次后续 poll 安全返回 Pending）
    let mut reload_accounts_fut = Fuse::terminated();

    // tokio Interval：内部维护下一次 deadline
    let mut stale_eviction_interval = time::interval(config.max_tx_lifetime);

    let mut first_event = true;

    loop {
        // 每轮开头：处理跨轮状态 / 必要时 spawn 后台任务
        if maintained_state.is_drifted() { … }
        if !dirty_addresses.is_empty() && reload_accounts_fut.is_terminated() {
            let (tx, rx) = oneshot::channel();
            // … 把 dirty 账户拆 chunk
            task_spawner.spawn_blocking_task(async move {
                let res = load_accounts(c, at, accs_to_reload);
                let _ = tx.send(res);                 // ← 通知主循环
            });
            reload_accounts_fut = rx.fuse();          // ← 主循环挂在 rx 上等
        }
        if let Some(finalized) = last_finalized_block.update(...) { … }

        let mut event = None;
        let mut reloaded = None;

        // ★ 多事件等待：三选一
        tokio::select! {
            res = &mut reload_accounts_fut => {       // 跨轮：用 &mut，Fuse 包过
                reloaded = Some(res);
            }
            ev = events.next() => {                   // 一次性：每轮新建
                if ev.is_none() { break; }            // ← 唯一的"自然退出"
                event = ev;
                if first_event {
                    maintained_state = MaintainedPoolState::Drifted;
                    first_event = false;
                }
            }
            _ = stale_eviction_interval.tick() => {   // 一次性：每轮新建（Interval 自身保留 deadline）
                let queued = pool.queued_transactions();
                let stale_txs: Vec<_> = queued.into_iter()
                    .filter(|tx| (tx.origin.is_external() || config.no_local_exemptions)
                                  && now - tx.timestamp > config.max_tx_lifetime)
                    .map(|tx| *tx.hash())
                    .collect();
                pool.remove_transactions(stale_txs);
                pool.delete_blobs(stale_blobs);
            }
        }

        // 处理选中的分支结果（写在 select! 外面 = 不会被取消）
        match reloaded { … }
        let Some(event) = event else { continue };
        // 处理 event（Commit / Reorg / …）
        // 更新 pool / 清理 blob / 维护 dirty_addresses / …
    }
}
```

### 4.1 一次性 future vs 跨轮 future

```rust
events.next()      // 每轮新建：上轮 ready 后被 drop，下轮 stream 自己造一个新的 Next
interval.tick()    // 每轮新建：但 Interval 内部状态保留下次 deadline
&mut reload_fut    // 跨轮共享：用 Fuse 包，ready 一次后续 poll 安全返回 Pending
```

**判定规则**：
- 要保留状态（如 oneshot、长期 future、Fuse）→ `&mut`
- 可丢弃的（stream item、tick）→ 每轮新建

这是 select! 设计上最容易踩坑的地方。`Fuse` 的作用是配合 `&mut` 使用：oneshot 一旦 ready 一次就不能再 poll，Fuse 把后续 poll 都返回 Pending，避免 panic。

### 4.2 取消即清理

select! 的"赢家通吃"语义：未中标分支的 future 在这一轮被 drop。所以：

- 分支 future 必须 drop 安全（不漏 lock、不丢一半状态）
- **业务逻辑写在 `=>` 之后**，那段代码已经赢得这一轮，不会被取消

```rust
ev = events.next() => {
    // ✓ 这里写处理代码，已经赢得这一轮，不会被取消
    handle(ev);
}
```

### 4.3 CPU 密集 / 阻塞 IO 外包

主循环不能被阻塞。固定套路：spawn 出去 + oneshot 回报。

```rust
let (tx, rx) = oneshot::channel();
spawner.spawn_blocking(move || {
    let r = sync_work();
    let _ = tx.send(r);
});
let mut fut = rx.fuse();
// 主 loop 里
tokio::select! {
    r = &mut fut => { /* 拿到结果 */ }
    // … 其他分支正常运行
}
```

主 loop 永远不被阻塞调用拖慢，又能拿到结果。Event-loop 风格分而治之。

---

## 5. Waker 的"自动注册"链路

业务代码完全不写 waker，但 leaf 内部都注册了。原理：

### 5.1 Future trait 的契约

```rust
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
//                            ^^^^^^^^^^^^^^^^^^^ cx 装着当前 task 的 waker
```

`async` / `await` 编译后的等价物：

```rust
loop {
    match Pin::new(&mut fut).poll(cx) {   // cx 自动从外面接力传进来
        Poll::Ready(v) => break v,
        Poll::Pending => yield,           // 把 cx 还回去
    }
}
```

### 5.2 一次 poll 的"穿透"轨迹

runtime 一次 poll 顺着同步调用栈一路下去：

```
Task::poll(cx)
└─ outer async block 状态机.poll(cx)
   └─ select(on_shutdown, task).poll(cx)
      ├─ on_shutdown.poll(cx)               ─── 注册到 shutdown 信号
      └─ task.poll(cx)
         └─ catch_unwind/map_err/instrumented.poll(cx)   ← 透传
            └─ BoxFuture.poll(cx)                          ← 透传
               └─ maintain_transaction_pool 状态机.poll(cx)
                  └─ tokio::select! { … }.poll(cx)        ← 不注册，只分发 cx
                     ├─ Fuse<oneshot::Receiver>.poll(cx)
                     │   └─ atomic state.store(cx.waker().clone())   ★
                     ├─ Next<'_, St>.poll(cx)
                     │   └─ broadcast::Receiver.poll(cx)
                     │      └─ wakers_list.push(cx.waker().clone())  ★
                     └─ Interval::poll_tick(cx)
                        └─ Sleep.poll(cx)
                           └─ timer_driver.register(deadline, cx.waker().clone())  ★
```

只有 ★ 才是真正"注册 waker"的地方。中间所有层都只是把同一个 `cx` 往下递。

### 5.3 三个 leaf 的注册时机

| Leaf | poll 里的 Pending 路径 |
|---|---|
| `events.next()`（broadcast `Receiver`）| 检查 channel；没新消息 → `cx.waker().clone()` 塞进 channel waker list → `Pending`。`send()` 时遍历 list 调 `wake()` |
| `interval.tick()`（`Sleep`）| 比对 `now` 和 `deadline`；没到 → `cx.waker().clone()` 注册到 tokio 时间轮按 deadline 槽位 → `Pending`。timer driver 到点调 `wake()` |
| `&mut reload_accounts_fut`（`Fuse<oneshot::Receiver>`）| 检查 oneshot atomic state；没值 → CAS 把 `cx.waker()` 写进共享 state → `Pending`。`tx.send()` 时取出 waker 调 `wake()` |

**注意**：每次 poll 返回 Pending 都得重新确保 waker 已注册（Future trait 契约）。tokio 的 leaf 实现里通常会做 `will_wake` 检查避免重复 clone，但语义上每次 Pending 都隐式承诺"已注册"。

### 5.4 select! 的角色：不注册，只分发

`tokio::select!` 宏展开后大约：

```rust
poll_fn(|cx| {
    if let Poll::Ready(v) = Pin::new(&mut branch1).poll(cx) { return Poll::Ready(...); }
    if let Poll::Ready(v) = Pin::new(&mut branch2).poll(cx) { return Poll::Ready(...); }
    if let Poll::Ready(v) = Pin::new(&mut branch3).poll(cx) { return Poll::Ready(...); }
    Poll::Pending
})
```

**select! 自己不注册任何 waker**，只是把同一个 `cx` 依次喂给每条分支。三条分支各自在 leaf 注册一次 → 三个事件源都挂着同一个 task waker → 任一调 `wake()` 都把整个 task 塞回 ready queue。

这就是 `tokio::select!` 多路等待的真相 —— 它根本没做"多路复用"，借助 waker 的"一对多挂载"自动做到。

---

## 6. 控制流总图

```
事件源（broadcast / timer / oneshot）
        │ 调 wake()
        ▼
runtime ready queue
        │ poll(cx)
        ▼
outer async（一次性 select(shutdown, task)）
        │
        ▼
透传层（catch_unwind / map_err / instrumented）
        │
        ▼
业务循环（loop { tokio::select! { … } }）  ← daemon 心脏
        │  ├─ 选中分支：执行处理代码（不可取消）
        │  ├─ 落库 / 更新状态 / 必要时 spawn 后台任务
        │  └─ continue → 下一轮重建一次性 future
        ▼
leaf future（Pending 时把 cx.waker() 注册到事件源）
        │
        ▼
返回 Pending → 透传上去 → runtime 把 task 摘出调度队列
        │
        ▼
休眠（0% CPU），等 wake() 再来一次完整穿透
```

---

## 7. 适用与不适用

### 适合用这个模式

任何"长期监听多种事件源、需要在事件之间维护状态"的服务：

- 共识层 driver（监听 P2P 消息 / 区块 / 投票超时）
- 连接管理器（监听 incoming / outgoing / heartbeat / disconnect）
- 缓存维护（监听失效事件 / 容量水位 / 后台 GC tick）
- transaction pool 维护（链事件 / 定时清理 / 异步 reload）

特征：**多事件源 + 共享内部状态 + 不能阻塞主路径**。三者满足 = 甜区。

### 不适合

- **单事件源的纯流式处理** → 直接 `while let Some(x) = stream.next().await`，不需要 select!
- **CPU 密集长任务** → `spawn_blocking` 或专用线程池，async 套不进来
- **强顺序依赖的工作流** → actor / state machine + channel，select! 反而让顺序变模糊

---

## 8. 设计 checklist

写自己的 daemon 时对照：

- [ ] 是否所有阻塞 / CPU 密集 IO 都通过 `spawn_blocking` + oneshot 外包？
- [ ] select! 里每条分支都是非阻塞 leaf？
- [ ] 跨轮存活的 future 是否都用了 `&mut` + `Fuse`？
- [ ] 一次性 future（stream.next、interval.tick）是否每轮重建？
- [ ] 处理逻辑写在 `=>` 之后而不是分支 future 内部？
- [ ] shutdown 是否放在外层包装里，业务代码不感知？
- [ ] panic 是否被 `catch_unwind` 捕获并上报到监控？
- [ ] 自然退出条件是否清晰（通常是某个上游 stream 关闭）？
- [ ] Drift / 状态修复 / 异常重置路径在哪里处理？

---

## 9. 一句话总结

**Rust async daemon = 「永不 ready 的 leaf 们」+「业务循环 select 它们」+「runtime 反复 poll 把所有人串起来」。** 外层包装（`spawn_critical_task` 那种）只是装上 panic 报警、shutdown 阀门、指标探头 —— 不影响"daemon 为什么活着"这个核心问题。

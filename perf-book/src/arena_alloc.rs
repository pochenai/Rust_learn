/*
Arena (chunked bump) allocation with `bumpalo`. It's different with bulk allocator, because it can still dynamiclly allocate memory when needed.

Key idea: N small allocations are physically backed by ~1 large system malloc.
Each `arena.alloc()` only bumps an internal pointer inside a pre-allocated chunk;
the whole arena is freed at once when it goes out of scope.

Compared with `Box<T>`:
- 1000 `Box::new` ≈ 1000 mallocs + 1000 frees
- 1000 `arena.alloc` ≈ a few chunk mallocs (not 1) + 1 bulk free

It also enables self-referential graphs (nodes referencing other nodes by `&`)
that would otherwise fight the borrow checker.


在 Go 里，小对象分配通常不是每次都 syscall。Go runtime 自己有 mcache/mcentral/mheap，小分配大多在 runtime allocator 内部完成。arena 更主要减少的是：
sync.Pool 是“复用对象”：一个个同类型对象借出来，一个个对象还回去。
    - 主要解决的是高频临时对象分配带来的 runtime allocator 开销和 GC 压力，不是直接解决 syscall
arena 是“复用一整块生命周期区域”：一批对象一起分配，一起失效
    - 主要用于同一生命周期的大量复杂临时对象，把它们集中放进一个区域并整体释放，以降低分配开销和 GC 压力

所以:
普通业务热路径：
  先用对象复用、减少切片扩容、sync.Pool, 如:
    - bytes.Buffer
    - json encoder/decoder state
    - 临时 []byte buffer
    - 请求上下文对象
    - 压缩器/哈希器状态

复杂短生命周期对象图：
  考虑 arena 风格, 如:
    - SQL 查询执行中的临时表达式树
    - 编译器/parser 的 AST 临时节点
    - 数据库 memtable/skiplist

cargo test --bin perf-book arena_alloc:: -- --nocapture
*/

#[cfg(test)]
mod test {
    use bumpalo::Bump;

    struct Node<'a> {
        name: &'a str,
        children: Vec<&'a Node<'a>>,
    }

    fn build_tree<'a>(arena: &'a Bump) -> &'a Node<'a> {
        let leaf1 = arena.alloc(Node {
            name: "leaf1",
            children: vec![],
        });
        let leaf2 = arena.alloc(Node {
            name: "leaf2",
            children: vec![],
        });
        arena.alloc(Node {
            name: "root",
            children: vec![leaf1, leaf2],
        })
    }

    #[test]
    fn test_bulk_alloc_shares_chunks() {
        // Allocate many nodes; they are physically packed into the arena's chunks.
        let arena = Bump::new();
        let mut nodes: Vec<&mut u64> = Vec::with_capacity(1000);
        for i in 0..1000u64 {
            nodes.push(arena.alloc(i));
        }

        assert_eq!(nodes.len(), 1000);
        assert_eq!(*nodes[0], 0);
        assert_eq!(*nodes[999], 999);

        // Total bytes handed out >= 1000 * 8. The arena's allocated_bytes()
        // counts the underlying chunk capacity, which should comfortably cover it
        // with far fewer than 1000 system mallocs.
        assert!(arena.allocated_bytes() >= 1000 * size_of::<u64>());
    }

    #[test]
    fn test_self_referential_tree() {
        let arena = Bump::new();
        let root = build_tree(&arena);

        assert_eq!(root.name, "root");
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].name, "leaf1");
        assert_eq!(root.children[1].name, "leaf2");
    }

    #[test]
    fn test_alloc_str_is_dropless_friendly() {
        // String slices have no Drop, so they're cheap to arena-allocate.
        let arena = Bump::new();
        let a: &str = arena.alloc_str("hello");
        let b: &str = arena.alloc_str(" world");
        assert_eq!(a, "hello");
        assert_eq!(b, " world");
    }

    #[test]
    fn test_reset_reuses_capacity() {
        // `reset` drops the contents but keeps the largest chunk for reuse —
        // useful for per-request arenas in a hot loop.
        let mut arena = Bump::new();
        for _ in 0..100 {
            let _ = arena.alloc(42u64);
        }

        arena.reset();
        // After reset the arena is logically empty but still has a chunk ready,
        // so the next alloc doesn't need a fresh system malloc.
        let v: &mut u64 = arena.alloc(7u64);
        assert_eq!(*v, 7);
        assert!(arena.allocated_bytes() > 0);
    }
}

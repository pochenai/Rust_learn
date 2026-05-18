/*
Immutable data structures are data structures which can be copied and modified efficiently without altering the original.
MVCC in rust.
 */

#[cfg(test)]
mod test {
    use imbl::OrdMap;

    #[test]
    fn test_mvcc() {
        let mut a: OrdMap<i32, &str> = OrdMap::new();
        a.insert(1, "x");
        a.insert(2, "y");

        let b = a.clone(); // O(1)

        a.insert(3, "z");
        a.insert(2, "Y_NEW"); // 即使 key 已存在，也是 path-copy 出新叶子

        assert_eq!(a.get(&2), Some(&"Y_NEW"));
        assert_eq!(a.get(&3), Some(&"z"));

        assert_eq!(b.get(&2), Some(&"y")); // ← b 看到的还是老值
        assert_eq!(b.get(&3), None); // ← b 看不到 a 后插入的
    }
}

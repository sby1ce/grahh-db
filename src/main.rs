use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

use chrono::Utc;

/// key struct that is only gien out by the database to prevent non-existent keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Key(u64);

impl Key {
    pub fn generate() -> Self {
        Self(u64::try_from(Utc::now().timestamp_nanos_opt().unwrap()).unwrap())
    }
}

static EMPTY_HASHSET: LazyLock<HashSet<Key>> = LazyLock::new(HashSet::new);

/// TODO: it's possible to connect to the same node more than once with different kinds
///
/// TODO: it's possible to a node to connect to itself
#[derive(Debug)]
struct Node {
    value: String,
    connections: HashMap<String, HashSet<Key>>,
}

impl Node {
    pub fn new(value: String) -> Self {
        Self {
            value,
            connections: HashMap::new(),
        }
    }
    pub fn destruct(self) -> (impl Iterator<Item = Key>, String) {
        (
            self.connections
                .into_iter()
                .flat_map(|(_kind, nodes)| nodes.into_iter()),
            self.value,
        )
    }
    pub fn remove_connection(&mut self, key: &Key) {
        self.connections.iter_mut().for_each(|(_kind, nodes)| {
            nodes.remove(key);
        });
    }
    pub fn connect(&mut self, kind: String, key: Key) {
        if let Some(nodes) = self.connections.get_mut(&kind) {
            nodes.insert(key);
        } else {
            let nodes = HashSet::from([key]);
            self.connections.insert(kind, nodes);
        }
    }
    pub fn get_connections(&self, kind: &str) -> &HashSet<Key> {
        self.connections.get(kind).unwrap_or(&EMPTY_HASHSET)
    }
}

#[derive(Debug)]
struct Database {
    inner: HashMap<Key, Node>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
    pub fn insert(&mut self, value: String) -> (Key, Option<String>) {
        let key = Key::generate();
        let node = Node::new(value);
        let previous = self.inner.insert(key, node);
        (key, previous.map(|node| node.value))
    }
    pub fn remove(&mut self, key: Key) -> Option<String> {
        let node = self.inner.remove(&key)?;
        let (connections, value) = node.destruct();
        for ref connected in connections {
            self.inner
                .get_mut(connected)
                .unwrap()
                .remove_connection(&key);
        }
        Some(value)
    }
    pub fn connect(&mut self, first: Key, first_kind: String, second: Key, second_kind: String) {
        let [Some(node1), Some(node2)] = self.inner.get_disjoint_mut([&first, &second]) else {
            panic!("keys {first:?} {second:?} were non-existent!?");
        };
        node1.connect(first_kind, second);
        node2.connect(second_kind, first);
    }
    pub fn select(&self, key: &Key, kind: &str) -> &HashSet<Key> {
        let Some(node) = self.inner.get(key) else {
            return &EMPTY_HASHSET;
        };
        node.get_connections(kind)
    }
}

fn main() {
    let mut db = Database::new();
    let key1 = db.insert("Hello".to_owned()).0;
    let key2 = db.insert("World".to_owned()).0;
    assert_ne!(key1, key2);
    db.connect(key1, "_".to_owned(), key2, "!".to_owned());
    println!("{:?}", db.select(&key2, "!"));
    let _ = db.remove(key1);
    println!("{db:#?}");
}

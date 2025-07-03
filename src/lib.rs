use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    path::PathBuf,
    sync::LazyLock,
};

use chrono::Utc;
use serde::{Serialize, de::DeserializeOwned};

/// key struct that is only gien out by the database to prevent non-existent keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Key(u64);

impl Key {
    pub fn generate() -> Self {
        Self(u64::try_from(Utc::now().timestamp_nanos_opt().unwrap()).unwrap())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Value(Vec<u8>);

impl Value {
    pub fn serialize(value: &impl Serialize) -> Self {
        Self(postcard::to_allocvec(value).unwrap())
    }
    pub fn deserialize<T: DeserializeOwned>(&self) -> T {
        postcard::from_bytes(&self.0).unwrap()
    }
}

static EMPTY_HASHSET: LazyLock<HashSet<Key>> = LazyLock::new(HashSet::new);

/// TODO: it's possible to connect to the same node more than once with different kinds
///
/// TODO: it's possible to a node to connect to itself
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Node {
    value: Value,
    connections: HashMap<String, HashSet<Key>>,
}

impl Node {
    pub fn new(value: &impl Serialize) -> Self {
        let value = Value::serialize(value);
        Self {
            value,
            connections: HashMap::new(),
        }
    }
    pub fn destruct(self) -> (impl Iterator<Item = Key>, Value) {
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
    pub fn value(&self) -> &Value {
        &self.value
    }
}

#[derive(Debug)]
pub enum Storage {
    Memory,
    File(PathBuf),
}

impl Storage {
    fn save(&self, data: &HashMap<Key, Node>) {
        if let Self::File(path) = self {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)
                .unwrap();
            postcard::to_io(data, &mut file).unwrap();
        }
    }
}

#[derive(Debug)]
pub struct Database {
    inner: HashMap<Key, Node>,
    storage: Storage,
}

impl Database {
    pub fn insert<I: Serialize>(&mut self, value: &I) -> (Key, Option<Value>) {
        let key = Key::generate();
        let node = Node::new(value);
        let previous = self.inner.insert(key, node);
        (key, previous.map(|node| node.value))
    }
    pub fn remove(&mut self, key: Key) -> Option<Value> {
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
    pub fn get(&self, key: &Key) -> Option<&Value> {
        Some(self.inner.get(key)?.value())
    }
    pub fn load(path: PathBuf) -> Self {
        let inner = {
            if !path.is_file() {
                let _ = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                HashMap::new()
            } else {
                let file = OpenOptions::new().read(true).open(&path).unwrap();
                let mut buffer = vec![0; file.metadata().unwrap().len() as usize];
                let inner = postcard::from_io((file, &mut buffer)).unwrap().0;
                inner
            }
        };
        Self {
            inner,
            storage: Storage::File(path),
        }
    }
    pub fn save(&self) {
        self.storage.save(&self.inner);
    }
    pub fn in_memory() -> Self {
        Self {
            inner: HashMap::new(),
            storage: Storage::Memory,
        }
    }
}

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    fs::OpenOptions,
    num::ParseIntError,
    path::PathBuf,
    sync::LazyLock,
};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// key struct that is only gien out by the database to prevent non-existent keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Key(u64);

#[derive(Error, Debug)]
#[error("invalid key")]
pub struct KeyParseError(#[from] ParseIntError);

impl Key {
    pub fn generate() -> Self {
        Self(u64::try_from(Utc::now().timestamp_nanos_opt().unwrap()).unwrap())
    }
    pub fn parse(key: &str) -> Result<Self, KeyParseError> {
        Ok(Self(key.parse()?))
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key({})", self.0)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Value(Vec<u8>);

impl Value {
    pub fn serialize(value: &impl Serialize) -> Self {
        Self(postcard::to_allocvec(value).unwrap())
    }
    pub fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> Option<T> {
        postcard::from_bytes(&self.0).ok()
    }
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
    pub fn len(&self) -> usize {
        self.0.len()
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
    pub fn connections(&self) -> impl Iterator<Item = (&str, usize)> {
        self.connections.iter().filter_map(|(kind, connections)| {
            (!connections.is_empty()).then_some((kind.as_str(), connections.len()))
        })
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
    pub fn create<I: Serialize>(&mut self, value: &I) -> Key {
        let key = Key::generate();
        let node = Node::new(value);
        // this should always be `None` because otherwise we're having key generator collisions
        let previous: Option<Node> = self.inner.insert(key, node);
        assert!(previous.is_none(), "we're having key generator collisions");
        key
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
    pub fn get(&self, key: &Key) -> Option<&Node> {
        self.inner.get(key)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Node)> {
        self.inner.iter()
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
                postcard::from_io((file, &mut buffer)).unwrap().0
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

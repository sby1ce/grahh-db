use std::path::PathBuf;

use grahh_db::{Database, Value};

fn main() {
    let mut db = Database::load(PathBuf::from("db.grahh"));
    let key1 = db.insert(&["Hello"]).0;
    let key2 = db.insert(&["World"]).0;
    assert_ne!(key1, key2);
    db.connect(key1, "_".to_owned(), key2, "!".to_owned());
    let connections = db.select(&key2, "!");
    println!("{:?}", connections);
    let connected: &Value = db.get(connections.iter().next().unwrap()).unwrap();
    println!("{connected:?}");
    let retrieved: [String; 1] = connected.deserialize();
    println!("{retrieved:?}");
    let _ = db.remove(key1);
    println!("{db:#?}");
    db.save();
}

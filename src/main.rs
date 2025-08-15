use std::path::PathBuf;

use grahh_db::{Database, Value};
use rkyv::string::ArchivedString;

fn main() {
    let mut db = Database::load(PathBuf::from("db.grahh"));
    let key1 = db.create(&"Hello".to_owned());
    let key2 = db.create(&"World".to_owned());
    assert_ne!(key1, key2);
    db.connect(key1, "_".to_owned(), key2, "!".to_owned());
    let connections = db.select(&key2, "!");
    println!("{connections:?}");
    let connected: &Value = db.get(connections.iter().next().unwrap()).unwrap().value();
    println!("{connected:?}");
    let retrieved: Option<&ArchivedString> = connected.deserialize();
    println!("{retrieved:?}");
    let _ = db.remove(key1);
    println!("{db:#?}");
    db.save();
}

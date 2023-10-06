use std::env::args;

use anyhow::Result;
use serde::Deserialize;
use sqlite3_rs::{db::DB, record::serialization::row_id, row::Row};

#[derive(Debug, Deserialize)]
struct Crash {
    #[serde(with = "row_id")]
    id: u64,
    _year: i32,
    _lat: f64,
    _lng: f64,
    _severity: i32,
    _total_vehicles: i32,
}

impl Row for Crash {
    fn set_row_id(&mut self, row_id: u64) {
        self.id = row_id;
    }
}

fn main() {
    let path = args().nth(1).unwrap();
    let db = DB::open(&path).unwrap();

    let crashes = db.find_schema("crashes").unwrap();
    dbg!(&crashes);

    let head_dyn = crashes
        .root()
        .unwrap()
        .rows_dyn()
        .take(5)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    dbg!(head_dyn);

    let head = crashes
        .root()
        .unwrap()
        .rows::<Crash>()
        .take(5)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    dbg!(head);
}

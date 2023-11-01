use std::env::args;

use anyhow::Result;
use serde::Deserialize;
use squeak::{
    physical::db::DB,
    schema::{serialization::row_id, SchemaType, Table, WithRowId},
};

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

impl Table for Crash {
    const TYPE: SchemaType = SchemaType::Table;
    const NAME: &'static str = "crashes";
}

impl WithRowId for Crash {
    fn deserialize_row_id(&mut self, row_id: u64) {
        self.id = row_id;
    }
}

fn main() {
    let path = args().nth(1).unwrap();
    let db = DB::open(&path).unwrap();
    dbg!(&db);

    let crashes_table = db.table::<Crash>().unwrap();
    dbg!(&crashes_table);

    let first_10 = crashes_table
        .iter()
        .unwrap()
        .take(10)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    dbg!(first_10);

    let first_10 = crashes_table
        .get(1..=10)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    dbg!(first_10);

    let crash_100 = crashes_table.get(100).unwrap();
    dbg!(crash_100);
}

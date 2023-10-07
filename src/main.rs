use std::env::args;

use anyhow::{anyhow, Result};
use serde::Deserialize;
use sqlite3_rs::{db::DB, record::serialization::row_id, row::Row, schema::Table};

#[derive(Debug)]
struct CrashDB {
    crashes: Table<Crash>,
}

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

impl CrashDB {
    fn new(db: &DB) -> Result<Self> {
        let schema = db.schema()?;
        Ok(Self {
            crashes: schema
                .find(|s| s.name == "crashes")?
                .ok_or_else(|| anyhow!("Could not find \"crashes\" table"))?
                .into_table()?,
        })
    }
}

fn main() {
    let path = args().nth(1).unwrap();
    let db = DB::open(&path).unwrap();

    let crash_db = CrashDB::new(&db).unwrap();
    dbg!(&crash_db);

    let all_crashes = crash_db.crashes.iter().collect::<Result<Vec<_>>>().unwrap();
    dbg!(all_crashes.len());
    dbg!(&all_crashes[0..10]);
}

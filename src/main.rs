use std::env::args;

use serde::Deserialize;
use sqlite3_rs::{
    db::DB,
    row::Row,
    schema::{Schema, SchemaType},
};

#[derive(Debug, Clone, Deserialize)]
struct Empty {
    _id: i64,
}

impl Row for Empty {}

fn main() {
    let path = args().nth(1).unwrap();
    let db = DB::open(&path).unwrap();

    let root = db.root().unwrap();
    for schema in root.table_rows::<Schema>() {
        dbg!(&schema);

        if schema.type_ == SchemaType::Table {
            let root = schema.root().unwrap();

            let rows = root.table_rows::<Empty>().take(20).collect::<Vec<_>>();
            dbg!(rows.len());
            for row in rows {
                dbg!(row);
            }
        }
    }
}

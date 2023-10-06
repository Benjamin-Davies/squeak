use std::env::args;

use sqlite3_rs::{
    db::DB,
    schema::{Schema, SchemaType},
};

fn main() {
    let path = args().nth(1).unwrap();
    let db = DB::open(&path).unwrap();

    for schema in db.schema().unwrap().rows::<Schema>() {
        dbg!(&schema);

        if schema.type_ == SchemaType::Table {
            let root = schema.root().unwrap();

            let rows = root.rows_dyn().collect::<Vec<_>>();
            dbg!(&rows[..5]);
        }
    }
}

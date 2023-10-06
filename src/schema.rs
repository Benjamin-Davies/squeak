use std::fmt;

use anyhow::Result;
use serde::Deserialize;

use crate::{btree::BTreePage, db::DB, row::Row};

#[derive(Clone, Deserialize)]
pub struct Schema {
    #[serde(skip)]
    db: Option<DB>,
    #[serde(rename = "type")]
    pub type_: SchemaType,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: i64,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    Table,
    Index,
    View,
    Trigger,
}

impl Row for Schema {
    fn set_db(&mut self, db: &DB) {
        self.db = Some(db.clone());
    }
}

impl Schema {
    pub fn root(&self) -> Result<BTreePage> {
        let db = self.db.as_ref().unwrap();
        db.btree(self.rootpage as u32)
    }
}

impl fmt::Debug for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Skip the `db` field.
        f.debug_struct("Schema")
            .field("type_", &self.type_)
            .field("name", &self.name)
            .field("tbl_name", &self.tbl_name)
            .field("rootpage", &self.rootpage)
            .field("sql", &self.sql)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::DB;

    #[test]
    fn test_read_schema() {
        let db = DB::open("examples/empty.db").unwrap();
        let root = db.root().unwrap();

        let rows = root.table_rows::<Schema>().collect::<Vec<_>>();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].type_, SchemaType::Table);
        assert_eq!(rows[0].name, "empty");
        assert_eq!(rows[0].tbl_name, "empty");
        assert_eq!(rows[0].rootpage, 2);
        assert_eq!(
            rows[0].sql,
            "CREATE TABLE empty (id integer not null primary key)"
        );
    }
}

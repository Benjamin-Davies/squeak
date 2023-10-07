use std::marker::PhantomData;

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::{btree::BTreePage, db::DB, row::Row};

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone)]
pub struct Table<T> {
    pub(crate) root: BTreePage,
    pub(crate) _marker: PhantomData<T>,
}

impl Row for Schema {
    fn set_db(&mut self, db: &DB) {
        self.db = Some(db.clone());
    }
}

impl Schema {
    pub fn into_table<T>(self) -> Result<Table<T>> {
        if self.type_ != SchemaType::Table {
            return Err(anyhow!("Schema is not a table"));
        }
        // TODO: Validate that this table matches the type T.
        let root = self.root()?;

        Ok(Table {
            root,
            _marker: PhantomData,
        })
    }

    pub(crate) fn root(&self) -> Result<BTreePage> {
        let db = self.db.as_ref().unwrap();
        db.btree_page(self.rootpage as u32)
    }
}

impl<T: Row> Table<T> {
    pub fn iter(&self) -> impl Iterator<Item = Result<T>> + '_ {
        self.root.rows()
    }

    pub fn find(&self, mut predicate: impl FnMut(&T) -> bool) -> Result<Option<T>> {
        for row in self.iter() {
            let row = row?;
            if predicate(&row) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::DB;

    #[test]
    fn test_read_schema() {
        let db = DB::open("examples/empty.db").unwrap();
        let root = db.btree_page(1).unwrap();

        let rows = root.rows::<Schema>().collect::<Result<Vec<_>>>().unwrap();
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

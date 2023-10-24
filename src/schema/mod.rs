use std::marker::PhantomData;

use anyhow::{anyhow, Result};
use serde::{
    de::{DeserializeOwned, IntoDeserializer},
    Deserialize,
};

use crate::physical::{btree::BTreePage, buf::ArcBufSlice, db::DB};

use self::record::Record;

pub mod range;
pub mod record;
pub mod serialization;

#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    pub type_: SchemaType,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u32,
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

pub trait Table: DeserializeOwned {
    const NAME: &'static str;

    fn deserialize_row_id(&mut self, _row_id: u64) {}
}

fn deserialize_row<T: Table>((row_id, buf): (u64, ArcBufSlice)) -> Result<T> {
    let record = Record::from(buf);
    let mut value = T::deserialize(record.into_deserializer())?;
    value.deserialize_row_id(row_id);
    Ok(value)
}

impl Table for Schema {
    const NAME: &'static str = "sqlite_schema";
}

#[derive(Debug, Clone)]
pub struct TableHandle<T> {
    db: DB,
    rootpage: u32,
    _marker: PhantomData<T>,
}

impl<T: Table> TableHandle<T> {
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<T>>> {
        let records = self.rootpage()?.into_entries();
        let rows = records.map(|entry| deserialize_row(entry?));
        Ok(rows)
    }

    pub fn rootpage(&self) -> Result<BTreePage> {
        self.db.btree_page(self.rootpage)
    }
}

impl DB {
    pub fn table<T: Table>(&self) -> Result<TableHandle<T>> {
        let rootpage = if T::NAME == Schema::NAME {
            1
        } else {
            let mut rootpage = None;
            for schema in self.table::<Schema>()?.iter()? {
                let schema = schema?;
                if schema.name == T::NAME {
                    rootpage = Some(schema.rootpage);
                    break;
                }
            }
            rootpage.ok_or_else(|| anyhow!("Table {} not found in schema", T::NAME))?
        };

        Ok(TableHandle {
            db: self.clone(),
            rootpage,
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::physical::db::DB;

    #[test]
    fn test_read_schema() {
        let db = DB::open("examples/empty.db").unwrap();

        let rows = db
            .table::<Schema>()
            .unwrap()
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
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

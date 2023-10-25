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
    pub sql: Option<String>,
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

pub trait Index: DeserializeOwned {
    type IndexedFields;

    const NAME: &'static str;

    fn into_indexed_fields(self) -> Self::IndexedFields;
}

fn deserialize_table_row<T: Table>((row_id, buf): (u64, ArcBufSlice)) -> Result<T> {
    let record = Record::from(buf);
    let mut value = T::deserialize(record.into_deserializer())?;
    value.deserialize_row_id(row_id);
    Ok(value)
}

fn deserialize_index_row<I: Index>(buf: ArcBufSlice) -> Result<I> {
    let record = Record::from(buf);
    let value = I::deserialize(record.into_deserializer())?;
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

#[derive(Debug, Clone)]
pub struct IndexHandle<I> {
    db: DB,
    rootpage: u32,
    _marker: PhantomData<I>,
}

impl<T: Table> TableHandle<T> {
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<T>>> {
        let records = self.rootpage()?.into_table_entries();
        let rows = records.map(|entry| deserialize_table_row(entry?));
        Ok(rows)
    }

    pub(crate) fn rootpage(&self) -> Result<BTreePage> {
        self.db.btree_page(self.rootpage)
    }
}

impl<I: Index> IndexHandle<I> {
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<I>>> {
        let records = self.rootpage()?.into_index_entries();
        let rows = records.map(|entry| deserialize_index_row(entry?));
        Ok(rows)
    }

    pub fn get<'a>(&'a self, matching: &'a I::IndexedFields) -> Result<Option<I>>
    where
        I::IndexedFields: Ord,
    {
        let entry = self
            .rootpage()?
            .into_index_entries_range(|entry| {
                // TODO: Do we need to deserialize everything?
                let row = deserialize_index_row::<I>(entry)?;
                let ordering = row.into_indexed_fields().cmp(matching);
                Ok(ordering)
            })?
            .next();
        let row = entry.map(|entry| deserialize_index_row(entry?));
        row.transpose()
    }

    pub(crate) fn rootpage(&self) -> Result<BTreePage> {
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
                if schema.type_ == SchemaType::Table && schema.name == T::NAME {
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

    pub fn index<I: Index>(&self) -> Result<IndexHandle<I>> {
        let mut rootpage = None;
        for schema in self.table::<Schema>()?.iter()? {
            let schema = schema?;
            if schema.type_ == SchemaType::Index && schema.name == I::NAME {
                rootpage = Some(schema.rootpage);
                break;
            }
        }
        let rootpage = rootpage.ok_or_else(|| anyhow!("Index {} not found in schema", I::NAME))?;

        Ok(IndexHandle {
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

    #[derive(Debug, Clone, Deserialize)]
    struct Empty;

    impl Table for Empty {
        const NAME: &'static str = "empty";
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
    struct StringPK {
        pub string: String,
        pub key: u64,
    }

    impl Index for StringPK {
        type IndexedFields = (String,);

        const NAME: &'static str = "sqlite_autoindex_strings_1";

        fn into_indexed_fields(self) -> Self::IndexedFields {
            (self.string,)
        }
    }

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
            rows[0].sql.as_ref().unwrap(),
            "CREATE TABLE empty (id integer not null primary key)"
        );
    }

    #[test]
    fn test_read_table() {
        let db = DB::open("examples/empty.db").unwrap();

        let row_count = db.table::<Empty>().unwrap().iter().unwrap().count();
        assert_eq!(row_count, 0);
    }

    #[test]
    fn test_read_index() {
        let db = DB::open("examples/string_index.db").unwrap();

        let index = db.index::<StringPK>().unwrap();
        let rows = index.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(
            rows,
            vec![
                StringPK {
                    string: "bar".to_owned(),
                    key: 2,
                },
                StringPK {
                    string: "baz".to_owned(),
                    key: 3,
                },
                StringPK {
                    string: "foo".to_owned(),
                    key: 1,
                },
            ]
        );
    }

    #[test]
    fn test_search_index() {
        let db = DB::open("examples/string_index.db").unwrap();

        let index = db.index::<StringPK>().unwrap();
        let index_entry = index.get(&("foo".to_owned(),)).unwrap();
        assert_eq!(
            index_entry,
            Some(StringPK {
                string: "foo".to_owned(),
                key: 1,
            })
        );
    }
}

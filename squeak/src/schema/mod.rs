use std::marker::PhantomData;

use anyhow::{anyhow, Result};
use serde::{
    de::{DeserializeOwned, IntoDeserializer},
    Deserialize,
};
use squeak_macros::Table;

use crate::physical::{btree::BTreePage, db::ReadDB};

use self::record::Record;

pub mod range;
pub mod record;
pub mod serialization;

#[derive(Debug, Clone, Deserialize, Table)]
#[table(name = "sqlite_schema")]
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
    const TYPE: SchemaType;
    const NAME: &'static str;
}

pub trait WithRowId: Table {
    fn deserialize_row_id(&mut self, _row_id: i64) {}
}

pub trait WithoutRowId: Table {
    type SortedFields: Ord;

    fn into_sorted_fields(self) -> Self::SortedFields;
}

pub trait Index<T: Table>: WithoutRowId {
    fn get_row_id(&self) -> i64;
}

fn deserialize_record_with_row_id<T: WithRowId>((row_id, buf): (i64, &[u8])) -> Result<T> {
    let record = Record::from(buf);
    let mut value = T::deserialize(record.into_deserializer())?;
    value.deserialize_row_id(row_id);
    Ok(value)
}

fn deserialize_record<T: DeserializeOwned>(buf: &[u8]) -> Result<T> {
    let record = Record::from(buf);
    let value = T::deserialize(record.into_deserializer())?;
    Ok(value)
}

#[derive(Debug, Clone, Copy)]
pub struct TableHandle<'db, T, DB: ?Sized> {
    db: &'db DB,
    rootpage: u32,
    _marker: PhantomData<T>,
}

impl<'db, T: Table, DB: ReadDB> TableHandle<'db, T, DB> {
    pub fn get_with_index<I: Index<T>>(&self, matching: &I::SortedFields) -> Result<Option<T>>
    where
        // TODO: Use indexes with non-rowid tables
        T: WithRowId,
    {
        let index = self.db.table::<I>()?;
        let entry = index.get(matching)?;
        let row = entry
            .map(|entry| self.get(entry.get_row_id()))
            .transpose()?
            .flatten();
        Ok(row)
    }

    pub(crate) fn rootpage(&self) -> Result<BTreePage<DB>> {
        BTreePage::new(self.db, self.rootpage)
    }
}

pub trait ReadSchema: ReadDB {
    fn table<T: Table>(&self) -> Result<TableHandle<T, Self>>;
}

impl<DB: ReadDB> ReadSchema for DB {
    fn table<T: Table>(&self) -> Result<TableHandle<T, DB>> {
        let rootpage = if T::NAME == Schema::NAME {
            1
        } else {
            let mut rootpage = None;
            for schema in self.table::<Schema>()?.iter()? {
                let schema = schema?;
                if schema.type_ == T::TYPE && schema.name == T::NAME {
                    rootpage = Some(schema.rootpage);
                    break;
                }
            }
            rootpage.ok_or_else(|| anyhow!("Table {} not found in schema", T::NAME))?
        };

        Ok(TableHandle {
            db: self,
            rootpage,
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::physical::db::DB;

    #[derive(Debug, Clone, Deserialize, Table)]
    struct Empty {}

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Table)]
    struct Strings {
        #[table(primary_key)]
        pub string: String,
    }

    #[test]
    fn test_read_schema() {
        let db = DB::open("examples/empty.db").unwrap();

        assert_eq!(Schema::NAME, "sqlite_schema");

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

        assert_eq!(Empty::NAME, "empty");

        let row_count = db.table::<Empty>().unwrap().iter().unwrap().count();
        assert_eq!(row_count, 0);
    }

    #[test]
    fn test_read_index() {
        let db = DB::open("examples/string_index.db").unwrap();

        assert_eq!(StringsPK::NAME, "sqlite_autoindex_strings_1");

        let index = db.table::<StringsPK>().unwrap();
        let rows = index
            .iter_without_row_id()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                StringsPK {
                    string: "bar".to_owned(),
                    key: 2,
                },
                StringsPK {
                    string: "baz".to_owned(),
                    key: 3,
                },
                StringsPK {
                    string: "foo".to_owned(),
                    key: 1,
                },
            ]
        );
    }

    #[test]
    fn test_search_index() {
        let db = DB::open("examples/string_index.db").unwrap();

        let index = db.table::<StringsPK>().unwrap();
        let index_entry = index.get(&("foo".to_owned(),)).unwrap();
        assert_eq!(
            index_entry,
            Some(StringsPK {
                string: "foo".to_owned(),
                key: 1,
            })
        );
    }

    #[test]
    fn test_search_with_index() {
        let db = DB::open("examples/string_index.db").unwrap();

        assert_eq!(Strings::NAME, "strings");

        let table = db.table::<Strings>().unwrap();
        let entry = table
            .get_with_index::<StringsPK>(&("bar".to_owned(),))
            .unwrap();
        assert_eq!(
            entry,
            Some(Strings {
                string: "bar".to_owned(),
            })
        );
    }

    #[test]
    fn test_search_with_index_in_transaction() {
        let mut db = DB::open("examples/string_index.db").unwrap();

        let transaction = db.begin_transaction().unwrap();

        let table = transaction.table::<Strings>().unwrap();
        let entry = table
            .get_with_index::<StringsPK>(&("bar".to_owned(),))
            .unwrap();
        assert_eq!(
            entry,
            Some(Strings {
                string: "bar".to_owned(),
            })
        );
    }
}

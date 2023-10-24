use serde::de::DeserializeOwned;

use crate::physical::db::DB;

pub trait Row: DeserializeOwned {
    fn set_db(&mut self, _db: &DB) {}
    fn set_row_id(&mut self, _row_id: u64) {}
}

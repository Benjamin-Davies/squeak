use serde::de::DeserializeOwned;

use crate::db::DB;

pub trait Row: DeserializeOwned {
    fn set_db(&mut self, _db: &DB) {}
}

use anyhow::Result;

use super::transaction::Transaction;

pub(crate) fn pop_page(transaction: &mut Transaction) -> Result<Option<u32>> {
    if transaction.freelist_count == 0 {
        return Ok(None);
    }

    todo!()
}

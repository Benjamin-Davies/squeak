use syn::{parse_macro_input, DeriveInput, Field, Ident};

use crate::{gen::gen_table_impls, parse::parse_input};

mod gen;

struct Table {
    ident: Ident,
    schema_type: Ident,
    name: String,
    pk_field: Option<Field>,
    row_id_field: Option<Field>,
}

#[proc_macro_derive(Table, attributes(table))]
pub fn derive_table(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let table = parse_input(input);

    gen_table_impls(table).into()
}

mod parse;

use quote::{format_ident, quote, TokenStreamExt};

use super::Table;

pub(crate) fn gen_table_impls(table: Table) -> proc_macro2::TokenStream {
    let Table {
        ident,
        schema_type,
        name,
        pk_field,
        row_id_field,
    } = table;

    let row_id_fn = if let Some(row_id_field) = row_id_field {
        let row_id_ident = row_id_field.ident.as_ref().unwrap();
        Some(quote!(
            fn deserialize_row_id(&mut self, row_id: i64) {
                self.#row_id_ident = row_id;
            }
        ))
    } else {
        None
    };

    let mut result = quote!(
        impl Table for #ident {
            const TYPE: SchemaType = SchemaType::#schema_type;
            const NAME: &'static str = #name;

            fn schemas() -> Vec<Schema> {
                vec![Schema {
                    type_: Self::TYPE,
                    name: Self::NAME.to_owned(),
                    tbl_name: Self::NAME.to_owned(),
                    rootpage: 0,
                    sql: None, // TODO
                }]
                // TODO: Indexes
            }
        }

        impl WithRowId for #ident {
            #row_id_fn
        }
    );

    if let Some(pk_field) = pk_field {
        let pk_index_ident = format_ident!("{}PK", ident);
        let pk_index_name = format!("sqlite_autoindex_{}_1", name);
        let pk_field_ident = pk_field.ident.as_ref().unwrap();
        let pk_field_ty = &pk_field.ty;

        result.append_all(quote!(
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
            struct #pk_index_ident {
                #pk_field_ident: #pk_field_ty,
                key: i64,
            }

            impl Table for #pk_index_ident {
                const TYPE: SchemaType = SchemaType::Index;
                const NAME: &'static str = #pk_index_name;

                fn schemas() -> Vec<Schema> {
                    todo!()
                }
            }

            impl WithoutRowId for #pk_index_ident {
                type SortedFields = (#pk_field_ty,);

                fn into_sorted_fields(self) -> Self::SortedFields {
                    (self.#pk_field_ident,)
                }
            }

            impl Index<#ident> for #pk_index_ident {
                fn get_row_id(&self) -> i64 {
                    self.key
                }
            }
        ));
    }

    result
}

use convert_case::{Case, Casing};
use quote::{format_ident, quote, TokenStreamExt};
use syn::{parse_macro_input, Data, DeriveInput, Expr, Field, Fields, Ident, Lit, Path};

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

    table_impls(table).into()
}

fn parse_input(input: DeriveInput) -> Table {
    let ident = input.ident.clone();
    let Data::Struct(struct_) = input.data else {
        unimplemented!("non-struct input");
    };
    let Fields::Named(fields) = struct_.fields else {
        unimplemented!("no named fields");
    };

    let schema_type = format_ident!("Table");
    let mut name = ident.to_string().to_case(Case::Snake);
    let mut pk_field = None;
    let mut row_id_field = None;

    for attr in &input.attrs {
        if into_ident(attr.path()) == "table" {
            let arg = attr.parse_args::<Expr>().unwrap();
            let Expr::Assign(assign) = arg else {
                unimplemented!("non-assign attribute");
            };
            let Expr::Path(left) = *assign.left else {
                unimplemented!("non-path left-hand side");
            };
            match into_ident(&left.path).to_string().as_str() {
                "name" => {
                    let Expr::Lit(lit) = *assign.right else {
                        unimplemented!("non-literal right-hand side");
                    };
                    let Lit::Str(lit) = lit.lit else {
                        unimplemented!("non-string literal");
                    };
                    name = lit.value();
                }
                _ => unimplemented!("unknown attribute"),
            }
        }
    }

    for field in &fields.named {
        for attr in &field.attrs {
            if into_ident(attr.path()) == "table" {
                let arg = attr.parse_args::<Path>().unwrap();
                match into_ident(&arg).to_string().as_str() {
                    "primary_key" => {
                        pk_field = Some(field.clone());
                    }
                    "row_id" => {
                        row_id_field = Some(field.clone());
                    }
                    _ => unimplemented!("unknown attribute"),
                }
            }
        }
    }

    Table {
        ident,
        schema_type,
        name,
        pk_field,
        row_id_field,
    }
}

fn table_impls(table: Table) -> proc_macro2::TokenStream {
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
            fn deserialize_row_id(&mut self, row_id: u64) {
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
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
            struct #pk_index_ident {
                #pk_field_ident: #pk_field_ty,
                key: u64,
            }

            impl Table for #pk_index_ident {
                const TYPE: SchemaType = SchemaType::Index;
                const NAME: &'static str = #pk_index_name;
            }

            impl WithoutRowId for #pk_index_ident {
                type SortedFields = (#pk_field_ty,);

                fn into_sorted_fields(self) -> Self::SortedFields {
                    (self.#pk_field_ident,)
                }
            }

            impl Index<#ident> for #pk_index_ident {
                fn get_row_id(&self) -> u64 {
                    self.key
                }
            }
        ));
    }

    result
}

fn into_ident(path: &Path) -> Ident {
    assert_eq!(path.segments.len(), 1);
    let path_segment = &path.segments[0];
    assert!(path_segment.arguments.is_empty());
    path_segment.ident.clone()
}

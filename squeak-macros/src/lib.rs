use convert_case::{Case, Casing};
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Expr, Ident, Lit, Path};

#[proc_macro_derive(Table, attributes(table))]
pub fn derive_table(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let ident = input.ident;
    let Data::Struct(_struct) = input.data else {
        unimplemented!("non-struct input");
    };

    let schema_type = format_ident!("Table");
    let mut name = ident.to_string().to_case(Case::Snake);

    for attr in input.attrs {
        match into_ident(&attr.meta.path()).to_string().as_str() {
            "table" => {
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
            ident => unimplemented!("{ident}"),
        }
    }

    quote!(
        impl Table for #ident {
            const TYPE: SchemaType = SchemaType::#schema_type;
            const NAME: &'static str = #name;
        }

        impl WithRowId for #ident {}
    )
    .into()
}

fn into_ident(path: &Path) -> Ident {
    assert_eq!(path.segments.len(), 1);
    let path_segment = &path.segments[0];
    assert!(path_segment.arguments.is_empty());
    path_segment.ident.clone()
}

use convert_case::{Case, Casing};
use quote::format_ident;
use syn::{Attribute, Data, DeriveInput, Expr, Field, Fields, FieldsNamed, Ident, Lit, Path, Type};

use crate::{Column, SqlType, Table};

pub(crate) fn parse_input(input: DeriveInput) -> Table {
    let ident = input.ident.clone();
    let Data::Struct(struct_) = input.data else {
        unimplemented!("non-struct input");
    };
    let Fields::Named(fields) = struct_.fields else {
        unimplemented!("no named fields");
    };

    let schema_type = format_ident!("Table");
    let default_name = ident.to_string().to_case(Case::Snake);

    let name = parse_struct_attrs(input.attrs).unwrap_or(default_name);
    let (columns, pk_field, row_id_field) = parse_fields(fields);

    Table {
        ident,
        schema_type,
        name,
        columns,
        pk_field,
        row_id_field,
    }
}

fn parse_struct_attrs(attrs: Vec<Attribute>) -> Option<String> {
    let mut name = None;

    for attr in attrs {
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
                    name = Some(lit.value());
                }
                _ => unimplemented!("unknown attribute"),
            }
        }
    }

    name
}

fn parse_fields(fields: FieldsNamed) -> (Vec<Column>, Option<Field>, Option<Field>) {
    let mut columns = Vec::new();
    let mut pk_field = None;
    let mut row_id_field = None;

    for field in fields.named {
        let mut pk = false;

        for attr in &field.attrs {
            if into_ident(attr.path()) == "table" {
                let arg = attr.parse_args::<Path>().unwrap();
                match into_ident(&arg).to_string().as_str() {
                    "primary_key" => {
                        pk_field = Some(field.clone());
                        pk = true;
                    }
                    "row_id" => {
                        row_id_field = Some(field.clone());
                        pk = true;
                    }
                    _ => unimplemented!("unknown attribute"),
                }
            }
        }

        let ty = match field.ty {
            Type::Path(type_path) => {
                let ident = &type_path.path.segments.last().unwrap().ident;
                match ident.to_string().as_str() {
                    "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" => SqlType::Integer,
                    "f32" | "f64" => SqlType::Real,
                    "String" => SqlType::Text,
                    "Vec" => SqlType::Blob,
                    _ => SqlType::None,
                }
            }
            _ => unimplemented!("unknown type"),
        };

        let column = Column {
            name: field.ident.as_ref().unwrap().to_string(),
            ty,
            pk,
        };
        columns.push(column);
    }

    (columns, pk_field, row_id_field)
}

fn into_ident(path: &Path) -> Ident {
    assert_eq!(path.segments.len(), 1);
    let path_segment = &path.segments[0];
    assert!(path_segment.arguments.is_empty());
    path_segment.ident.clone()
}

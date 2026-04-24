//! Procedural macros backing the `macros` facade crate.
//!
//! This crate should not be used directly. Depend on the `macros` crate
//! instead, which re-exports the derives along with the runtime trait.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, DeriveInput, Expr, ExprLit, Lit, Meta, parse_macro_input};

/// Derive macro that implements the `macros::Table` trait for the annotated
/// struct.
///
/// By default the table name is derived from the struct identifier by
/// converting it to `snake_case` and pluralizing it (simple English rules:
/// appends `es` if the name ends with `s`, `sh`, `ch`, `x`, or `z`, otherwise
/// appends `s`).
///
/// The default behaviour can be overridden with the `#[table(name = "...")]`
/// attribute.
#[proc_macro_derive(Table, attributes(table))]
pub fn derive_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let table_name = match parse_table_name(&input.attrs) {
        Ok(Some(name)) => name,
        Ok(None) => default_table_name(&struct_ident.to_string()),
        Err(err) => return err.to_compile_error().into(),
    };

    let expanded = quote! {
        #[automatically_derived]
        impl #impl_generics ::macros::Table for #struct_ident #ty_generics #where_clause {
            #[inline]
            fn table_name() -> &'static str {
                #table_name
            }
        }
    };

    expanded.into()
}

/// Looks for a `#[table(name = "...")]` attribute and returns the name if
/// present.
fn parse_table_name(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    let mut result: Option<String> = None;

    for attr in attrs {
        if !attr.path().is_ident("table") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value = meta.value()?;
                let expr: Expr = value.parse()?;
                match expr {
                    Expr::Lit(ExprLit {
                        lit: Lit::Str(s), ..
                    }) => {
                        result = Some(s.value());
                        Ok(())
                    }
                    _ => Err(meta.error("expected string literal for `name`")),
                }
            } else {
                Err(meta.error("unsupported `table` attribute, expected `name`"))
            }
        })?;

        // Also support the rare case of `#[table = "..."]`-style via Meta::NameValue.
        if result.is_none() {
            if let Meta::NameValue(nv) = &attr.meta {
                if let Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
                }) = &nv.value
                {
                    result = Some(s.value());
                }
            }
        }
    }

    Ok(result)
}

/// Converts `CamelCase` / `PascalCase` identifiers to `snake_case` and
/// pluralizes the result using simple English rules.
fn default_table_name(ident: &str) -> String {
    let snake = to_snake_case(ident);
    pluralize(&snake)
}

fn to_snake_case(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 4);
    let chars: Vec<char> = ident.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let prev = if i == 0 {
                None
            } else {
                chars.get(i - 1).copied()
            };
            let next = chars.get(i + 1).copied();

            let should_underscore = match (prev, next) {
                // Not at the very start
                (None, _) => false,
                // Previous char is lowercase or a digit: boundary like `aB` -> `a_b`
                (Some(p), _) if p.is_lowercase() || p.is_ascii_digit() => true,
                // Previous is uppercase, next is lowercase: boundary like `HTTPServer` -> `http_server`
                (Some(p), Some(n)) if p.is_uppercase() && n.is_lowercase() => true,
                _ => false,
            };

            if should_underscore {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }

    out
}

fn pluralize(name: &str) -> String {
    if name.is_empty() {
        return name.to_string();
    }

    let lower = name.to_ascii_lowercase();
    let needs_es = lower.ends_with('s')
        || lower.ends_with("sh")
        || lower.ends_with("ch")
        || lower.ends_with('x')
        || lower.ends_with('z');

    let mut out = String::with_capacity(name.len() + 2);
    out.push_str(name);
    if needs_es {
        out.push_str("es");
    } else {
        out.push('s');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_case_basic() {
        assert_eq!(to_snake_case("User"), "user");
        assert_eq!(to_snake_case("UserProfile"), "user_profile");
        assert_eq!(to_snake_case("HTTPServer"), "http_server");
        assert_eq!(to_snake_case("HTTP"), "http");
        assert_eq!(to_snake_case("A"), "a");
        assert_eq!(to_snake_case("IOError"), "io_error");
    }

    #[test]
    fn pluralize_basic() {
        assert_eq!(pluralize("user"), "users");
        assert_eq!(pluralize("box"), "boxes");
        assert_eq!(pluralize("class"), "classes");
        assert_eq!(pluralize("dish"), "dishes");
        assert_eq!(pluralize("watch"), "watches");
        assert_eq!(pluralize("buzz"), "buzzes");
        assert_eq!(pluralize("event"), "events");
    }

    #[test]
    fn default_table_name_combines() {
        assert_eq!(default_table_name("User"), "users");
        assert_eq!(default_table_name("UserProfile"), "user_profiles");
        assert_eq!(default_table_name("Box"), "boxes");
        assert_eq!(default_table_name("HTTPClass"), "http_classes");
    }
}

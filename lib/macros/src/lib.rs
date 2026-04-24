//! Runtime traits plus re-exports of the derive macros for chutils.
//!
//! # Example
//!
//! ```
//! use macros::Table;
//!
//! #[derive(Table)]
//! struct User;
//!
//! #[derive(Table)]
//! #[table(name = "my_events")]
//! struct Event;
//!
//! assert_eq!(<User as macros::Table>::table_name(), "users");
//! assert_eq!(<Event as macros::Table>::table_name(), "my_events");
//! ```

/// A type that corresponds to a database table.
///
/// The [`Table`](macro@Table) derive macro implements this trait automatically,
/// deriving the table name from the struct identifier (snake_case + simple
/// pluralization) or from an explicit `#[table(name = "...")]` attribute.
pub trait Table {
    /// Returns the table name associated with this type.
    fn table_name() -> &'static str;
}

pub use macros_derive::Table;

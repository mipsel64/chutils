use macros::Table;

#[derive(Table)]
struct User;

#[derive(Table)]
struct UserProfile;

#[derive(Table)]
struct HTTPServer;

#[derive(Table)]
#[table(name = "my_custom_events")]
struct Event;

#[derive(Table)]
struct Class {
    #[allow(dead_code)]
    id: u64,
}

#[derive(Table)]
struct Generic<T> {
    #[allow(dead_code)]
    value: T,
}

#[test]
fn default_single_word() {
    assert_eq!(<User as Table>::table_name(), "users");
}

#[test]
fn default_camel_case() {
    assert_eq!(<UserProfile as Table>::table_name(), "user_profiles");
}

#[test]
fn default_acronym_prefix() {
    assert_eq!(<HTTPServer as Table>::table_name(), "http_servers");
}

#[test]
fn default_with_fields() {
    // `Class` ends with `s` so it should be pluralized with `es`.
    assert_eq!(<Class as Table>::table_name(), "classes");
}

#[test]
fn custom_name_overrides_default() {
    assert_eq!(<Event as Table>::table_name(), "my_custom_events");
}

#[test]
fn works_with_generics() {
    assert_eq!(<Generic<u32> as Table>::table_name(), "generics");
    assert_eq!(<Generic<String> as Table>::table_name(), "generics");
}

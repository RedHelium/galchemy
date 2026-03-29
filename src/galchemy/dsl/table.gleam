import galchemy/ast/schema.{type Table}
import gleam/option

// Creates a table reference without an alias.
pub fn table(name: String) -> Table {
  schema.Table(schema: option.None, name: name, alias: option.None)
}

// Sets the schema name for a table reference.
pub fn in_schema(table: Table, schema_name: String) -> Table {
  schema.Table(..table, schema: option.Some(schema_name))
}

// Replaces the alias value for a table reference.
pub fn as_(table: Table, alias: String) -> Table {
  schema.Table(..table, alias: option.Some(alias))
}

// Creates raw column metadata for the given table and column name.
pub fn column(table: Table, name: String) -> schema.ColumnMeta {
  schema.ColumnMeta(table: table, name: name)
}

// Creates a typed integer column reference.
pub fn int(table: Table, name: String) -> schema.Column(Int) {
  schema.Column(meta: schema.ColumnMeta(table: table, name: name))
}

// Creates a typed text column reference.
pub fn text(table: Table, name: String) -> schema.Column(String) {
  schema.Column(meta: schema.ColumnMeta(table: table, name: name))
}

// Creates a typed boolean column reference.
pub fn bool(table: Table, name: String) -> schema.Column(Bool) {
  schema.Column(meta: schema.ColumnMeta(table: table, name: name))
}

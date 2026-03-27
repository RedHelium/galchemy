import gleam/option.{type Option}

/// Represents a database table reference with an optional SQL alias.
pub type Table {
  Table(name: String, alias: Option(String))
}

/// Identifies a concrete column within a specific table reference.
pub type ColumnMeta {
  ColumnMeta(table: Table, name: String)
}

/// Wraps column metadata with a phantom type for typed query construction.
pub type Column(a) {
  Column(meta: ColumnMeta)
}

import galchemy/ast/schema.{type Table}
import gleam/option

pub fn table(name: String) -> Table {
  schema.Table(name: name, alias: option.None)
}

pub fn set_alias(table: Table, alias: option.Option(String)) -> Table {
  schema.Table(..table, alias: alias)
}

pub fn column(table: Table, name: String) -> schema.ColumnMeta {
  schema.ColumnMeta(table: table, name: name)
}

pub fn int(table: Table, name: String) -> schema.Column(Int) {
  schema.Column(meta: schema.ColumnMeta(table: table, name: name))
}

pub fn text(table: Table, name: String) -> schema.Column(String) {
  schema.Column(meta: schema.ColumnMeta(table: table, name: name))
}

pub fn bool(table: Table, name: String) -> schema.Column(Bool) {
  schema.Column(meta: schema.ColumnMeta(table: table, name: name))
}

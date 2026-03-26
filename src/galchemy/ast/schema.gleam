import gleam/option.{type Option}

pub type Table {
  Table(name: String, alias: Option(String))
}

pub type ColumnMeta {
  ColumnMeta(table: Table, name: String)
}

pub type Column(a) {
  Column(meta: ColumnMeta)
}

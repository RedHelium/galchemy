import gleam/option.{type Option}

pub type SchemaSnapshot {
  SchemaSnapshot(tables: List(TableSchema))
}

pub type TableSchema {
  TableSchema(
    schema: String,
    name: String,
    columns: List(ColumnSchema),
    primary_key: Option(PrimaryKey),
    unique_constraints: List(UniqueConstraint),
    foreign_keys: List(ForeignKey),
    indexes: List(IndexSchema),
  )
}

pub type ColumnSchema {
  ColumnSchema(
    name: String,
    data_type: ColumnType,
    nullable: Bool,
    default: Option(String),
    ordinal_position: Int,
  )
}

pub type ColumnType {
  SmallIntType
  IntegerType
  BigIntType
  BooleanType
  TextType
  VarCharType(length: Option(Int))
  TimestampType(with_time_zone: Bool)
  TimeType(with_time_zone: Bool)
  DateType
  RealType
  DoublePrecisionType
  NumericType(precision: Option(Int), scale: Option(Int))
  JsonType
  JsonbType
  UuidType
  ByteaType
  ArrayType(item_type: ColumnType)
  CustomType(name: String)
}

pub type PrimaryKey {
  PrimaryKey(name: String, columns: List(String))
}

pub type UniqueConstraint {
  UniqueConstraint(name: String, columns: List(String))
}

pub type ForeignKey {
  ForeignKey(
    name: String,
    columns: List(String),
    referenced_schema: String,
    referenced_table: String,
    referenced_columns: List(String),
  )
}

pub type IndexSchema {
  IndexSchema(name: String, unique: Bool, definition: String)
}

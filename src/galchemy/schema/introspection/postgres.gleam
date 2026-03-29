import galchemy/ast/expression
import galchemy/schema/model
import galchemy/sql/compiler
import galchemy/sql/postgres
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import pog

pub type IntrospectionOptions {
  IntrospectionOptions(schemas: List(String), include_system_schemas: Bool)
}

pub type ColumnRow {
  ColumnRow(
    schema_name: String,
    table_name: String,
    column_name: String,
    ordinal_position: Int,
    is_nullable: Bool,
    data_type: String,
    udt_name: String,
    character_maximum_length: Option(Int),
    numeric_precision: Option(Int),
    numeric_scale: Option(Int),
    datetime_precision: Option(Int),
    column_default: Option(String),
  )
}

pub type ConstraintKind {
  PrimaryKeyConstraint
  UniqueConstraint
  ForeignKeyConstraint
}

pub type ConstraintRow {
  ConstraintRow(
    schema_name: String,
    table_name: String,
    constraint_name: String,
    kind: ConstraintKind,
    column_name: String,
    ordinal_position: Int,
    referenced_schema: Option(String),
    referenced_table: Option(String),
    referenced_column: Option(String),
  )
}

pub type IndexRow {
  IndexRow(
    schema_name: String,
    table_name: String,
    index_name: String,
    is_unique: Bool,
    definition: String,
  )
}

pub type IntrospectionError {
  ColumnsQueryError(pog.QueryError)
  ConstraintsQueryError(pog.QueryError)
  IndexesQueryError(pog.QueryError)
}

pub fn default_options() -> IntrospectionOptions {
  IntrospectionOptions(schemas: [], include_system_schemas: False)
}

pub fn only_schemas(
  options: IntrospectionOptions,
  schemas: List(String),
) -> IntrospectionOptions {
  IntrospectionOptions(..options, schemas: schemas)
}

pub fn include_system_schemas(
  options: IntrospectionOptions,
) -> IntrospectionOptions {
  IntrospectionOptions(..options, include_system_schemas: True)
}

pub fn compile_query(options: IntrospectionOptions) -> compiler.CompiledQuery {
  compile_columns_query(options)
}

pub fn compile_columns_query(
  options: IntrospectionOptions,
) -> compiler.CompiledQuery {
  let IntrospectionOptions(
    schemas: schemas,
    include_system_schemas: include_system_schemas,
  ) = options

  let base_sql =
    "SELECT c.table_schema, c.table_name, c.column_name, c.ordinal_position, "
    <> "(c.is_nullable = 'YES') AS is_nullable, "
    <> "c.data_type, c.udt_name, c.character_maximum_length, "
    <> "c.numeric_precision, c.numeric_scale, c.datetime_precision, c.column_default "
    <> "FROM information_schema.columns AS c "
    <> "INNER JOIN information_schema.tables AS t "
    <> "ON t.table_schema = c.table_schema "
    <> "AND t.table_name = c.table_name "
    <> "WHERE t.table_type = 'BASE TABLE'"

  let system_filter_sql =
    compile_system_filter("c.table_schema", include_system_schemas)
  let #(schema_filter_sql, params) =
    compile_schema_filter("c.table_schema", schemas)

  compiler.CompiledQuery(
    sql: base_sql
      <> system_filter_sql
      <> schema_filter_sql
      <> " ORDER BY c.table_schema, c.table_name, c.ordinal_position",
    params: params,
  )
}

pub fn compile_constraints_query(
  options: IntrospectionOptions,
) -> compiler.CompiledQuery {
  let IntrospectionOptions(
    schemas: schemas,
    include_system_schemas: include_system_schemas,
  ) = options

  let base_sql =
    "SELECT tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type, "
    <> "kcu.column_name, kcu.ordinal_position, "
    <> "ccu.table_schema AS referenced_schema, "
    <> "ccu.table_name AS referenced_table, "
    <> "ccu.column_name AS referenced_column "
    <> "FROM information_schema.table_constraints AS tc "
    <> "INNER JOIN information_schema.key_column_usage AS kcu "
    <> "ON tc.constraint_schema = kcu.constraint_schema "
    <> "AND tc.constraint_name = kcu.constraint_name "
    <> "AND tc.table_schema = kcu.table_schema "
    <> "AND tc.table_name = kcu.table_name "
    <> "LEFT JOIN information_schema.referential_constraints AS rc "
    <> "ON tc.constraint_schema = rc.constraint_schema "
    <> "AND tc.constraint_name = rc.constraint_name "
    <> "LEFT JOIN information_schema.key_column_usage AS ccu "
    <> "ON rc.unique_constraint_schema = ccu.constraint_schema "
    <> "AND rc.unique_constraint_name = ccu.constraint_name "
    <> "AND kcu.position_in_unique_constraint = ccu.ordinal_position "
    <> "WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')"

  let system_filter_sql =
    compile_system_filter("tc.table_schema", include_system_schemas)
  let #(schema_filter_sql, params) =
    compile_schema_filter("tc.table_schema", schemas)

  compiler.CompiledQuery(
    sql: base_sql
      <> system_filter_sql
      <> schema_filter_sql
      <> " ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position",
    params: params,
  )
}

pub fn compile_indexes_query(
  options: IntrospectionOptions,
) -> compiler.CompiledQuery {
  let IntrospectionOptions(
    schemas: schemas,
    include_system_schemas: include_system_schemas,
  ) = options

  let base_sql =
    "SELECT ns.nspname AS table_schema, tbl.relname AS table_name, "
    <> "idx.relname AS index_name, ind.indisunique AS is_unique, "
    <> "pg_get_indexdef(ind.indexrelid) AS index_definition "
    <> "FROM pg_index AS ind "
    <> "INNER JOIN pg_class AS tbl ON tbl.oid = ind.indrelid "
    <> "INNER JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace "
    <> "INNER JOIN pg_class AS idx ON idx.oid = ind.indexrelid "
    <> "WHERE tbl.relkind = 'r' AND NOT ind.indisprimary"

  let system_filter_sql =
    compile_system_filter("ns.nspname", include_system_schemas)
  let #(schema_filter_sql, params) =
    compile_schema_filter("ns.nspname", schemas)

  compiler.CompiledQuery(
    sql: base_sql
      <> system_filter_sql
      <> schema_filter_sql
      <> " ORDER BY ns.nspname, tbl.relname, idx.relname",
    params: params,
  )
}

pub fn to_query(options: IntrospectionOptions) -> pog.Query(ColumnRow) {
  to_columns_query(options)
}

pub fn to_columns_query(options: IntrospectionOptions) -> pog.Query(ColumnRow) {
  pog.returning(
    postgres.to_query_from_compiled(compile_columns_query(options)),
    column_row_decoder(),
  )
}

pub fn to_constraints_query(
  options: IntrospectionOptions,
) -> pog.Query(ConstraintRow) {
  pog.returning(
    postgres.to_query_from_compiled(compile_constraints_query(options)),
    constraint_row_decoder(),
  )
}

pub fn to_indexes_query(options: IntrospectionOptions) -> pog.Query(IndexRow) {
  pog.returning(
    postgres.to_query_from_compiled(compile_indexes_query(options)),
    index_row_decoder(),
  )
}

pub fn introspect(
  connection: pog.Connection,
  options: IntrospectionOptions,
) -> Result(model.SchemaSnapshot, IntrospectionError) {
  case execute_columns_query(connection, options) {
    Ok(columns) -> {
      case execute_constraints_query(connection, options) {
        Ok(constraints) -> {
          case execute_indexes_query(connection, options) {
            Ok(indexes) -> Ok(rows_to_snapshot(columns, constraints, indexes))
            Error(error) -> Error(error)
          }
        }
        Error(error) -> Error(error)
      }
    }
    Error(error) -> Error(error)
  }
}

pub fn rows_to_snapshot(
  columns: List(ColumnRow),
  constraints: List(ConstraintRow),
  indexes: List(IndexRow),
) -> model.SchemaSnapshot {
  let base_tables = column_rows_to_tables(columns, [])
  let tables_with_constraints = apply_constraint_rows(base_tables, constraints)
  let tables_with_indexes = apply_index_rows(tables_with_constraints, indexes)
  model.SchemaSnapshot(tables: tables_with_indexes)
}

pub fn column_row_decoder() -> decode.Decoder(ColumnRow) {
  decode.at([0], decode.string)
  |> decode.then(fn(schema_name) {
    decode.at([1], decode.string)
    |> decode.then(fn(table_name) {
      decode.at([2], decode.string)
      |> decode.then(fn(column_name) {
        decode.at([3], decode.int)
        |> decode.then(fn(ordinal_position) {
          decode.at([4], decode.bool)
          |> decode.then(fn(is_nullable) {
            decode.at([5], decode.string)
            |> decode.then(fn(data_type) {
              decode.at([6], decode.string)
              |> decode.then(fn(udt_name) {
                decode.at([7], decode.optional(decode.int))
                |> decode.then(fn(character_maximum_length) {
                  decode.at([8], decode.optional(decode.int))
                  |> decode.then(fn(numeric_precision) {
                    decode.at([9], decode.optional(decode.int))
                    |> decode.then(fn(numeric_scale) {
                      decode.at([10], decode.optional(decode.int))
                      |> decode.then(fn(datetime_precision) {
                        decode.at([11], decode.optional(decode.string))
                        |> decode.map(fn(column_default) {
                          ColumnRow(
                            schema_name: schema_name,
                            table_name: table_name,
                            column_name: column_name,
                            ordinal_position: ordinal_position,
                            is_nullable: is_nullable,
                            data_type: data_type,
                            udt_name: udt_name,
                            character_maximum_length: character_maximum_length,
                            numeric_precision: numeric_precision,
                            numeric_scale: numeric_scale,
                            datetime_precision: datetime_precision,
                            column_default: column_default,
                          )
                        })
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  })
}

pub fn constraint_row_decoder() -> decode.Decoder(ConstraintRow) {
  decode.at([0], decode.string)
  |> decode.then(fn(schema_name) {
    decode.at([1], decode.string)
    |> decode.then(fn(table_name) {
      decode.at([2], decode.string)
      |> decode.then(fn(constraint_name) {
        decode.at([3], decode.string)
        |> decode.then(fn(kind_name) {
          decode.at([4], decode.string)
          |> decode.then(fn(column_name) {
            decode.at([5], decode.int)
            |> decode.then(fn(ordinal_position) {
              decode.at([6], decode.optional(decode.string))
              |> decode.then(fn(referenced_schema) {
                decode.at([7], decode.optional(decode.string))
                |> decode.then(fn(referenced_table) {
                  decode.at([8], decode.optional(decode.string))
                  |> decode.map(fn(referenced_column) {
                    ConstraintRow(
                      schema_name: schema_name,
                      table_name: table_name,
                      constraint_name: constraint_name,
                      kind: constraint_kind_from_string(kind_name),
                      column_name: column_name,
                      ordinal_position: ordinal_position,
                      referenced_schema: referenced_schema,
                      referenced_table: referenced_table,
                      referenced_column: referenced_column,
                    )
                  })
                })
              })
            })
          })
        })
      })
    })
  })
}

pub fn index_row_decoder() -> decode.Decoder(IndexRow) {
  decode.at([0], decode.string)
  |> decode.then(fn(schema_name) {
    decode.at([1], decode.string)
    |> decode.then(fn(table_name) {
      decode.at([2], decode.string)
      |> decode.then(fn(index_name) {
        decode.at([3], decode.bool)
        |> decode.then(fn(is_unique) {
          decode.at([4], decode.string)
          |> decode.map(fn(definition) {
            IndexRow(
              schema_name: schema_name,
              table_name: table_name,
              index_name: index_name,
              is_unique: is_unique,
              definition: definition,
            )
          })
        })
      })
    })
  })
}

fn execute_columns_query(
  connection: pog.Connection,
  options: IntrospectionOptions,
) -> Result(List(ColumnRow), IntrospectionError) {
  case pog.execute(to_columns_query(options), on: connection) {
    Ok(pog.Returned(count: _, rows: rows)) -> Ok(rows)
    Error(error) -> Error(ColumnsQueryError(error))
  }
}

fn execute_constraints_query(
  connection: pog.Connection,
  options: IntrospectionOptions,
) -> Result(List(ConstraintRow), IntrospectionError) {
  case pog.execute(to_constraints_query(options), on: connection) {
    Ok(pog.Returned(count: _, rows: rows)) -> Ok(rows)
    Error(error) -> Error(ConstraintsQueryError(error))
  }
}

fn execute_indexes_query(
  connection: pog.Connection,
  options: IntrospectionOptions,
) -> Result(List(IndexRow), IntrospectionError) {
  case pog.execute(to_indexes_query(options), on: connection) {
    Ok(pog.Returned(count: _, rows: rows)) -> Ok(rows)
    Error(error) -> Error(IndexesQueryError(error))
  }
}

fn compile_system_filter(
  qualifier: String,
  include_system_schemas: Bool,
) -> String {
  case include_system_schemas {
    True -> ""
    False ->
      " AND " <> qualifier <> " NOT IN ('pg_catalog', 'information_schema')"
  }
}

fn compile_schema_filter(
  qualifier: String,
  schemas: List(String),
) -> #(String, List(expression.SqlValue)) {
  case schemas {
    [] -> #("", [])
    _ -> {
      let placeholders = schema_placeholders(schemas, 1, [])
      let params = list.map(schemas, expression.Text)

      #(
        " AND "
          <> qualifier
          <> " IN ("
          <> join_strings(placeholders, ", ")
          <> ")",
        params,
      )
    }
  }
}

fn schema_placeholders(
  schemas: List(String),
  next_index: Int,
  acc: List(String),
) -> List(String) {
  case schemas {
    [] -> reverse(acc)
    [_, ..rest] ->
      schema_placeholders(rest, next_index + 1, [
        "$" <> int.to_string(next_index),
        ..acc
      ])
  }
}

fn column_rows_to_tables(
  rows: List(ColumnRow),
  acc: List(model.TableSchema),
) -> List(model.TableSchema) {
  case rows {
    [] -> reverse(acc)
    [first, ..rest] -> {
      let #(table_schema, remaining_rows) =
        collect_table_rows(rest, first.schema_name, first.table_name, [first])
      column_rows_to_tables(remaining_rows, [table_schema, ..acc])
    }
  }
}

fn collect_table_rows(
  rows: List(ColumnRow),
  schema_name: String,
  table_name: String,
  acc: List(ColumnRow),
) -> #(model.TableSchema, List(ColumnRow)) {
  case rows {
    [] -> #(
      empty_table(
        schema_name,
        table_name,
        column_rows_to_columns(reverse(acc), []),
      ),
      [],
    )

    [row, ..rest] -> {
      case row.schema_name == schema_name && row.table_name == table_name {
        True -> collect_table_rows(rest, schema_name, table_name, [row, ..acc])
        False -> #(
          empty_table(
            schema_name,
            table_name,
            column_rows_to_columns(reverse(acc), []),
          ),
          rows,
        )
      }
    }
  }
}

fn column_rows_to_columns(
  rows: List(ColumnRow),
  acc: List(model.ColumnSchema),
) -> List(model.ColumnSchema) {
  case rows {
    [] -> reverse(acc)
    [row, ..rest] ->
      column_rows_to_columns(rest, [
        model.ColumnSchema(
          name: row.column_name,
          data_type: infer_column_type(row),
          nullable: row.is_nullable,
          default: row.column_default,
          ordinal_position: row.ordinal_position,
        ),
        ..acc
      ])
  }
}

fn apply_constraint_rows(
  tables: List(model.TableSchema),
  rows: List(ConstraintRow),
) -> List(model.TableSchema) {
  case rows {
    [] -> tables
    [first, ..rest] -> {
      let #(same_constraint_rows, remaining_rows) =
        collect_constraint_rows(
          rest,
          first.schema_name,
          first.table_name,
          first.constraint_name,
          [first],
        )

      let next_tables =
        apply_constraint_group(tables, reverse(same_constraint_rows))

      apply_constraint_rows(next_tables, remaining_rows)
    }
  }
}

fn collect_constraint_rows(
  rows: List(ConstraintRow),
  schema_name: String,
  table_name: String,
  constraint_name: String,
  acc: List(ConstraintRow),
) -> #(List(ConstraintRow), List(ConstraintRow)) {
  case rows {
    [] -> #(acc, [])
    [row, ..rest] -> {
      case
        row.schema_name == schema_name
        && row.table_name == table_name
        && row.constraint_name == constraint_name
      {
        True ->
          collect_constraint_rows(
            rest,
            schema_name,
            table_name,
            constraint_name,
            [row, ..acc],
          )
        False -> #(acc, rows)
      }
    }
  }
}

fn apply_constraint_group(
  tables: List(model.TableSchema),
  rows: List(ConstraintRow),
) -> List(model.TableSchema) {
  case rows {
    [] -> tables
    [first, ..] -> {
      let updater = case first.kind {
        PrimaryKeyConstraint -> add_primary_key(rows)
        UniqueConstraint -> add_unique_constraint(rows)
        ForeignKeyConstraint -> add_foreign_key(rows)
      }

      upsert_table(tables, first.schema_name, first.table_name, updater)
    }
  }
}

fn add_primary_key(
  rows: List(ConstraintRow),
) -> fn(model.TableSchema) -> model.TableSchema {
  fn(table_schema) {
    case rows {
      [] -> table_schema
      [first, ..] ->
        model.TableSchema(
          ..table_schema,
          primary_key: Some(model.PrimaryKey(
            name: first.constraint_name,
            columns: constraint_columns(rows, []),
          )),
        )
    }
  }
}

fn add_unique_constraint(
  rows: List(ConstraintRow),
) -> fn(model.TableSchema) -> model.TableSchema {
  fn(table_schema) {
    case rows {
      [] -> table_schema
      [first, ..] ->
        model.TableSchema(
          ..table_schema,
          unique_constraints: list.append(table_schema.unique_constraints, [
            model.UniqueConstraint(
              name: first.constraint_name,
              columns: constraint_columns(rows, []),
            ),
          ]),
        )
    }
  }
}

fn add_foreign_key(
  rows: List(ConstraintRow),
) -> fn(model.TableSchema) -> model.TableSchema {
  fn(table_schema) {
    case rows {
      [] -> table_schema
      [first, ..] -> {
        let referenced_schema =
          option_or_panic(
            first.referenced_schema,
            "Foreign key referenced schema is missing",
          )
        let referenced_table =
          option_or_panic(
            first.referenced_table,
            "Foreign key referenced table is missing",
          )

        model.TableSchema(
          ..table_schema,
          foreign_keys: list.append(table_schema.foreign_keys, [
            model.ForeignKey(
              name: first.constraint_name,
              columns: constraint_columns(rows, []),
              referenced_schema: referenced_schema,
              referenced_table: referenced_table,
              referenced_columns: foreign_key_columns(rows, []),
            ),
          ]),
        )
      }
    }
  }
}

fn apply_index_rows(
  tables: List(model.TableSchema),
  rows: List(IndexRow),
) -> List(model.TableSchema) {
  case rows {
    [] -> tables
    [row, ..rest] -> {
      let next_tables =
        upsert_table(tables, row.schema_name, row.table_name, fn(table_schema) {
          model.TableSchema(
            ..table_schema,
            indexes: list.append(table_schema.indexes, [
              model.IndexSchema(
                name: row.index_name,
                unique: row.is_unique,
                definition: row.definition,
              ),
            ]),
          )
        })

      apply_index_rows(next_tables, rest)
    }
  }
}

fn constraint_columns(
  rows: List(ConstraintRow),
  acc: List(String),
) -> List(String) {
  case rows {
    [] -> reverse(acc)
    [row, ..rest] -> constraint_columns(rest, [row.column_name, ..acc])
  }
}

fn foreign_key_columns(
  rows: List(ConstraintRow),
  acc: List(String),
) -> List(String) {
  case rows {
    [] -> reverse(acc)
    [row, ..rest] -> {
      let referenced_column =
        option_or_panic(
          row.referenced_column,
          "Foreign key referenced column is missing",
        )
      foreign_key_columns(rest, [referenced_column, ..acc])
    }
  }
}

fn upsert_table(
  tables: List(model.TableSchema),
  schema_name: String,
  table_name: String,
  updater: fn(model.TableSchema) -> model.TableSchema,
) -> List(model.TableSchema) {
  case tables {
    [] -> [updater(empty_table(schema_name, table_name, []))]
    [table_schema, ..rest] -> {
      case
        table_schema.schema == schema_name && table_schema.name == table_name
      {
        True -> [updater(table_schema), ..rest]
        False -> [
          table_schema,
          ..upsert_table(rest, schema_name, table_name, updater)
        ]
      }
    }
  }
}

fn empty_table(
  schema_name: String,
  table_name: String,
  columns: List(model.ColumnSchema),
) -> model.TableSchema {
  model.TableSchema(
    schema: schema_name,
    name: table_name,
    columns: columns,
    primary_key: None,
    unique_constraints: [],
    foreign_keys: [],
    indexes: [],
  )
}

fn infer_column_type(row: ColumnRow) -> model.ColumnType {
  case row.data_type {
    "smallint" -> model.SmallIntType
    "integer" -> model.IntegerType
    "bigint" -> model.BigIntType
    "boolean" -> model.BooleanType
    "text" -> model.TextType
    "character varying" ->
      model.VarCharType(length: row.character_maximum_length)
    "timestamp without time zone" -> model.TimestampType(with_time_zone: False)
    "timestamp with time zone" -> model.TimestampType(with_time_zone: True)
    "time without time zone" -> model.TimeType(with_time_zone: False)
    "time with time zone" -> model.TimeType(with_time_zone: True)
    "date" -> model.DateType
    "real" -> model.RealType
    "double precision" -> model.DoublePrecisionType
    "numeric" ->
      model.NumericType(
        precision: row.numeric_precision,
        scale: row.numeric_scale,
      )
    "json" -> model.JsonType
    "jsonb" -> model.JsonbType
    "uuid" -> model.UuidType
    "bytea" -> model.ByteaType
    "ARRAY" -> model.ArrayType(item_type: infer_array_item_type(row.udt_name))
    _ -> model.CustomType(name: row.udt_name)
  }
}

fn infer_array_item_type(udt_name: String) -> model.ColumnType {
  case udt_name {
    "_int2" -> model.SmallIntType
    "_int4" -> model.IntegerType
    "_int8" -> model.BigIntType
    "_bool" -> model.BooleanType
    "_text" -> model.TextType
    "_varchar" -> model.VarCharType(length: None)
    "_timestamp" -> model.TimestampType(with_time_zone: False)
    "_timestamptz" -> model.TimestampType(with_time_zone: True)
    "_time" -> model.TimeType(with_time_zone: False)
    "_timetz" -> model.TimeType(with_time_zone: True)
    "_date" -> model.DateType
    "_float4" -> model.RealType
    "_float8" -> model.DoublePrecisionType
    "_numeric" -> model.NumericType(precision: None, scale: None)
    "_json" -> model.JsonType
    "_jsonb" -> model.JsonbType
    "_uuid" -> model.UuidType
    "_bytea" -> model.ByteaType
    _ -> model.CustomType(name: udt_name)
  }
}

fn constraint_kind_from_string(value: String) -> ConstraintKind {
  case value {
    "PRIMARY KEY" -> PrimaryKeyConstraint
    "UNIQUE" -> UniqueConstraint
    "FOREIGN KEY" -> ForeignKeyConstraint
    _ -> {
      let message = "Unsupported constraint kind: " <> value
      panic as message
    }
  }
}

fn option_or_panic(value: Option(a), message: String) -> a {
  case value {
    Some(inner) -> inner
    None -> panic as message
  }
}

fn join_strings(parts: List(String), sep: String) -> String {
  case parts {
    [] -> ""
    [first, ..rest] -> join_strings_loop(rest, first, sep)
  }
}

fn join_strings_loop(parts: List(String), acc: String, sep: String) -> String {
  case parts {
    [] -> acc
    [part, ..rest] -> join_strings_loop(rest, acc <> sep <> part, sep)
  }
}

fn reverse(items: List(a)) -> List(a) {
  reverse_loop(items, [])
}

fn reverse_loop(items: List(a), acc: List(a)) -> List(a) {
  case items {
    [] -> acc
    [item, ..rest] -> reverse_loop(rest, [item, ..acc])
  }
}

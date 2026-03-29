import galchemy/schema/diff
import galchemy/schema/model
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub type DdlCompileError {
  EmptyCreateTableColumns(diff.TableRef)
  EmptyPrimaryKeyColumns(String)
  EmptyUniqueConstraintColumns(String)
  EmptyForeignKeyColumns(String)
  MismatchedForeignKeyColumns(String)
}

pub fn compile(
  operations: List(diff.SchemaOperation),
) -> Result(List(String), DdlCompileError) {
  compile_loop(operations, [])
}

pub fn compile_operation(
  operation: diff.SchemaOperation,
) -> Result(List(String), DdlCompileError) {
  case operation {
    diff.CreateTable(table) -> compile_create_table(table)
    diff.DropTable(ref) -> Ok(["DROP TABLE " <> compile_table_ref(ref)])
    diff.AddColumn(table: ref, column: column) ->
      Ok([
        "ALTER TABLE "
        <> compile_table_ref(ref)
        <> " ADD COLUMN "
        <> compile_column_definition(column),
      ])
    diff.DropColumn(table: ref, column_name: column_name) ->
      Ok([
        "ALTER TABLE "
        <> compile_table_ref(ref)
        <> " DROP COLUMN "
        <> compile_identifier(column_name),
      ])
    diff.AlterColumn(
      table: ref,
      column_name: column_name,
      current: current,
      target: target,
    ) -> compile_alter_column(ref, column_name, current, target)
    diff.AddPrimaryKey(table: ref, primary_key: primary_key) ->
      compile_add_primary_key(ref, primary_key)
    diff.DropPrimaryKey(table: ref, primary_key_name: primary_key_name) ->
      Ok([
        "ALTER TABLE "
        <> compile_table_ref(ref)
        <> " DROP CONSTRAINT "
        <> compile_identifier(primary_key_name),
      ])
    diff.AddUniqueConstraint(table: ref, constraint: constraint) ->
      compile_add_unique_constraint(ref, constraint)
    diff.DropUniqueConstraint(table: ref, constraint_name: constraint_name) ->
      Ok([
        "ALTER TABLE "
        <> compile_table_ref(ref)
        <> " DROP CONSTRAINT "
        <> compile_identifier(constraint_name),
      ])
    diff.AddForeignKey(table: ref, foreign_key: foreign_key) ->
      compile_add_foreign_key(ref, foreign_key)
    diff.DropForeignKey(table: ref, foreign_key_name: foreign_key_name) ->
      Ok([
        "ALTER TABLE "
        <> compile_table_ref(ref)
        <> " DROP CONSTRAINT "
        <> compile_identifier(foreign_key_name),
      ])
    diff.AddIndex(table: _, index: index) -> Ok([index.definition])
    diff.DropIndex(table: ref, index_name: index_name) ->
      Ok([
        "DROP INDEX "
        <> compile_identifier(ref.schema)
        <> "."
        <> compile_identifier(index_name),
      ])
  }
}

fn compile_loop(
  operations: List(diff.SchemaOperation),
  acc: List(String),
) -> Result(List(String), DdlCompileError) {
  case operations {
    [] -> Ok(reverse(acc))
    [operation, ..rest] -> {
      case compile_operation(operation) {
        Ok(statements) -> compile_loop(rest, reverse_append(statements, acc))
        Error(error) -> Error(error)
      }
    }
  }
}

fn compile_create_table(
  table: model.TableSchema,
) -> Result(List(String), DdlCompileError) {
  let ref = diff.TableRef(schema: table.schema, name: table.name)

  case table.columns {
    [] -> Error(EmptyCreateTableColumns(ref))
    _ -> {
      use primary_key_sql <- result_try(compile_create_table_primary_key(
        table.primary_key,
      ))
      use unique_constraints_sql <- result_try(
        compile_create_table_unique_constraints(table.unique_constraints),
      )
      use foreign_keys_sql <- result_try(compile_create_table_foreign_keys(
        table.foreign_keys,
      ))

      let column_sql = list.map(table.columns, compile_column_definition)
      let parts =
        list.flatten([
          column_sql,
          primary_key_sql,
          unique_constraints_sql,
          foreign_keys_sql,
        ])

      let create_table_sql =
        "CREATE TABLE "
        <> compile_table_ref(ref)
        <> " ("
        <> string.join(parts, with: ", ")
        <> ")"

      Ok([create_table_sql, ..table_index_definitions(table.indexes, [])])
    }
  }
}

fn compile_create_table_primary_key(
  primary_key: Option(model.PrimaryKey),
) -> Result(List(String), DdlCompileError) {
  case primary_key {
    None -> Ok([])
    Some(primary_key) -> {
      use clause <- result_try(compile_primary_key_clause(primary_key))
      Ok([clause])
    }
  }
}

fn compile_create_table_unique_constraints(
  constraints: List(model.UniqueConstraint),
) -> Result(List(String), DdlCompileError) {
  compile_unique_constraint_clauses(constraints, [])
}

fn compile_create_table_foreign_keys(
  foreign_keys: List(model.ForeignKey),
) -> Result(List(String), DdlCompileError) {
  compile_foreign_key_clauses(foreign_keys, [])
}

fn compile_unique_constraint_clauses(
  constraints: List(model.UniqueConstraint),
  acc: List(String),
) -> Result(List(String), DdlCompileError) {
  case constraints {
    [] -> Ok(reverse(acc))
    [constraint, ..rest] -> {
      use clause <- result_try(compile_unique_constraint_clause(constraint))
      compile_unique_constraint_clauses(rest, [clause, ..acc])
    }
  }
}

fn compile_foreign_key_clauses(
  foreign_keys: List(model.ForeignKey),
  acc: List(String),
) -> Result(List(String), DdlCompileError) {
  case foreign_keys {
    [] -> Ok(reverse(acc))
    [foreign_key, ..rest] -> {
      use clause <- result_try(compile_foreign_key_clause(foreign_key))
      compile_foreign_key_clauses(rest, [clause, ..acc])
    }
  }
}

fn compile_add_primary_key(
  ref: diff.TableRef,
  primary_key: model.PrimaryKey,
) -> Result(List(String), DdlCompileError) {
  use clause <- result_try(compile_primary_key_clause(primary_key))
  Ok(["ALTER TABLE " <> compile_table_ref(ref) <> " ADD " <> clause])
}

fn compile_add_unique_constraint(
  ref: diff.TableRef,
  constraint: model.UniqueConstraint,
) -> Result(List(String), DdlCompileError) {
  use clause <- result_try(compile_unique_constraint_clause(constraint))
  Ok(["ALTER TABLE " <> compile_table_ref(ref) <> " ADD " <> clause])
}

fn compile_add_foreign_key(
  ref: diff.TableRef,
  foreign_key: model.ForeignKey,
) -> Result(List(String), DdlCompileError) {
  use clause <- result_try(compile_foreign_key_clause(foreign_key))
  Ok(["ALTER TABLE " <> compile_table_ref(ref) <> " ADD " <> clause])
}

fn compile_alter_column(
  ref: diff.TableRef,
  column_name: String,
  current: model.ColumnSchema,
  target: model.ColumnSchema,
) -> Result(List(String), DdlCompileError) {
  let base = "ALTER TABLE " <> compile_table_ref(ref) <> " ALTER COLUMN "
  let name = compile_identifier(column_name)

  let type_statements = case current.data_type == target.data_type {
    True -> []
    False -> [base <> name <> " TYPE " <> compile_column_type(target.data_type)]
  }

  let default_statements = case current.default, target.default {
    None, None -> []
    Some(current_default), Some(target_default) -> {
      case current_default == target_default {
        True -> []
        False -> [base <> name <> " SET DEFAULT " <> target_default]
      }
    }
    None, Some(target_default) -> [
      base <> name <> " SET DEFAULT " <> target_default,
    ]
    Some(_), None -> [base <> name <> " DROP DEFAULT"]
  }

  let nullability_statements = case current.nullable == target.nullable {
    True -> []
    False ->
      case target.nullable {
        True -> [base <> name <> " DROP NOT NULL"]
        False -> [base <> name <> " SET NOT NULL"]
      }
  }

  Ok(list.append(
    list.append(type_statements, default_statements),
    nullability_statements,
  ))
}

fn compile_primary_key_clause(
  primary_key: model.PrimaryKey,
) -> Result(String, DdlCompileError) {
  case primary_key.columns {
    [] -> Error(EmptyPrimaryKeyColumns(primary_key.name))
    columns ->
      Ok(
        "CONSTRAINT "
        <> compile_identifier(primary_key.name)
        <> " PRIMARY KEY ("
        <> compile_column_name_list(columns)
        <> ")",
      )
  }
}

fn compile_unique_constraint_clause(
  constraint: model.UniqueConstraint,
) -> Result(String, DdlCompileError) {
  case constraint.columns {
    [] -> Error(EmptyUniqueConstraintColumns(constraint.name))
    columns ->
      Ok(
        "CONSTRAINT "
        <> compile_identifier(constraint.name)
        <> " UNIQUE ("
        <> compile_column_name_list(columns)
        <> ")",
      )
  }
}

fn compile_foreign_key_clause(
  foreign_key: model.ForeignKey,
) -> Result(String, DdlCompileError) {
  case foreign_key.columns, foreign_key.referenced_columns {
    [], _ -> Error(EmptyForeignKeyColumns(foreign_key.name))
    _, [] -> Error(EmptyForeignKeyColumns(foreign_key.name))
    left, right -> {
      case list.length(left) == list.length(right) {
        False -> Error(MismatchedForeignKeyColumns(foreign_key.name))
        True ->
          Ok(
            "CONSTRAINT "
            <> compile_identifier(foreign_key.name)
            <> " FOREIGN KEY ("
            <> compile_column_name_list(foreign_key.columns)
            <> ") REFERENCES "
            <> compile_identifier(foreign_key.referenced_schema)
            <> "."
            <> compile_identifier(foreign_key.referenced_table)
            <> " ("
            <> compile_column_name_list(foreign_key.referenced_columns)
            <> ")",
          )
      }
    }
  }
}

fn table_index_definitions(
  indexes: List(model.IndexSchema),
  acc: List(String),
) -> List(String) {
  case indexes {
    [] -> reverse(acc)
    [index, ..rest] -> table_index_definitions(rest, [index.definition, ..acc])
  }
}

fn compile_column_definition(column: model.ColumnSchema) -> String {
  let default_sql = case column.default {
    None -> ""
    Some(default) -> " DEFAULT " <> default
  }

  let nullability_sql = case column.nullable {
    True -> ""
    False -> " NOT NULL"
  }

  compile_identifier(column.name)
  <> " "
  <> compile_column_type(column.data_type)
  <> default_sql
  <> nullability_sql
}

fn compile_column_type(column_type: model.ColumnType) -> String {
  case column_type {
    model.SmallIntType -> "SMALLINT"
    model.IntegerType -> "INTEGER"
    model.BigIntType -> "BIGINT"
    model.BooleanType -> "BOOLEAN"
    model.TextType -> "TEXT"
    model.VarCharType(length) ->
      case length {
        Some(length) -> "VARCHAR(" <> int_to_string(length) <> ")"
        None -> "VARCHAR"
      }
    model.TimestampType(with_time_zone) ->
      case with_time_zone {
        True -> "TIMESTAMP WITH TIME ZONE"
        False -> "TIMESTAMP WITHOUT TIME ZONE"
      }
    model.TimeType(with_time_zone) ->
      case with_time_zone {
        True -> "TIME WITH TIME ZONE"
        False -> "TIME WITHOUT TIME ZONE"
      }
    model.DateType -> "DATE"
    model.RealType -> "REAL"
    model.DoublePrecisionType -> "DOUBLE PRECISION"
    model.NumericType(precision, scale) ->
      compile_numeric_type(precision, scale)
    model.JsonType -> "JSON"
    model.JsonbType -> "JSONB"
    model.UuidType -> "UUID"
    model.ByteaType -> "BYTEA"
    model.ArrayType(item_type) -> compile_column_type(item_type) <> "[]"
    model.CustomType(name) -> compile_custom_type_name(name)
  }
}

fn compile_numeric_type(precision: Option(Int), scale: Option(Int)) -> String {
  case precision, scale {
    Some(precision), Some(scale) ->
      "NUMERIC("
      <> int_to_string(precision)
      <> ", "
      <> int_to_string(scale)
      <> ")"
    Some(precision), None -> "NUMERIC(" <> int_to_string(precision) <> ")"
    None, _ -> "NUMERIC"
  }
}

fn compile_custom_type_name(name: String) -> String {
  string.split(name, on: ".")
  |> list.map(compile_identifier)
  |> string.join(with: ".")
}

fn compile_column_name_list(columns: List(String)) -> String {
  columns
  |> list.map(compile_identifier)
  |> string.join(with: ", ")
}

fn compile_table_ref(ref: diff.TableRef) -> String {
  compile_identifier(ref.schema) <> "." <> compile_identifier(ref.name)
}

fn compile_identifier(identifier: String) -> String {
  "\"" <> string.replace(in: identifier, each: "\"", with: "\"\"") <> "\""
}

fn int_to_string(value: Int) -> String {
  int.to_string(value)
}

fn result_try(result: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case result {
    Ok(value) -> next(value)
    Error(error) -> Error(error)
  }
}

fn reverse_append(items: List(a), acc: List(a)) -> List(a) {
  case items {
    [] -> acc
    [item, ..rest] -> reverse_append(rest, [item, ..acc])
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

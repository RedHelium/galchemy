import galchemy/schema/introspection/postgres as introspection
import gleam/io
import gleam/option
import gleam/string

pub fn main() -> Nil {
  let options =
    introspection.default_options()
    |> introspection.only_schemas(["public"])

  io.println("Schema introspection example")
  io.println("Columns query:")
  io.println(string.inspect(introspection.compile_columns_query(options)))
  io.println("Constraints query:")
  io.println(string.inspect(introspection.compile_constraints_query(options)))
  io.println("Indexes query:")
  io.println(string.inspect(introspection.compile_indexes_query(options)))
  io.println("Snapshot from sampled rows:")
  io.println(
    string.inspect(introspection.rows_to_snapshot(
      sample_columns(),
      sample_constraints(),
      sample_indexes(),
    )),
  )
}

fn sample_columns() -> List(introspection.ColumnRow) {
  [
    introspection.ColumnRow(
      schema_name: "public",
      table_name: "users",
      column_name: "id",
      ordinal_position: 1,
      is_nullable: False,
      data_type: "integer",
      udt_name: "int4",
      character_maximum_length: option.None,
      numeric_precision: option.None,
      numeric_scale: option.None,
      datetime_precision: option.None,
      column_default: option.Some("nextval('users_id_seq'::regclass)"),
    ),
    introspection.ColumnRow(
      schema_name: "public",
      table_name: "users",
      column_name: "email",
      ordinal_position: 2,
      is_nullable: False,
      data_type: "text",
      udt_name: "text",
      character_maximum_length: option.None,
      numeric_precision: option.None,
      numeric_scale: option.None,
      datetime_precision: option.None,
      column_default: option.None,
    ),
  ]
}

fn sample_constraints() -> List(introspection.ConstraintRow) {
  [
    introspection.ConstraintRow(
      schema_name: "public",
      table_name: "users",
      constraint_name: "users_pkey",
      kind: introspection.PrimaryKeyConstraint,
      column_name: "id",
      ordinal_position: 1,
      referenced_schema: option.None,
      referenced_table: option.None,
      referenced_column: option.None,
    ),
    introspection.ConstraintRow(
      schema_name: "public",
      table_name: "users",
      constraint_name: "users_email_key",
      kind: introspection.UniqueConstraint,
      column_name: "email",
      ordinal_position: 1,
      referenced_schema: option.None,
      referenced_table: option.None,
      referenced_column: option.None,
    ),
  ]
}

fn sample_indexes() -> List(introspection.IndexRow) {
  [
    introspection.IndexRow(
      schema_name: "public",
      table_name: "users",
      index_name: "users_email_idx",
      is_unique: True,
      definition: "CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email)",
    ),
  ]
}

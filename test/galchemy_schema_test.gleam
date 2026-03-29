import galchemy/ast/expression as ast_expression
import galchemy/schema/introspection/postgres as schema_postgres
import galchemy/schema/model
import galchemy/sql/compiler
import gleam/option
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn compile_schema_columns_query_test() {
  let compiler.CompiledQuery(sql: sql, params: params) =
    schema_postgres.compile_columns_query(schema_postgres.default_options())

  assert sql
    == "SELECT c.table_schema, c.table_name, c.column_name, c.ordinal_position, (c.is_nullable = 'YES') AS is_nullable, c.data_type, c.udt_name, c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.datetime_precision, c.column_default FROM information_schema.columns AS c INNER JOIN information_schema.tables AS t ON t.table_schema = c.table_schema AND t.table_name = c.table_name WHERE t.table_type = 'BASE TABLE' AND c.table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY c.table_schema, c.table_name, c.ordinal_position"
  assert params == []
}

pub fn compile_schema_constraints_query_test() {
  let compiler.CompiledQuery(sql: sql, params: params) =
    schema_postgres.default_options()
    |> schema_postgres.only_schemas(["public"])
    |> schema_postgres.compile_constraints_query

  assert sql
    == "SELECT tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position, ccu.table_schema AS referenced_schema, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column FROM information_schema.table_constraints AS tc INNER JOIN information_schema.key_column_usage AS kcu ON tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name LEFT JOIN information_schema.referential_constraints AS rc ON tc.constraint_schema = rc.constraint_schema AND tc.constraint_name = rc.constraint_name LEFT JOIN information_schema.key_column_usage AS ccu ON rc.unique_constraint_schema = ccu.constraint_schema AND rc.unique_constraint_name = ccu.constraint_name AND kcu.position_in_unique_constraint = ccu.ordinal_position WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY') AND tc.table_schema NOT IN ('pg_catalog', 'information_schema') AND tc.table_schema IN ($1) ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position"
  assert params == [ast_expression.Text("public")]
}

pub fn compile_schema_indexes_query_test() {
  let compiler.CompiledQuery(sql: sql, params: params) =
    schema_postgres.default_options()
    |> schema_postgres.only_schemas(["public", "analytics"])
    |> schema_postgres.compile_indexes_query

  assert sql
    == "SELECT ns.nspname AS table_schema, tbl.relname AS table_name, idx.relname AS index_name, ind.indisunique AS is_unique, pg_get_indexdef(ind.indexrelid) AS index_definition FROM pg_index AS ind INNER JOIN pg_class AS tbl ON tbl.oid = ind.indrelid INNER JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace INNER JOIN pg_class AS idx ON idx.oid = ind.indexrelid WHERE tbl.relkind = 'r' AND NOT ind.indisprimary AND ns.nspname NOT IN ('pg_catalog', 'information_schema') AND ns.nspname IN ($1, $2) ORDER BY ns.nspname, tbl.relname, idx.relname"
  assert params
    == [ast_expression.Text("public"), ast_expression.Text("analytics")]
}

pub fn rows_to_snapshot_test() {
  let columns = [
    schema_postgres.ColumnRow(
      schema_name: "public",
      table_name: "users",
      column_name: "id",
      ordinal_position: 1,
      is_nullable: False,
      data_type: "integer",
      udt_name: "int4",
      character_maximum_length: option.None,
      numeric_precision: option.Some(32),
      numeric_scale: option.Some(0),
      datetime_precision: option.None,
      column_default: option.Some("nextval('users_id_seq'::regclass)"),
    ),
    schema_postgres.ColumnRow(
      schema_name: "public",
      table_name: "users",
      column_name: "email",
      ordinal_position: 2,
      is_nullable: False,
      data_type: "character varying",
      udt_name: "varchar",
      character_maximum_length: option.Some(255),
      numeric_precision: option.None,
      numeric_scale: option.None,
      datetime_precision: option.None,
      column_default: option.None,
    ),
    schema_postgres.ColumnRow(
      schema_name: "public",
      table_name: "company_identities",
      column_name: "user_id",
      ordinal_position: 1,
      is_nullable: False,
      data_type: "integer",
      udt_name: "int4",
      character_maximum_length: option.None,
      numeric_precision: option.Some(32),
      numeric_scale: option.Some(0),
      datetime_precision: option.None,
      column_default: option.None,
    ),
    schema_postgres.ColumnRow(
      schema_name: "public",
      table_name: "company_identities",
      column_name: "company_id",
      ordinal_position: 2,
      is_nullable: False,
      data_type: "integer",
      udt_name: "int4",
      character_maximum_length: option.None,
      numeric_precision: option.Some(32),
      numeric_scale: option.Some(0),
      datetime_precision: option.None,
      column_default: option.None,
    ),
  ]

  let constraints = [
    schema_postgres.ConstraintRow(
      schema_name: "public",
      table_name: "users",
      constraint_name: "users_pkey",
      kind: schema_postgres.PrimaryKeyConstraint,
      column_name: "id",
      ordinal_position: 1,
      referenced_schema: option.None,
      referenced_table: option.None,
      referenced_column: option.None,
    ),
    schema_postgres.ConstraintRow(
      schema_name: "public",
      table_name: "users",
      constraint_name: "users_email_key",
      kind: schema_postgres.UniqueConstraint,
      column_name: "email",
      ordinal_position: 1,
      referenced_schema: option.None,
      referenced_table: option.None,
      referenced_column: option.None,
    ),
    schema_postgres.ConstraintRow(
      schema_name: "public",
      table_name: "company_identities",
      constraint_name: "company_identities_user_company_key",
      kind: schema_postgres.UniqueConstraint,
      column_name: "user_id",
      ordinal_position: 1,
      referenced_schema: option.None,
      referenced_table: option.None,
      referenced_column: option.None,
    ),
    schema_postgres.ConstraintRow(
      schema_name: "public",
      table_name: "company_identities",
      constraint_name: "company_identities_user_company_key",
      kind: schema_postgres.UniqueConstraint,
      column_name: "company_id",
      ordinal_position: 2,
      referenced_schema: option.None,
      referenced_table: option.None,
      referenced_column: option.None,
    ),
    schema_postgres.ConstraintRow(
      schema_name: "public",
      table_name: "company_identities",
      constraint_name: "company_identities_user_id_fkey",
      kind: schema_postgres.ForeignKeyConstraint,
      column_name: "user_id",
      ordinal_position: 1,
      referenced_schema: option.Some("public"),
      referenced_table: option.Some("users"),
      referenced_column: option.Some("id"),
    ),
  ]

  let indexes = [
    schema_postgres.IndexRow(
      schema_name: "public",
      table_name: "users",
      index_name: "users_email_idx",
      is_unique: False,
      definition: "CREATE INDEX users_email_idx ON public.users USING btree (email)",
    ),
  ]

  assert schema_postgres.rows_to_snapshot(columns, constraints, indexes)
    == model.SchemaSnapshot(tables: [
      model.TableSchema(
        schema: "public",
        name: "users",
        columns: [
          model.ColumnSchema(
            name: "id",
            data_type: model.IntegerType,
            nullable: False,
            default: option.Some("nextval('users_id_seq'::regclass)"),
            ordinal_position: 1,
          ),
          model.ColumnSchema(
            name: "email",
            data_type: model.VarCharType(length: option.Some(255)),
            nullable: False,
            default: option.None,
            ordinal_position: 2,
          ),
        ],
        primary_key: option.Some(
          model.PrimaryKey(name: "users_pkey", columns: ["id"]),
        ),
        unique_constraints: [
          model.UniqueConstraint(name: "users_email_key", columns: ["email"]),
        ],
        foreign_keys: [],
        indexes: [
          model.IndexSchema(
            name: "users_email_idx",
            unique: False,
            definition: "CREATE INDEX users_email_idx ON public.users USING btree (email)",
          ),
        ],
      ),
      model.TableSchema(
        schema: "public",
        name: "company_identities",
        columns: [
          model.ColumnSchema(
            name: "user_id",
            data_type: model.IntegerType,
            nullable: False,
            default: option.None,
            ordinal_position: 1,
          ),
          model.ColumnSchema(
            name: "company_id",
            data_type: model.IntegerType,
            nullable: False,
            default: option.None,
            ordinal_position: 2,
          ),
        ],
        primary_key: option.None,
        unique_constraints: [
          model.UniqueConstraint(
            name: "company_identities_user_company_key",
            columns: ["user_id", "company_id"],
          ),
        ],
        foreign_keys: [
          model.ForeignKey(
            name: "company_identities_user_id_fkey",
            columns: ["user_id"],
            referenced_schema: "public",
            referenced_table: "users",
            referenced_columns: ["id"],
          ),
        ],
        indexes: [],
      ),
    ])
}

import galchemy/schema/diff
import galchemy/schema/migration/postgres as migration
import galchemy/schema/model
import gleam/option
import gleeunit
import pog

@external(erlang, "galchemy_test_support", "query_sql")
fn query_sql(query: pog.Query(a)) -> String

@external(erlang, "galchemy_test_support", "query_parameters")
fn query_parameters(query: pog.Query(a)) -> List(pog.Value)

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn ensure_history_table_query_test() {
  let query = migration.ensure_history_table_query()

  assert query_sql(query)
    == "CREATE TABLE IF NOT EXISTS \"public\".\"galchemy_schema_migrations\" (\"name\" TEXT PRIMARY KEY, \"applied_at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), \"statement_count\" INTEGER NOT NULL)"
  assert query_parameters(query) == []
}

pub fn history_query_test() {
  let query = migration.history_query()

  assert query_sql(query)
    == "SELECT \"name\", \"applied_at\"::text, \"statement_count\" FROM \"public\".\"galchemy_schema_migrations\" ORDER BY \"applied_at\", \"name\""
  assert query_parameters(query) == []
}

pub fn record_migration_query_test() {
  let plan =
    migration.MigrationPlan(
      name: "20260329_create_users",
      operations: [],
      statements: ["CREATE TABLE ...", "CREATE INDEX ..."],
    )

  let query = migration.record_migration_query(plan)

  assert query_sql(query)
    == "INSERT INTO \"public\".\"galchemy_schema_migrations\" (\"name\", \"statement_count\") VALUES ($1, $2)"
  assert query_parameters(query)
    == [pog.text("20260329_create_users"), pog.int(2)]
}

pub fn migration_plan_test() {
  let current = model.SchemaSnapshot(tables: [])
  let target =
    model.SchemaSnapshot(tables: [
      model.TableSchema(
        schema: "public",
        name: "users",
        columns: [column("id", model.IntegerType, False, option.None, 1)],
        primary_key: option.None,
        unique_constraints: [],
        foreign_keys: [],
        indexes: [],
      ),
    ])

  assert migration.plan("20260329_create_users", current, target)
    == Ok(
      migration.MigrationPlan(
        name: "20260329_create_users",
        operations: [
          diff.CreateTable(
            model.TableSchema(
              schema: "public",
              name: "users",
              columns: [column("id", model.IntegerType, False, option.None, 1)],
              primary_key: option.None,
              unique_constraints: [],
              foreign_keys: [],
              indexes: [],
            ),
          ),
        ],
        statements: [
          "CREATE TABLE \"public\".\"users\" (\"id\" INTEGER NOT NULL)",
        ],
      ),
    )
}

pub fn migration_statuses_test() {
  let pending_plan =
    migration.MigrationPlan(
      name: "20260329_add_accounts",
      operations: [],
      statements: ["CREATE TABLE ..."],
    )
  let applied_plan =
    migration.MigrationPlan(
      name: "20260329_create_users",
      operations: [],
      statements: ["CREATE TABLE ..."],
    )

  let applied = [
    migration.AppliedMigration(
      name: "20260329_create_users",
      applied_at: "2026-03-29 18:00:00+00",
      statement_count: 1,
    ),
  ]

  assert migration.statuses([pending_plan, applied_plan], applied)
    == [
      migration.Pending(pending_plan),
      migration.Applied(
        plan: applied_plan,
        record: migration.AppliedMigration(
          name: "20260329_create_users",
          applied_at: "2026-03-29 18:00:00+00",
          statement_count: 1,
        ),
      ),
    ]
}

fn column(
  name: String,
  data_type: model.ColumnType,
  nullable: Bool,
  default: option.Option(String),
  ordinal_position: Int,
) -> model.ColumnSchema {
  model.ColumnSchema(name:, data_type:, nullable:, default:, ordinal_position:)
}

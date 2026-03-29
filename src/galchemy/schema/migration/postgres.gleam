import galchemy/schema/ddl/postgres as ddl
import galchemy/schema/diff
import galchemy/schema/model
import gleam/dynamic/decode
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import pog

const history_table_schema = "public"
const history_table_name = "galchemy_schema_migrations"

pub type MigrationPlan {
  MigrationPlan(
    name: String,
    operations: List(diff.SchemaOperation),
    statements: List(String),
  )
}

pub type AppliedMigration {
  AppliedMigration(name: String, applied_at: String, statement_count: Int)
}

pub type MigrationStatus {
  Pending(MigrationPlan)
  Applied(plan: MigrationPlan, record: AppliedMigration)
}

pub type ApplyError {
  AlreadyApplied(String)
  HistoryQueryError(pog.QueryError)
  StatementError(statement: String, error: pog.QueryError)
  RecordError(pog.QueryError)
}

pub fn plan(
  name: String,
  current: model.SchemaSnapshot,
  target: model.SchemaSnapshot,
) -> Result(MigrationPlan, ddl.DdlCompileError) {
  let operations = diff.diff(current, target)
  use statements <- result.try(ddl.compile(operations))
  Ok(MigrationPlan(name: name, operations: operations, statements: statements))
}

pub fn ensure_history_table_query() -> pog.Query(Nil) {
  pog.query(
    "CREATE TABLE IF NOT EXISTS "
    <> compile_history_table_ref()
    <> " ("
    <> "\"name\" TEXT PRIMARY KEY, "
    <> "\"applied_at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), "
    <> "\"statement_count\" INTEGER NOT NULL"
    <> ")",
  )
}

pub fn history_query() -> pog.Query(AppliedMigration) {
  pog.query(
    "SELECT \"name\", \"applied_at\"::text, \"statement_count\" "
    <> "FROM "
    <> compile_history_table_ref()
    <> " ORDER BY \"applied_at\", \"name\"",
  )
  |> pog.returning(applied_migration_decoder())
}

pub fn applied_migration_query(name: String) -> pog.Query(AppliedMigration) {
  pog.query(
    "SELECT \"name\", \"applied_at\"::text, \"statement_count\" "
    <> "FROM "
    <> compile_history_table_ref()
    <> " WHERE \"name\" = $1",
  )
  |> pog.parameter(pog.text(name))
  |> pog.returning(applied_migration_decoder())
}

pub fn record_migration_query(plan: MigrationPlan) -> pog.Query(Nil) {
  pog.query(
    "INSERT INTO "
    <> compile_history_table_ref()
    <> " (\"name\", \"statement_count\") VALUES ($1, $2)",
  )
  |> pog.parameter(pog.text(plan.name))
  |> pog.parameter(pog.int(list.length(plan.statements)))
}

pub fn statuses(
  plans: List(MigrationPlan),
  applied: List(AppliedMigration),
) -> List(MigrationStatus) {
  statuses_loop(plans, applied, [])
}

pub fn apply(
  connection: pog.Connection,
  plan: MigrationPlan,
) -> Result(Nil, pog.TransactionError(ApplyError)) {
  pog.transaction(connection, fn(tx_connection) {
    case
      execute_unit_query(
        ensure_history_table_query(),
        tx_connection,
        HistoryQueryError,
      )
    {
      Error(error) -> Error(error)
      Ok(_) -> {
        case fetch_applied_migration(tx_connection, plan.name) {
          Error(error) -> Error(error)
          Ok(option.Some(_)) -> Error(AlreadyApplied(plan.name))
          Ok(option.None) -> {
            case apply_statements(plan.statements, tx_connection) {
              Error(error) -> Error(error)
              Ok(_) ->
                execute_unit_query(
                  record_migration_query(plan),
                  tx_connection,
                  RecordError,
                )
            }
          }
        }
      }
    }
  })
}

fn statuses_loop(
  plans: List(MigrationPlan),
  applied: List(AppliedMigration),
  acc: List(MigrationStatus),
) -> List(MigrationStatus) {
  case plans {
    [] -> reverse(acc)
    [plan, ..rest] -> {
      let next_status = case find_applied(applied, plan.name) {
        option.Some(record) -> Applied(plan: plan, record: record)
        option.None -> Pending(plan)
      }

      statuses_loop(rest, applied, [next_status, ..acc])
    }
  }
}

fn fetch_applied_migration(
  connection: pog.Connection,
  name: String,
) -> Result(option.Option(AppliedMigration), ApplyError) {
  case pog.execute(applied_migration_query(name), on: connection) {
    Ok(pog.Returned(count: 0, rows: _)) -> Ok(option.None)
    Ok(pog.Returned(count: _, rows: [])) -> Ok(option.None)
    Ok(pog.Returned(count: _, rows: [first, .._])) -> Ok(option.Some(first))
    Error(error) -> Error(HistoryQueryError(error))
  }
}

fn apply_statements(
  statements: List(String),
  connection: pog.Connection,
) -> Result(Nil, ApplyError) {
  case statements {
    [] -> Ok(Nil)
    [statement, ..rest] -> {
      case
        execute_unit_query(
          pog.query(statement),
          connection,
          fn(error) { StatementError(statement: statement, error: error) },
        )
      {
        Error(error) -> Error(error)
        Ok(_) -> apply_statements(rest, connection)
      }
    }
  }
}

fn execute_unit_query(
  query: pog.Query(t),
  connection: pog.Connection,
  map_error: fn(pog.QueryError) -> error,
) -> Result(Nil, error) {
  case pog.execute(query, on: connection) {
    Ok(_) -> Ok(Nil)
    Error(error) -> Error(map_error(error))
  }
}

fn find_applied(
  applied: List(AppliedMigration),
  name: String,
) -> option.Option(AppliedMigration) {
  case applied {
    [] -> option.None
    [migration, ..rest] -> {
      case migration.name == name {
        True -> option.Some(migration)
        False -> find_applied(rest, name)
      }
    }
  }
}

fn applied_migration_decoder() -> decode.Decoder(AppliedMigration) {
  decode.at([0], decode.string)
  |> decode.then(fn(name) {
    decode.at([1], decode.string)
    |> decode.then(fn(applied_at) {
      decode.at([2], decode.int)
      |> decode.map(fn(statement_count) {
        AppliedMigration(
          name: name,
          applied_at: applied_at,
          statement_count: statement_count,
        )
      })
    })
  })
}

fn compile_history_table_ref() -> String {
  compile_identifier(history_table_schema)
  <> "."
  <> compile_identifier(history_table_name)
}

fn compile_identifier(identifier: String) -> String {
  "\""
  <> string.replace(in: identifier, each: "\"", with: "\"\"")
  <> "\""
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

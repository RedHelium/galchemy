import galchemy
import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/orm/entity
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/sql/compiler
import galchemy/session/execution
import galchemy/session/runtime
import galchemy/session/transaction
import galchemy/session/unit_of_work
import gleam/list
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn transaction_stage_and_flush_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot)
  let new_user =
    entity.new_(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity

  let #(flush_result, next_transaction) =
    transaction.begin("tx-1", runtime.new(snapshot))
    |> transaction.stage(new_user)
    |> expect_transaction
    |> transaction.flush(fake_executor)
    |> expect_flushed

  assert list.length(execution.queries(flush_result)) == 1
  assert transaction.connection(next_transaction) == "tx-1"
  assert runtime.tracked_entities(transaction.session(next_transaction))
    == [entity.mark_clean(new_user)]
}

pub fn transaction_commit_returns_runtime_session_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot)
  let clean_user =
    entity.materialize(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity
  let dirty_user =
    clean_user
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Annie"))])
    |> expect_entity

  let #(flush_result, committed_session) =
    transaction.begin("tx-2", runtime.new(snapshot))
    |> transaction.track(clean_user)
    |> expect_transaction
    |> transaction.stage(dirty_user)
    |> expect_transaction
    |> transaction.commit(fake_executor)
    |> expect_committed
  let committed =
    case runtime.get(
      committed_session,
      relation.table_ref("public", "users"),
      expect_identity(clean_user),
    ) {
      option.Some(value) -> value
      option.None -> panic as "expected committed entity"
    }

  assert list.length(execution.queries(flush_result)) == 1
  assert entity.status(committed) == entity.Clean
}

pub fn transaction_rollback_preserves_connection_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot)
  let new_user =
    entity.new_(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity

  let rolled_back =
    transaction.begin("tx-3", runtime.new(snapshot))
    |> transaction.stage(new_user)
    |> expect_transaction
    |> transaction.rollback

  assert transaction.connection(rolled_back) == "tx-3"
  assert runtime.tracked_entities(transaction.session(rolled_back)) == []
}

pub fn transaction_executor_error_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot)
  let new_user =
    entity.new_(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity

  assert transaction.begin("tx-4", runtime.new(snapshot))
    |> transaction.stage(new_user)
    |> expect_transaction
    |> transaction.flush(failing_executor)
    == Error(
      transaction.ExecutionError(runtime.ExecutionError(execution.QueryError("boom"))),
    )
}

fn fake_executor(
  next_query: query.Query,
  _connection: String,
) -> Result(compiler.CompiledQuery, compiler.CompileError) {
  galchemy.compile(next_query)
}

fn failing_executor(
  _next_query: query.Query,
  _connection: String,
) -> Result(compiler.CompiledQuery, String) {
  Error("boom")
}

fn expect_metadata(snapshot: model.SchemaSnapshot) -> metadata.ModelMetadata {
  case metadata.from_snapshot(snapshot, "public", "users") {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_entity(result: Result(entity.Entity, entity.EntityError)) -> entity.Entity {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_transaction(
  result: Result(
    transaction.TransactionSession(String),
    transaction.TransactionError(error),
  ),
) -> transaction.TransactionSession(String) {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_flushed(
  result: Result(
    #(execution.FlushExecution(compiler.CompiledQuery), transaction.TransactionSession(String)),
    transaction.TransactionError(compiler.CompileError),
  ),
) -> #(execution.FlushExecution(compiler.CompiledQuery), transaction.TransactionSession(String)) {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_committed(
  result: Result(
    #(execution.FlushExecution(compiler.CompiledQuery), runtime.Session),
    transaction.TransactionError(compiler.CompileError),
  ),
) -> #(execution.FlushExecution(compiler.CompiledQuery), runtime.Session) {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_identity(next_entity: entity.Entity) -> unit_of_work.Identity {
  case entity.identity(next_entity) {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn blog_snapshot() -> model.SchemaSnapshot {
  model.SchemaSnapshot(tables: [
    model.TableSchema(
      schema: "public",
      name: "users",
      columns: [
        column("id", model.IntegerType, False, option.None, 1),
        column("name", model.TextType, False, option.None, 2),
      ],
      primary_key: option.Some(
        model.PrimaryKey(name: "users_pkey", columns: ["id"]),
      ),
      unique_constraints: [],
      foreign_keys: [],
      indexes: [],
    ),
  ])
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

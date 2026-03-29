import galchemy
import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/orm/entity
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/execution
import galchemy/session/runtime
import galchemy/session/unit_of_work
import galchemy/sql/compiler
import gleam/list
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn track_adds_clean_entity_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity

  let session =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime

  let identity = expect_identity(clean_user)

  assert runtime.get(session, relation.table_ref("public", "users"), identity)
    == option.Some(clean_user)
  assert tracked_pending_queries(session) == []
}

pub fn attach_adds_clean_entity_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity

  let session =
    runtime.new(snapshot)
    |> runtime.attach(clean_user)
    |> expect_runtime

  assert runtime.tracked_entities(session) == [clean_user]
}

pub fn rollback_restores_persisted_baseline_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let dirty_user =
    clean_user
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Annie"))])
    |> expect_entity

  let staged_session =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage(dirty_user)
    |> expect_runtime
  let rolled_back = runtime.rollback(staged_session)
  let identity = expect_identity(clean_user)

  assert runtime.get(
      rolled_back,
      relation.table_ref("public", "users"),
      identity,
    )
    == option.Some(clean_user)
  assert tracked_pending_queries(rolled_back) == []
}

pub fn rollback_drops_new_entities_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let new_user =
    entity.new_(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity

  let staged_session =
    runtime.new(snapshot)
    |> runtime.stage(new_user)
    |> expect_runtime
  let rolled_back = runtime.rollback(staged_session)

  assert runtime.tracked_entities(rolled_back) == []
}

pub fn flush_normalizes_statuses_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let dirty_user =
    clean_user
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Annie"))])
    |> expect_entity

  let #(flush_result, flushed_session) =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage(dirty_user)
    |> expect_runtime
    |> runtime.flush(galchemy.compile)
    |> expect_flushed
  let identity = expect_identity(dirty_user)
  let flushed_user = case
    runtime.get(
      flushed_session,
      relation.table_ref("public", "users"),
      identity,
    )
  {
    option.Some(value) -> value
    option.None -> panic as "expected tracked entity"
  }

  assert list.length(execution.queries(flush_result)) == 1
  assert entity.status(flushed_user) == entity.Clean
  assert tracked_pending_queries(flushed_session) == []
}

pub fn commit_removes_deleted_entities_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let deleted_user = entity.mark_deleted(clean_user)

  let #(_, committed_session) =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage(deleted_user)
    |> expect_runtime
    |> runtime.commit(galchemy.compile)
    |> expect_flushed
  let identity = expect_identity(clean_user)

  assert runtime.get(
      committed_session,
      relation.table_ref("public", "users"),
      identity,
    )
    == option.None
}

pub fn detach_removes_entity_and_pending_changes_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let dirty_user =
    clean_user
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Annie"))])
    |> expect_entity

  let detached =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage(dirty_user)
    |> expect_runtime
    |> runtime.detach(dirty_user)
    |> expect_runtime

  assert runtime.tracked_entities(detached) == []
  assert tracked_pending_queries(detached) == []
}

pub fn refresh_restores_persisted_entity_and_clears_pending_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let dirty_user =
    clean_user
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Annie"))])
    |> expect_entity

  let refreshed =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage(dirty_user)
    |> expect_runtime
    |> runtime.refresh(dirty_user)
    |> expect_runtime

  assert runtime.tracked_entities(refreshed) == [clean_user]
  assert tracked_pending_queries(refreshed) == []
}

pub fn refresh_unknown_entity_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity

  assert runtime.refresh(runtime.new(snapshot), clean_user)
    == Error(runtime.UnknownTrackedEntity(
      relation.table_ref("public", "users"),
      expect_identity(clean_user),
    ))
}

fn tracked_pending_queries(session: runtime.Session) -> List(query.Query) {
  case unit_of_work.flush_plan(runtime.pending_changes(session)) {
    Ok(plan) -> unit_of_work.queries(plan)
    Error(_) -> []
  }
}

fn expect_metadata(
  snapshot: model.SchemaSnapshot,
  schema_name: String,
  table_name: String,
) -> metadata.ModelMetadata {
  case metadata.from_snapshot(snapshot, schema_name, table_name) {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_entity(
  result: Result(entity.Entity, entity.EntityError),
) -> entity.Entity {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_runtime(
  result: Result(runtime.Session, runtime.TrackError),
) -> runtime.Session {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_flushed(
  result: Result(
    #(execution.FlushExecution(compiler.CompiledQuery), runtime.Session),
    runtime.SessionExecutionError(compiler.CompileError),
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

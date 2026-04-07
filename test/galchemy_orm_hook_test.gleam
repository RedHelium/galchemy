import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/graph
import galchemy/orm/hook
import galchemy/orm/identity_map
import galchemy/orm/mapper_registry
import galchemy/orm/materializer
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/session/runtime
import galchemy/session/unit_of_work
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn materialize_with_hooks_applies_after_load_test() {
  let registry = expect_registry(blog_snapshot())
  let #(loaded_entity, _) =
    materializer.new(registry)
    |> materializer.materialize_with_hooks(
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
        unit_of_work.field("audit", ast_expression.Text("raw")),
      ]),
      audit_hooks("loaded"),
    )
    |> expect_materialized_with_hooks

  assert field_value(loaded_entity, "audit")
    == option.Some(ast_expression.Text("loaded"))
  assert entity.status(loaded_entity) == entity.Clean
}

pub fn materialize_with_hooks_propagates_hook_error_test() {
  let registry = expect_registry(blog_snapshot())

  assert materializer.materialize_with_hooks(
      materializer.new(registry),
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
        unit_of_work.field("audit", ast_expression.Text("raw")),
      ]),
      failing_hooks("after_load failed"),
    )
    == Error(materializer.HookError("after_load failed"))
}

pub fn stage_with_hooks_applies_before_insert_update_delete_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
      unit_of_work.field("audit", ast_expression.Text("clean")),
    ])
    |> expect_entity
  let new_user =
    entity.new_(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(2)),
      unit_of_work.field("name", ast_expression.Text("Bob")),
      unit_of_work.field("audit", ast_expression.Text("raw")),
    ])
    |> expect_entity
  let dirty_user =
    clean_user
    |> entity.change([
      unit_of_work.field("name", ast_expression.Text("Annie")),
    ])
    |> expect_entity
  let deleted_user = entity.mark_deleted(clean_user)
  let hooks = audit_hooks("hooked")

  let insert_session =
    runtime.new(snapshot)
    |> runtime.stage_with_hooks(new_user, hooks)
    |> expect_runtime_with_hooks
  let update_session =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage_with_hooks(dirty_user, hooks)
    |> expect_runtime_with_hooks
  let delete_session =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage_with_hooks(deleted_user, hooks)
    |> expect_runtime_with_hooks

  let inserted_user = first_entity(runtime.tracked_entities(insert_session))
  let updated_user = first_entity(runtime.tracked_entities(update_session))
  let removed_user = first_entity(runtime.tracked_entities(delete_session))

  assert field_value(inserted_user, "audit")
    == option.Some(ast_expression.Text("hooked"))
  assert field_value(updated_user, "audit")
    == option.Some(ast_expression.Text("hooked"))
  assert field_value(removed_user, "audit")
    == option.Some(ast_expression.Text("hooked"))
  assert entity.status(removed_user) == entity.Deleted
}

pub fn attach_refresh_and_hydrate_with_hooks_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
      unit_of_work.field("audit", ast_expression.Text("clean")),
    ])
    |> expect_entity
  let dirty_user =
    clean_user
    |> entity.change([
      unit_of_work.field("name", ast_expression.Text("Annie")),
    ])
    |> expect_entity
  let post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity
  let hooks =
    hook.EntityHooks(
      after_load: fn(next_entity) { Ok(next_entity) },
      before_insert: fn(next_entity) { Ok(next_entity) },
      before_update: fn(next_entity) { Ok(next_entity) },
      before_delete: fn(next_entity) { Ok(next_entity) },
      after_attach: fn(next_entity) {
        entity.change(next_entity, [
          unit_of_work.field("audit", ast_expression.Text("attached")),
        ])
      },
      after_refresh: fn(next_entity) {
        entity.change(next_entity, [
          unit_of_work.field("audit", ast_expression.Text("refreshed")),
        ])
      },
      after_relation_loaded: fn(next_entity, relation_name) {
        entity.change(next_entity, [
          unit_of_work.field(
            "audit",
            ast_expression.Text(relation_name <> "_loaded"),
          ),
        ])
      },
    )

  let attached_session =
    runtime.new(snapshot)
    |> runtime.attach_with_hooks(clean_user, hooks)
    |> expect_runtime_with_hooks
  let refreshed_session =
    runtime.new(snapshot)
    |> runtime.track(clean_user)
    |> expect_runtime
    |> runtime.stage(dirty_user)
    |> expect_runtime
    |> runtime.refresh_with_hooks(dirty_user, hooks)
    |> expect_runtime_with_hooks
  let hydrated =
    graph.hydrate_only_with_hooks(
      clean_user,
      ["posts"],
      seed_identity_map([clean_user, post]),
      hooks,
    )
    |> expect_hydrated_with_hooks

  assert field_value(
      first_entity(runtime.tracked_entities(attached_session)),
      "audit",
    )
    == option.Some(ast_expression.Text("attached"))
  assert field_value(
      first_entity(runtime.tracked_entities(refreshed_session)),
      "audit",
    )
    == option.Some(ast_expression.Text("refreshed"))
  assert field_value(graph.hydrated_entity(hydrated), "audit")
    == option.Some(ast_expression.Text("posts_loaded"))
}

pub fn stage_with_hooks_propagates_hook_error_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let new_user =
    entity.new_(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
      unit_of_work.field("audit", ast_expression.Text("raw")),
    ])
    |> expect_entity

  assert runtime.stage_with_hooks(
      runtime.new(snapshot),
      new_user,
      failing_hooks("before_insert failed"),
    )
    == Error(runtime.HookFailure("before_insert failed"))
}

fn audit_hooks(value: String) -> hook.EntityHooks(entity.EntityError) {
  hook.EntityHooks(
    after_load: fn(next_entity) {
      entity.change(next_entity, [
        unit_of_work.field("audit", ast_expression.Text(value)),
      ])
    },
    before_insert: fn(next_entity) {
      entity.change(next_entity, [
        unit_of_work.field("audit", ast_expression.Text(value)),
      ])
    },
    before_update: fn(next_entity) {
      entity.change(next_entity, [
        unit_of_work.field("audit", ast_expression.Text(value)),
      ])
    },
    before_delete: fn(next_entity) {
      entity.change(next_entity, [
        unit_of_work.field("audit", ast_expression.Text(value)),
      ])
    },
    after_attach: fn(next_entity) { Ok(next_entity) },
    after_refresh: fn(next_entity) { Ok(next_entity) },
    after_relation_loaded: fn(next_entity, _relation_name) { Ok(next_entity) },
  )
}

fn failing_hooks(message: String) -> hook.EntityHooks(String) {
  hook.EntityHooks(
    after_load: fn(_next_entity) { Error(message) },
    before_insert: fn(_next_entity) { Error(message) },
    before_update: fn(_next_entity) { Error(message) },
    before_delete: fn(_next_entity) { Error(message) },
    after_attach: fn(_next_entity) { Error(message) },
    after_refresh: fn(_next_entity) { Error(message) },
    after_relation_loaded: fn(_next_entity, _relation_name) { Error(message) },
  )
}

fn first_entity(entities: List(entity.Entity)) -> entity.Entity {
  case entities {
    [first, ..] -> first
    [] -> panic as "expected at least one entity"
  }
}

fn field_value(
  next_entity: entity.Entity,
  column_name: String,
) -> option.Option(ast_expression.SqlValue) {
  find_field(entity.fields(next_entity), column_name)
}

fn find_field(
  fields: List(unit_of_work.FieldValue),
  column_name: String,
) -> option.Option(ast_expression.SqlValue) {
  case fields {
    [] -> option.None
    [field_value, ..rest] -> {
      case field_value.column == column_name {
        True -> option.Some(field_value.value)
        False -> find_field(rest, column_name)
      }
    }
  }
}

fn seed_identity_map(entities: List(entity.Entity)) -> identity_map.IdentityMap {
  case entities {
    [] -> identity_map.empty()
    [next_entity, ..rest] -> {
      let next_map = case
        identity_map.insert(seed_identity_map(rest), next_entity)
      {
        Ok(value) -> value
        Error(error) -> panic as string.inspect(error)
      }
      next_map
    }
  }
}

fn expect_registry(
  snapshot: model.SchemaSnapshot,
) -> mapper_registry.MapperRegistry {
  case mapper_registry.from_snapshot(snapshot) {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
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

fn expect_materialized_with_hooks(
  result: Result(
    #(entity.Entity, materializer.Materializer),
    materializer.MaterializationHookError(entity.EntityError),
  ),
) -> #(entity.Entity, materializer.Materializer) {
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

fn expect_runtime_with_hooks(
  result: Result(runtime.Session, runtime.HookTrackError(entity.EntityError)),
) -> runtime.Session {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_hydrated_with_hooks(
  result: Result(
    graph.HydratedEntity,
    graph.HydrationHookError(entity.EntityError),
  ),
) -> graph.HydratedEntity {
  case result {
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
        column("audit", model.TextType, False, option.None, 3),
      ],
      primary_key: option.Some(
        model.PrimaryKey(name: "users_pkey", columns: ["id"]),
      ),
      unique_constraints: [],
      foreign_keys: [],
      indexes: [],
    ),
    model.TableSchema(
      schema: "public",
      name: "posts",
      columns: [
        column("id", model.IntegerType, False, option.None, 1),
        column("user_id", model.IntegerType, False, option.None, 2),
        column("title", model.TextType, False, option.None, 3),
      ],
      primary_key: option.Some(
        model.PrimaryKey(name: "posts_pkey", columns: ["id"]),
      ),
      unique_constraints: [],
      foreign_keys: [
        model.ForeignKey(
          name: "posts_user_id_fkey",
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

fn column(
  name: String,
  data_type: model.ColumnType,
  nullable: Bool,
  default: option.Option(String),
  ordinal_position: Int,
) -> model.ColumnSchema {
  model.ColumnSchema(name:, data_type:, nullable:, default:, ordinal_position:)
}

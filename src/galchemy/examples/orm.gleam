import galchemy
import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/graph
import galchemy/orm/identity_map
import galchemy/orm/mapper_registry
import galchemy/orm/materializer
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/session/execution
import galchemy/session/runtime
import galchemy/session/unit_of_work
import galchemy/sql/compiler
import gleam/io
import gleam/option
import gleam/string

pub fn main() -> Nil {
  let snapshot = blog_snapshot()
  let users_metadata = case
    metadata.from_snapshot(snapshot, "public", "users")
  {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }

  let inserted_user = case
    entity.new_(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
  {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }

  let updated_user =
    case
      entity.materialize(users_metadata, [
        unit_of_work.field("id", ast_expression.Int(2)),
        unit_of_work.field("name", ast_expression.Text("Bob")),
      ])
    {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Bobby"))])
    |> unwrap_entity
  let registry =
    blog_snapshot() |> mapper_registry.from_snapshot |> unwrap_registry
  let #(loaded_user, _) =
    materializer.new(registry)
    |> materializer.materialize(
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(3)),
        unit_of_work.field("name", ast_expression.Text("Cara")),
      ]),
    )
    |> unwrap_materialized
  let posts_metadata = case
    metadata.from_snapshot(snapshot, "public", "posts")
  {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
  let related_post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(20)),
      unit_of_work.field("user_id", ast_expression.Int(3)),
      unit_of_work.field("title", ast_expression.Text("Hydrated")),
    ])
    |> unwrap_entity
  let hydrated_user =
    graph.hydrate_only(
      loaded_user,
      ["posts"],
      seed_identity_map([loaded_user, related_post]),
    )
    |> unwrap_hydrated

  let pending_work =
    unit_of_work.new(snapshot)
    |> entity.stage(inserted_user)
    |> unwrap_pending
    |> entity.stage(updated_user)
    |> unwrap_pending

  let flush_plan = case unit_of_work.flush_plan(pending_work) {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
  let #(committed_flush, committed_session) =
    runtime.new(snapshot)
    |> runtime.track(loaded_user)
    |> unwrap_runtime
    |> runtime.stage(inserted_user)
    |> unwrap_runtime
    |> runtime.stage(updated_user)
    |> unwrap_runtime
    |> runtime.commit(galchemy.compile)
    |> unwrap_committed

  io.println("ORM example")
  io.println(string.inspect(users_metadata))
  io.println(string.inspect(loaded_user))
  io.println(string.inspect(hydrated_user))
  io.println(string.inspect(flush_plan))
  io.println(string.inspect(committed_flush))
  io.println(string.inspect(runtime.tracked_entities(committed_session)))
}

fn unwrap_entity(
  result: Result(entity.Entity, entity.EntityError),
) -> entity.Entity {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn unwrap_pending(
  result: Result(unit_of_work.Session, entity.EntityError),
) -> unit_of_work.Session {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn unwrap_registry(
  result: Result(mapper_registry.MapperRegistry, mapper_registry.RegistryError),
) -> mapper_registry.MapperRegistry {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn unwrap_materialized(
  result: Result(
    #(entity.Entity, materializer.Materializer),
    materializer.MaterializationError,
  ),
) -> #(entity.Entity, materializer.Materializer) {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn unwrap_hydrated(
  result: Result(graph.HydratedEntity, graph.HydrationError),
) -> graph.HydratedEntity {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn unwrap_runtime(
  result: Result(runtime.Session, runtime.TrackError),
) -> runtime.Session {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn unwrap_committed(
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

fn seed_identity_map(entities: List(entity.Entity)) -> identity_map.IdentityMap {
  seed_identity_map_loop(entities, identity_map.empty())
}

fn seed_identity_map_loop(
  entities: List(entity.Entity),
  acc: identity_map.IdentityMap,
) -> identity_map.IdentityMap {
  case entities {
    [] -> acc
    [next_entity, ..rest] -> {
      let next_map = case identity_map.insert(acc, next_entity) {
        Ok(value) -> value
        Error(error) -> panic as string.inspect(error)
      }

      seed_identity_map_loop(rest, next_map)
    }
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

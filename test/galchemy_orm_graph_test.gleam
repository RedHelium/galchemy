import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/graph
import galchemy/orm/identity_map
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/unit_of_work
import gleam/list
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn hydrate_belongs_to_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity
  let identities = seed_identity_map([user, post])

  let hydrated =
    graph.hydrate_only(post, ["user"], identities)
    |> expect_hydrated
  let loaded_post = graph.hydrated_entity(hydrated)

  assert entity.relation_loaded(loaded_post, "user")
  assert graph.relation_named(hydrated, "user")
    == option.Some(graph.HydratedRelation(
      name: "user",
      value: graph.ToOne(option.Some(user)),
    ))
}

pub fn hydrate_has_many_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let first_post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity
  let second_post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(11)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("World")),
    ])
    |> expect_entity
  let identities = seed_identity_map([user, first_post, second_post])

  let hydrated =
    graph.hydrate_only(user, ["posts"], identities)
    |> expect_hydrated
  let loaded_user = graph.hydrated_entity(hydrated)

  assert entity.relation_loaded(loaded_user, "posts")
  assert graph.relation_named(hydrated, "posts")
    == option.Some(graph.HydratedRelation(
      name: "posts",
      value: graph.ToMany([first_post, second_post]),
    ))
}

pub fn hydrate_missing_belongs_to_test() {
  let snapshot = blog_snapshot()
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(999)),
      unit_of_work.field("title", ast_expression.Text("Orphan")),
    ])
    |> expect_entity

  let hydrated =
    graph.hydrate_only(post, ["user"], identity_map.empty())
    |> expect_hydrated

  assert graph.relation_named(hydrated, "user")
    == option.Some(graph.HydratedRelation(
      name: "user",
      value: graph.ToOne(option.None),
    ))
}

pub fn hydrate_unknown_relation_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity

  assert graph.hydrate_only(user, ["comments"], identity_map.empty())
    == Error(graph.UnknownRelation(
      table: relation.table_ref("public", "users"),
      relation_name: "comments",
    ))
}

pub fn hydrate_many_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let first_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let second_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(2)),
      unit_of_work.field("name", ast_expression.Text("Bob")),
    ])
    |> expect_entity
  let first_post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity
  let second_post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(11)),
      unit_of_work.field("user_id", ast_expression.Int(2)),
      unit_of_work.field("title", ast_expression.Text("World")),
    ])
    |> expect_entity
  let identities =
    seed_identity_map([first_user, second_user, first_post, second_post])

  let hydrated_users =
    graph.hydrate_many([first_user, second_user], ["posts"], identities)
    |> expect_many_hydrated

  assert list.length(hydrated_users) == 2
  assert graph.relation_named(first_hydrated(hydrated_users), "posts")
    == option.Some(graph.HydratedRelation(
      name: "posts",
      value: graph.ToMany([first_post]),
    ))
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

fn first_hydrated(entities: List(graph.HydratedEntity)) -> graph.HydratedEntity {
  case entities {
    [first, ..] -> first
    [] -> panic as "expected at least one hydrated entity"
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

fn expect_hydrated(
  result: Result(graph.HydratedEntity, graph.HydrationError),
) -> graph.HydratedEntity {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_many_hydrated(
  result: Result(List(graph.HydratedEntity), graph.HydrationError),
) -> List(graph.HydratedEntity) {
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

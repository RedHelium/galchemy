import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/identity_map
import galchemy/orm/mapper_registry
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

pub fn mapper_registry_from_snapshot_test() {
  let registry =
    case mapper_registry.from_snapshot(blog_snapshot()) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  assert list.length(mapper_registry.all(registry)) == 2
  assert mapper_registry.lookup(registry, "public", "users")
    == option.Some(expect_metadata(blog_snapshot(), "public", "users"))
  assert mapper_registry.lookup(registry, "public", "posts")
    == option.Some(expect_metadata(blog_snapshot(), "public", "posts"))
}

pub fn mapper_registry_duplicate_test() {
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")

  let registry =
    case mapper_registry.empty() |> mapper_registry.register(users_metadata) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  assert mapper_registry.register(registry, users_metadata)
    == Error(mapper_registry.DuplicateMapper(relation.table_ref("public", "users")))
}

pub fn mapper_registry_unknown_mapper_test() {
  assert mapper_registry.get(mapper_registry.empty(), "public", "users")
    == Error(mapper_registry.UnknownMapper(relation.table_ref("public", "users")))
}

pub fn identity_map_insert_and_get_test() {
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")
  let users_entity =
    entity.materialize(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity

  let map =
    case identity_map.insert(identity_map.empty(), users_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let identity =
    case entity.identity(users_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  assert identity_map.get(map, relation.table_ref("public", "users"), identity)
    == option.Some(users_entity)
  assert identity_map.values_for_table(map, relation.table_ref("public", "users"))
    == [users_entity]
}

pub fn identity_map_duplicate_identity_test() {
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")
  let users_entity =
    entity.materialize(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity

  let inserted_map =
    case identity_map.insert(identity_map.empty(), users_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let identity =
    case entity.identity(users_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  assert identity_map.insert(inserted_map, users_entity)
    == Error(
      identity_map.DuplicateIdentity(
        relation.table_ref("public", "users"),
        identity,
      ),
    )
}

pub fn identity_map_upsert_test() {
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")
  let initial_entity =
    entity.materialize(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity
  let updated_entity =
    initial_entity
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Annie"))])
    |> expect_entity

  let map =
    case identity_map.insert(identity_map.empty(), initial_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let replaced_map =
    case identity_map.upsert(map, updated_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let identity =
    case entity.identity(updated_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  assert identity_map.get(replaced_map, relation.table_ref("public", "users"), identity)
    == option.Some(updated_entity)
}

pub fn identity_map_remove_test() {
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")
  let users_entity =
    entity.materialize(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity

  let map =
    case identity_map.insert(identity_map.empty(), users_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let identity =
    case entity.identity(users_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let removed_map =
    identity_map.remove(map, relation.table_ref("public", "users"), identity)

  assert identity_map.get(removed_map, relation.table_ref("public", "users"), identity)
    == option.None
  assert identity_map.values(removed_map) == []
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

fn expect_entity(result: Result(entity.Entity, entity.EntityError)) -> entity.Entity {
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

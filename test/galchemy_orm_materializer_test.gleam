import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/identity_map
import galchemy/orm/mapper_registry
import galchemy/orm/materializer
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

pub fn materialize_row_inserts_into_identity_map_test() {
  let registry = expect_registry(blog_snapshot())

  let #(next_entity, next_materializer) =
    materializer.new(registry)
    |> materializer.materialize(
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ]),
    )
    |> expect_materialized

  let identity =
    case entity.identity(next_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  assert identity_map.get(
    materializer.identity_map(next_materializer),
    relation.table_ref("public", "users"),
    identity,
  )
    == option.Some(next_entity)
}

pub fn materialize_row_reuses_existing_identity_test() {
  let registry = expect_registry(blog_snapshot())
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")
  let dirty_entity =
    entity.materialize(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity
    |> entity.change([
      unit_of_work.field("name", ast_expression.Text("Annie")),
    ])
    |> expect_entity
  let identities =
    case identity_map.insert(identity_map.empty(), dirty_entity) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }
  let seeded_materializer = materializer.with_identity_map(registry, identities)

  let #(materialized_entity, next_materializer) =
    seeded_materializer
    |> materializer.materialize(
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Fresh From Db")),
      ]),
    )
    |> expect_materialized

  assert materialized_entity == dirty_entity
  assert identity_map.values(materializer.identity_map(next_materializer)) == [dirty_entity]
}

pub fn materialize_row_unknown_mapper_test() {
  let registry = expect_registry(blog_snapshot())

  assert materializer.materialize(
    materializer.new(registry),
    materializer.row("public", "comments", [
      unit_of_work.field("id", ast_expression.Int(1)),
    ]),
  )
    == Error(
      materializer.RegistryError(
        mapper_registry.UnknownMapper(relation.table_ref("public", "comments")),
      ),
    )
}

pub fn materialize_row_invalid_fields_test() {
  let registry = expect_registry(blog_snapshot())

  assert materializer.materialize(
    materializer.new(registry),
    materializer.row("public", "users", [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("unknown", ast_expression.Text("bad")),
    ]),
  )
    == Error(
      materializer.EntityError(
        entity.UnknownColumn(
          table: relation.table_ref("public", "users"),
          column: "unknown",
        ),
      ),
    )
}

pub fn materialize_many_preserves_order_test() {
  let registry = expect_registry(blog_snapshot())

  let #(entities, next_materializer) =
    materializer.new(registry)
    |> materializer.materialize_many([
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ]),
      materializer.row("public", "posts", [
        unit_of_work.field("id", ast_expression.Int(10)),
        unit_of_work.field("user_id", ast_expression.Int(1)),
        unit_of_work.field("title", ast_expression.Text("Hello")),
      ]),
    ])
    |> expect_many_materialized

  assert list.length(entities) == 2
  assert list.length(identity_map.values(materializer.identity_map(next_materializer))) == 2
  assert entity.status(first_entity(entities)) == entity.Clean
}

fn first_entity(entities: List(entity.Entity)) -> entity.Entity {
  case entities {
    [first, ..] -> first
    [] -> panic as "expected at least one entity"
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

fn expect_entity(result: Result(entity.Entity, entity.EntityError)) -> entity.Entity {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_materialized(
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

fn expect_many_materialized(
  result: Result(
    #(List(entity.Entity), materializer.Materializer),
    materializer.MaterializationError,
  ),
) -> #(List(entity.Entity), materializer.Materializer) {
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

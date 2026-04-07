import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/identity_map
import galchemy/orm/mapper_registry
import galchemy/orm/materializer
import galchemy/orm/metadata
import galchemy/orm/result
import galchemy/schema/model
import galchemy/session/unit_of_work
import gleam/list
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn scalar_result_mapping_test() {
  let registry = expect_registry(blog_snapshot())
  let mapper =
    result.scalar_value("post_count")
    |> result.map(fn(value) {
      case value {
        ast_expression.Int(count) -> count
        _ -> panic as "expected integer scalar"
      }
    })
  let #(count, _) =
    result.one(
      mapper,
      result.row([
        result.scalar("post_count", ast_expression.Int(3)),
      ], []),
      materializer.new(registry),
    )
    |> expect_mapped

  assert count == 3
}

pub fn entity_result_mapping_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let registry = expect_registry(snapshot)
  let #(loaded_user, next_materializer) =
    result.one(
      result.entity_value(users_metadata),
      result.row([], [
        materializer.row("public", "users", [
          unit_of_work.field("id", ast_expression.Int(1)),
          unit_of_work.field("email", ast_expression.Text("ann@example.com")),
          unit_of_work.field("name", ast_expression.Text("Ann")),
        ]),
      ]),
      materializer.new(registry),
    )
    |> expect_mapped

  assert entity.status(loaded_user) == entity.Clean
  assert list.length(identity_map.values(materializer.identity_map(next_materializer)))
    == 1
}

pub fn tuple_result_mapping_reuses_identity_map_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let registry = expect_registry(snapshot)
  let mapper =
    result.tuple2(
      result.entity_value(users_metadata),
      result.scalar_value("post_count"),
    )
  let rows = [
    result.row([
      result.scalar("post_count", ast_expression.Int(2)),
    ], [
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("email", ast_expression.Text("ann@example.com")),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ]),
    ]),
    result.row([
      result.scalar("post_count", ast_expression.Int(5)),
    ], [
      materializer.row("public", "users", [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("email", ast_expression.Text("changed@example.com")),
        unit_of_work.field("name", ast_expression.Text("Changed")),
      ]),
    ]),
  ]
  let #(mapped_rows, next_materializer) =
    result.many(mapper, rows, materializer.new(registry))
    |> expect_many_mapped
  let first_entity = case mapped_rows {
    [#(next_entity, _), ..] -> next_entity
    [] -> panic as "expected mapped rows"
  }
  let second_entity = case mapped_rows {
    [_, #(next_entity, _), ..] -> next_entity
    _ -> panic as "expected two mapped rows"
  }

  assert first_entity == second_entity
  assert list.length(identity_values(next_materializer)) == 1
}

pub fn missing_scalar_and_entity_errors_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let registry = expect_registry(snapshot)
  let empty_row = result.row([], [])

  assert result.one(
      result.scalar_value("post_count"),
      empty_row,
      materializer.new(registry),
    )
    == Error(result.MissingScalar("post_count"))
  assert result.one(
      result.entity_value(users_metadata),
      empty_row,
      materializer.new(registry),
    )
    == Error(result.MissingEntity(users_metadata))
}

fn identity_values(next_materializer: materializer.Materializer) -> List(entity.Entity) {
  identity_map.values(materializer.identity_map(next_materializer))
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

fn expect_mapped(
  result_: Result(#(a, materializer.Materializer), result.MappingError),
) -> #(a, materializer.Materializer) {
  case result_ {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_many_mapped(
  result_: Result(#(List(a), materializer.Materializer), result.MappingError),
) -> #(List(a), materializer.Materializer) {
  case result_ {
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
        column("email", model.TextType, False, option.None, 2),
        column("name", model.TextType, False, option.None, 3),
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

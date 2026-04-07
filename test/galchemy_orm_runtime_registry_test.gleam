import galchemy/orm/declarative
import galchemy/orm/mapper_registry
import galchemy/orm/runtime_registry
import galchemy/schema/model
import galchemy/schema/relation
import gleam/list
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn runtime_registry_from_snapshot_test() {
  let registry =
    expect_registry(runtime_registry.from_snapshot(blog_snapshot()))

  assert list.length(runtime_registry.all(registry)) == 2
  assert runtime_registry.has_column(registry, "public", "users", "email")
    == Ok(True)
  assert runtime_registry.has_relation(registry, "public", "users", "posts")
    == Ok(True)
  assert runtime_registry.has_relation(registry, "public", "posts", "user")
    == Ok(True)

  let posts_relation =
    runtime_registry.relation_named(registry, "public", "users", "posts")
    |> expect_lookup

  assert posts_relation
    == option.Some(
      relation.has_many(
        "posts",
        "posts_user_id_fkey",
        relation.table_ref("public", "posts"),
        [relation.pair("id", "user_id")],
      ),
    )
}

pub fn runtime_registry_from_models_and_mapper_registry_test() {
  let users =
    declarative.model_(
      "public",
      "users",
      [
        declarative.primary_key(declarative.int("id")),
        declarative.text("email"),
        declarative.text("name"),
      ],
      [
        declarative.has_many("posts", "posts_user_id_fkey", "public", "posts", [
          declarative.pair("id", "user_id"),
        ]),
      ],
    )
  let posts =
    declarative.model_(
      "public",
      "posts",
      [
        declarative.primary_key(declarative.int("id")),
        declarative.int("user_id"),
        declarative.text("title"),
      ],
      [
        declarative.belongs_to("user", "posts_user_id_fkey", "public", "users", [
          declarative.pair("user_id", "id"),
        ]),
      ],
    )
  let registry = expect_registry(runtime_registry.from_models([users, posts]))
  let mapper_registry_ =
    runtime_registry.to_mapper_registry(registry)
    |> expect_mapper_registry

  assert list.length(runtime_registry.snapshot(registry).tables) == 2
  assert runtime_registry.table_schema(registry, "public", "posts")
    == Ok(
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
    )
  assert mapper_registry.get(mapper_registry_, "public", "users")
    |> result_is_ok
}

pub fn runtime_registry_duplicate_and_unknown_model_errors_test() {
  let users =
    declarative.model_(
      "public",
      "users",
      [
        declarative.primary_key(declarative.int("id")),
      ],
      [],
    )
  let registry = expect_registry(runtime_registry.from_models([users]))

  assert runtime_registry.register_model(registry, users)
    == Error(
      runtime_registry.DuplicateModel(relation.table_ref("public", "users")),
    )
  assert runtime_registry.get(registry, "public", "posts")
    == Error(
      runtime_registry.UnknownModel(relation.table_ref("public", "posts")),
    )
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

fn expect_registry(
  result: Result(
    runtime_registry.RuntimeRegistry,
    runtime_registry.RegistryError,
  ),
) -> runtime_registry.RuntimeRegistry {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_lookup(
  result: Result(
    option.Option(relation.Relation),
    runtime_registry.RegistryError,
  ),
) -> option.Option(relation.Relation) {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_mapper_registry(
  result: Result(mapper_registry.MapperRegistry, runtime_registry.RegistryError),
) -> mapper_registry.MapperRegistry {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn result_is_ok(result: Result(a, e)) -> Bool {
  case result {
    Ok(_) -> True
    Error(_) -> False
  }
}

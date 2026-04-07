import galchemy/orm/declarative
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import gleam/list
import gleam/option
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn declarative_model_to_metadata_test() {
  let next_model =
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

  let next_metadata = expect_metadata(declarative.to_metadata(next_model))

  assert next_metadata.table == relation.table_ref("public", "posts")
  assert next_metadata.identity_columns == ["id"]
  assert list.contains(next_metadata.columns, "title")
  assert metadata.has_relation(next_metadata, "user")
}

pub fn declarative_model_to_snapshot_emits_foreign_keys_test() {
  let users =
    declarative.model_(
      "public",
      "users",
      [
        declarative.primary_key(declarative.int("id")),
        declarative.unique(declarative.text("email")),
        declarative.default(
          declarative.nullable(declarative.text("nickname")),
          "'guest'",
        ),
      ],
      [],
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

  let snapshot = expect_snapshot(declarative.to_snapshot([users, posts]))
  let users_table = expect_table(snapshot, "public", "users")
  let posts_table = expect_table(snapshot, "public", "posts")

  assert users_table.primary_key
    == option.Some(model.PrimaryKey(name: "users_pkey", columns: ["id"]))
  assert users_table.unique_constraints
    == [
      model.UniqueConstraint(name: "users_email_key", columns: ["email"]),
    ]
  assert posts_table.foreign_keys
    == [
      model.ForeignKey(
        name: "posts_user_id_fkey",
        columns: ["user_id"],
        referenced_schema: "public",
        referenced_table: "users",
        referenced_columns: ["id"],
      ),
    ]
}

pub fn declarative_model_identity_override_test() {
  let next_model =
    declarative.model_(
      "public",
      "accounts",
      [
        declarative.text("email"),
        declarative.text("name"),
      ],
      [],
    )
    |> declarative.identity(["email"])

  let next_metadata = expect_metadata(declarative.to_metadata(next_model))

  assert next_metadata.identity_columns == ["email"]
}

pub fn declarative_model_validation_errors_test() {
  let duplicate_columns =
    declarative.model_(
      "public",
      "users",
      [
        declarative.int("id"),
        declarative.text("id"),
      ],
      [],
    )
  let broken_relation =
    declarative.model_(
      "public",
      "posts",
      [
        declarative.primary_key(declarative.int("id")),
        declarative.int("user_id"),
      ],
      [
        declarative.belongs_to("user", "posts_user_id_fkey", "public", "users", [
          declarative.pair("author_id", "id"),
        ]),
      ],
    )
  let missing_identity =
    declarative.model_(
      "public",
      "logs",
      [
        declarative.text("message"),
      ],
      [],
    )

  assert declarative.to_metadata(duplicate_columns)
    == Error(declarative.DuplicateColumn(
      table: relation.table_ref("public", "users"),
      column: "id",
    ))
  assert declarative.to_metadata(broken_relation)
    == Error(declarative.UnknownLocalColumn(
      table: relation.table_ref("public", "posts"),
      column: "author_id",
    ))
  assert declarative.to_metadata(missing_identity)
    == Error(
      declarative.MissingIdentity(table: relation.table_ref("public", "logs")),
    )
}

fn expect_metadata(
  result: Result(metadata.ModelMetadata, declarative.DeclarativeError),
) -> metadata.ModelMetadata {
  case result {
    Ok(value) -> value
    Error(_error) -> panic as "unexpected metadata error"
  }
}

fn expect_snapshot(
  result: Result(model.SchemaSnapshot, declarative.DeclarativeError),
) -> model.SchemaSnapshot {
  case result {
    Ok(value) -> value
    Error(_error) -> panic as "unexpected snapshot error"
  }
}

fn expect_table(
  snapshot: model.SchemaSnapshot,
  schema_name: String,
  table_name: String,
) -> model.TableSchema {
  let matches =
    list.filter(snapshot.tables, fn(table_schema) {
      table_schema.schema == schema_name && table_schema.name == table_name
    })

  case matches {
    [table_schema, ..] -> table_schema
    [] -> panic as "expected table in snapshot"
  }
}

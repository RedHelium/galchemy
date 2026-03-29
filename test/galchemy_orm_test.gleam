import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/orm/entity
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/unit_of_work
import galchemy/sql/compiler
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn metadata_from_snapshot_test() {
  assert metadata.from_snapshot(blog_snapshot(), "public", "posts")
    == Ok(
      metadata.ModelMetadata(
        table: relation.table_ref("public", "posts"),
        identity_columns: ["id"],
        columns: ["id", "user_id", "title"],
        relations: [
          relation.belongs_to(
            "user",
            "posts_user_id_fkey",
            relation.table_ref("public", "users"),
            [relation.pair("user_id", "id")],
          ),
        ],
      ),
    )
}

pub fn metadata_uses_unique_constraint_when_primary_key_missing_test() {
  assert metadata.from_snapshot(unique_only_snapshot(), "public", "accounts")
    == Ok(
      metadata.ModelMetadata(
        table: relation.table_ref("public", "accounts"),
        identity_columns: ["email"],
        columns: ["email", "name"],
        relations: [],
      ),
    )
}

pub fn metadata_missing_identity_test() {
  assert metadata.from_snapshot(no_identity_snapshot(), "public", "events")
    == Error(metadata.MissingIdentity(relation.table_ref("public", "events")))
}

pub fn entity_materialize_and_mark_loaded_test() {
  let posts_metadata = expect_metadata(blog_snapshot(), "public", "posts")

  let posts_entity =
    entity.materialize(
      posts_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(10)),
        unit_of_work.field("user_id", ast_expression.Int(1)),
        unit_of_work.field("title", ast_expression.Text("Hello")),
      ],
    )
    |> expect_entity
    |> entity.mark_relation_loaded("user")
    |> expect_entity

  assert entity.relation_loaded(posts_entity, "user")
  assert entity.status(posts_entity) == entity.Clean
}

pub fn entity_new_stages_insert_test() {
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")
  let users_entity =
    entity.new_(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    )
    |> expect_entity

  let planned =
    unit_of_work.new(blog_snapshot())
    |> entity.stage(users_entity)
    |> expect_session
    |> unit_of_work.flush_plan
    |> expect_plan
    |> unit_of_work.queries
    |> compiled_queries

  assert planned
    == [
      #(
        "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES ($1, $2)",
        [ast_expression.Int(1), ast_expression.Text("Ann")],
      ),
    ]
}

pub fn entity_change_stages_update_test() {
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
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Bob"))])
    |> expect_entity

  let planned =
    unit_of_work.new(blog_snapshot())
    |> entity.stage(users_entity)
    |> expect_session
    |> unit_of_work.flush_plan
    |> expect_plan
    |> unit_of_work.queries
    |> compiled_queries

  assert planned
    == [
      #(
        "UPDATE \"public\".\"users\" SET \"name\" = $1 WHERE (\"public\".\"users\".\"id\" = $2)",
        [ast_expression.Text("Bob"), ast_expression.Int(1)],
      ),
    ]
}

pub fn entity_delete_stages_delete_test() {
  let posts_metadata = expect_metadata(blog_snapshot(), "public", "posts")
  let posts_entity =
    entity.materialize(
      posts_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(10)),
        unit_of_work.field("user_id", ast_expression.Int(1)),
        unit_of_work.field("title", ast_expression.Text("Hello")),
      ],
    )
    |> expect_entity
    |> entity.mark_deleted

  let planned =
    unit_of_work.new(blog_snapshot())
    |> entity.stage(posts_entity)
    |> expect_session
    |> unit_of_work.flush_plan
    |> expect_plan
    |> unit_of_work.queries
    |> compiled_queries

  assert planned
    == [
      #(
        "DELETE FROM \"public\".\"posts\" WHERE (\"public\".\"posts\".\"id\" = $1)",
        [ast_expression.Int(10)],
      ),
    ]
}

pub fn entity_rejects_unknown_column_test() {
  let users_metadata = expect_metadata(blog_snapshot(), "public", "users")

  assert entity.materialize(
    users_metadata,
    [unit_of_work.field("unknown", ast_expression.Text("x"))],
  )
    == Error(
      entity.UnknownColumn(
        table: relation.table_ref("public", "users"),
        column: "unknown",
      ),
    )
}

pub fn entity_rejects_change_after_delete_test() {
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
    |> entity.mark_deleted

  assert entity.change(
    users_entity,
    [unit_of_work.field("name", ast_expression.Text("Bob"))],
  )
    == Error(entity.DeletedEntity(relation.table_ref("public", "users")))
}

fn expect_metadata(
  snapshot: model.SchemaSnapshot,
  schema_name: String,
  table_name: String,
) -> metadata.ModelMetadata {
  case metadata.from_snapshot(snapshot, schema_name, table_name) {
    Ok(value) -> value
    Error(error) -> {
      let message = "Expected metadata, got: " <> string.inspect(error)
      panic as message
    }
  }
}

fn expect_entity(result: Result(entity.Entity, entity.EntityError)) -> entity.Entity {
  case result {
    Ok(value) -> value
    Error(error) -> {
      let message = "Expected entity, got: " <> string.inspect(error)
      panic as message
    }
  }
}

fn expect_session(
  result: Result(unit_of_work.Session, entity.EntityError),
) -> unit_of_work.Session {
  case result {
    Ok(value) -> value
    Error(error) -> {
      let message = "Expected session, got: " <> string.inspect(error)
      panic as message
    }
  }
}

fn expect_plan(
  result: Result(unit_of_work.FlushPlan, unit_of_work.SessionError),
) -> unit_of_work.FlushPlan {
  case result {
    Ok(value) -> value
    Error(error) -> {
      let message = "Expected flush plan, got: " <> string.inspect(error)
      panic as message
    }
  }
}

fn compiled_queries(
  queries: List(query.Query),
) -> List(#(String, List(ast_expression.SqlValue))) {
  case queries {
    [] -> []
    [next_query, ..rest] -> {
      let compiler.CompiledQuery(sql: sql, params: params) =
        case compiler.compile(next_query) {
          Ok(compiled) -> compiled
          Error(error) -> {
            let message =
              "Expected compiler output, got: " <> string.inspect(error)
            panic as message
          }
        }

      [#(sql, params), ..compiled_queries(rest)]
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
      unique_constraints: [
        model.UniqueConstraint(
          name: "posts_id_user_id_key",
          columns: ["id", "user_id"],
        ),
      ],
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

fn unique_only_snapshot() -> model.SchemaSnapshot {
  model.SchemaSnapshot(tables: [
    model.TableSchema(
      schema: "public",
      name: "accounts",
      columns: [
        column("email", model.TextType, False, option.None, 1),
        column("name", model.TextType, False, option.None, 2),
      ],
      primary_key: option.None,
      unique_constraints: [
        model.UniqueConstraint(name: "accounts_email_key", columns: ["email"]),
      ],
      foreign_keys: [],
      indexes: [],
    ),
  ])
}

fn no_identity_snapshot() -> model.SchemaSnapshot {
  model.SchemaSnapshot(tables: [
    model.TableSchema(
      schema: "public",
      name: "events",
      columns: [column("name", model.TextType, False, option.None, 1)],
      primary_key: option.None,
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

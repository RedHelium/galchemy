import galchemy
import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/cascade
import galchemy/session/runtime
import galchemy/session/unit_of_work
import gleam/list
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn cascade_persist_has_many_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let new_user =
    entity.new_(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let new_post =
    entity.new_(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity

  let session =
    cascade.stage(
      runtime.new(snapshot),
      cascade.node(new_user, [
        cascade.related("posts", [cascade.node(new_post, [])]),
      ]),
      [cascade.persist_rule("posts")],
    )
    |> expect_session

  assert list.length(runtime.tracked_entities(session)) == 2
  assert pending_queries(session)
    == [
      "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES ($1, $2)",
      "INSERT INTO \"public\".\"posts\" (\"id\", \"user_id\", \"title\") VALUES ($1, $2, $3)",
    ]
}

pub fn cascade_persist_attaches_clean_relation_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let new_post =
    entity.new_(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity

  let session =
    cascade.stage(
      runtime.new(snapshot),
      cascade.node(new_post, [
        cascade.related("user", [cascade.node(clean_user, [])]),
      ]),
      [cascade.persist_rule("user")],
    )
    |> expect_session

  assert list.length(runtime.tracked_entities(session)) == 2
  assert pending_queries(session)
    == [
      "INSERT INTO \"public\".\"posts\" (\"id\", \"user_id\", \"title\") VALUES ($1, $2, $3)",
    ]
}

pub fn cascade_delete_has_many_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let posts_metadata = expect_metadata(snapshot, "public", "posts")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity
  let clean_post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity

  let session =
    cascade.delete(
      runtime.new(snapshot),
      cascade.node(clean_user, [
        cascade.related("posts", [cascade.node(clean_post, [])]),
      ]),
      [cascade.delete_rule("posts")],
    )
    |> expect_session

  assert pending_queries(session)
    == [
      "DELETE FROM \"public\".\"posts\" WHERE (\"public\".\"posts\".\"id\" = $1)",
      "DELETE FROM \"public\".\"users\" WHERE (\"public\".\"users\".\"id\" = $1)",
    ]
}

pub fn cascade_unknown_relation_test() {
  let snapshot = blog_snapshot()
  let users_metadata = expect_metadata(snapshot, "public", "users")
  let clean_user =
    entity.materialize(users_metadata, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> expect_entity

  assert cascade.stage(
      runtime.new(snapshot),
      cascade.node(clean_user, [
        cascade.related("comments", []),
      ]),
      [cascade.persist_rule("comments")],
    )
    == Error(cascade.UnknownRelation(
      table: relation.table_ref("public", "users"),
      relation_name: "comments",
    ))
}

fn pending_queries(session: runtime.Session) -> List(String) {
  case unit_of_work.flush_plan(runtime.pending_changes(session)) {
    Ok(plan) ->
      list.map(unit_of_work.queries(plan), fn(next_query) {
        let assert Ok(compiled) = galchemy.compile(next_query)
        compiled.sql
      })
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

fn expect_session(
  result: Result(runtime.Session, cascade.CascadeError),
) -> runtime.Session {
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

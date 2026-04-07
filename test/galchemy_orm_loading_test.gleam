import galchemy
import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/orm/declarative
import galchemy/orm/entity
import galchemy/orm/loading
import galchemy/orm/metadata
import galchemy/orm/query as orm_query
import galchemy/schema/relation
import galchemy/session/unit_of_work
import galchemy/sql/compiler
import gleam/list
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn joinedload_applies_left_join_to_query_test() {
  let users =
    user_model()
    |> orm_query.from_model
    |> expect_model_ref
    |> orm_query.as_("u")
  let posts =
    post_model()
    |> orm_query.from_model
    |> expect_model_ref
    |> orm_query.as_("p")
  let applied =
    orm_query.select_fields(users, [orm_query.field("id")])
    |> expect_query
    |> loading.apply(users, [loading.joinedload("posts", posts)])
    |> expect_applied
  let compiled =
    loading.query(applied)
    |> query.Select
    |> galchemy.compile
    |> expect_compiled

  assert compiled.sql
    == "SELECT \"u\".\"id\" FROM \"public\".\"users\" AS \"u\" LEFT JOIN \"public\".\"posts\" AS \"p\" ON (\"u\".\"id\" = \"p\".\"user_id\")"
}

pub fn selectinload_builds_has_many_follow_up_query_test() {
  let users = user_model() |> orm_query.from_model |> expect_model_ref
  let posts = post_model() |> orm_query.from_model |> expect_model_ref
  let applied =
    orm_query.select_all(users)
    |> loading.apply(users, [loading.selectinload("posts", posts)])
    |> expect_applied
  let parent_entities = [
    user_entity(1, "ann@example.com", "Ann"),
    user_entity(2, "bob@example.com", "Bob"),
  ]
  let compiled_queries =
    loading.selectin_queries(applied, parent_entities)
    |> expect_queries
    |> compile_queries

  assert compiled_queries == [
    "SELECT \"public\".\"posts\".\"id\", \"public\".\"posts\".\"user_id\", \"public\".\"posts\".\"title\" FROM \"public\".\"posts\" WHERE ((\"public\".\"posts\".\"user_id\" = $1) OR (\"public\".\"posts\".\"user_id\" = $2))",
  ]
}

pub fn selectinload_builds_belongs_to_follow_up_query_test() {
  let posts = post_model() |> orm_query.from_model |> expect_model_ref
  let users = user_model() |> orm_query.from_model |> expect_model_ref
  let applied =
    orm_query.select_all(posts)
    |> loading.apply(posts, [loading.selectinload("user", users)])
    |> expect_applied
  let parent_entities = [
    post_entity(10, 1, "Hello"),
    post_entity(11, 2, "World"),
  ]
  let compiled_queries =
    loading.selectin_queries(applied, parent_entities)
    |> expect_queries
    |> compile_queries

  assert compiled_queries == [
    "SELECT \"public\".\"users\".\"id\", \"public\".\"users\".\"email\", \"public\".\"users\".\"name\" FROM \"public\".\"users\" WHERE ((\"public\".\"users\".\"id\" = $1) OR (\"public\".\"users\".\"id\" = $2))",
  ]
}

pub fn selectinload_missing_parent_field_error_test() {
  let posts = post_model() |> orm_query.from_model |> expect_model_ref
  let users = user_model() |> orm_query.from_model |> expect_model_ref
  let posts_metadata =
    post_model()
    |> declarative.to_metadata
    |> expect_metadata
  let broken_post =
    entity.materialize(posts_metadata, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> expect_entity
  let applied =
    orm_query.select_all(posts)
    |> loading.apply(posts, [loading.selectinload("user", users)])
    |> expect_applied

  assert loading.selectin_queries(applied, [broken_post])
    == Error(loading.MissingParentField(
      table: relation.table_ref("public", "posts"),
      relation_name: "user",
      column: "user_id",
    ))
}

fn user_model() -> declarative.Model {
  declarative.model_(
    "public",
    "users",
    [
      declarative.primary_key(declarative.int("id")),
      declarative.unique(declarative.text("email")),
      declarative.text("name"),
    ],
    [
      declarative.has_many("posts", "posts_user_id_fkey", "public", "posts", [
        declarative.pair("id", "user_id"),
      ]),
    ],
  )
}

fn post_model() -> declarative.Model {
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
}

fn user_entity(id: Int, email: String, name: String) -> entity.Entity {
  let users_metadata = user_model() |> declarative.to_metadata |> expect_metadata

  entity.materialize(users_metadata, [
    unit_of_work.field("id", ast_expression.Int(id)),
    unit_of_work.field("email", ast_expression.Text(email)),
    unit_of_work.field("name", ast_expression.Text(name)),
  ])
  |> expect_entity
}

fn post_entity(id: Int, user_id: Int, title: String) -> entity.Entity {
  let posts_metadata = post_model() |> declarative.to_metadata |> expect_metadata

  entity.materialize(posts_metadata, [
    unit_of_work.field("id", ast_expression.Int(id)),
    unit_of_work.field("user_id", ast_expression.Int(user_id)),
    unit_of_work.field("title", ast_expression.Text(title)),
  ])
  |> expect_entity
}

fn compile_queries(queries: List(query.Query)) -> List(String) {
  list.map(queries, fn(next_query) {
    let compiled = galchemy.compile(next_query) |> expect_compiled
    compiled.sql
  })
}

fn expect_model_ref(
  result: Result(orm_query.ModelRef, orm_query.QueryError),
) -> orm_query.ModelRef {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_query(
  result: Result(ast_expression.SelectQuery, orm_query.QueryError),
) -> ast_expression.SelectQuery {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_applied(
  result: Result(loading.AppliedOptions, loading.LoadingError),
) -> loading.AppliedOptions {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_queries(
  result: Result(List(query.Query), loading.LoadingError),
) -> List(query.Query) {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_metadata(
  result: Result(metadata.ModelMetadata, declarative.DeclarativeError),
) -> metadata.ModelMetadata {
  case result {
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

fn expect_compiled(
  result: Result(compiler.CompiledQuery, compiler.CompileError),
) -> compiler.CompiledQuery {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

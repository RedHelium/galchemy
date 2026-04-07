import galchemy
import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/select
import galchemy/orm/declarative
import galchemy/orm/metadata
import galchemy/orm/query as orm_query
import galchemy/schema/relation
import galchemy/sql/compiler
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn model_select_all_test() {
  let users = expect_model_ref(user_model() |> orm_query.from_model)
  let compiled =
    orm_query.select_all(users)
    |> query.Select
    |> galchemy.compile
    |> expect_compiled

  assert compiled.sql
    == "SELECT \"public\".\"users\".\"id\", \"public\".\"users\".\"email\", \"public\".\"users\".\"name\" FROM \"public\".\"users\""
  assert compiled.params == []
}

pub fn model_select_fields_with_alias_test() {
  let users =
    user_model()
    |> orm_query.from_model
    |> expect_model_ref
    |> orm_query.as_("u")
  let compiled =
    orm_query.select_fields(users, [
      orm_query.field("id"),
      orm_query.field_as("email", "user_email"),
    ])
    |> expect_query
    |> orm_query.distinct
    |> orm_query.limit(10)
    |> query.Select
    |> galchemy.compile
    |> expect_compiled

  assert compiled.sql
    == "SELECT DISTINCT \"u\".\"id\", \"u\".\"email\" AS \"user_email\" FROM \"public\".\"users\" AS \"u\" LIMIT 10"
}

pub fn model_predicates_and_ordering_test() {
  let users = expect_model_ref(user_model() |> orm_query.from_model)
  let where_ =
    orm_query.ilike(users, "email", expr.text("%@example.com"))
    |> expect_predicate
  let order = orm_query.asc(users, "name") |> expect_order
  let compiled =
    orm_query.select_fields(users, [
      orm_query.field("id"),
      orm_query.field("name"),
    ])
    |> expect_query
    |> orm_query.where_(where_)
    |> orm_query.order_by(order)
    |> orm_query.offset(5)
    |> query.Select
    |> galchemy.compile
    |> expect_compiled

  assert compiled.sql
    == "SELECT \"public\".\"users\".\"id\", \"public\".\"users\".\"name\" FROM \"public\".\"users\" WHERE (\"public\".\"users\".\"email\" ILIKE $1) ORDER BY \"public\".\"users\".\"name\" ASC OFFSET 5"
  assert compiled.params == [ast_expression.Text("%@example.com")]
}

pub fn model_query_unknown_column_test() {
  let users = expect_model_ref(user_model() |> orm_query.from_model)

  assert orm_query.select_fields(users, [orm_query.field("unknown")])
    == Error(orm_query.UnknownColumn(
      table: relation.table_ref("public", "users"),
      column: "unknown",
    ))
}

pub fn from_metadata_test() {
  let metadata =
    user_model()
    |> declarative.to_metadata
    |> expect_metadata
  let users = orm_query.from_metadata(metadata)
  let compiled =
    orm_query.eq(users, "id", expr.int(1))
    |> expect_predicate
    |> fn(where_) {
      orm_query.select_all(users)
      |> orm_query.where_(where_)
    }
    |> query.Select
    |> galchemy.compile
    |> expect_compiled

  assert string.contains(compiled.sql, "\"public\".\"users\".\"id\" = $1")
}

pub fn join_relation_metadata_test() {
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
  let compiled =
    select.select([
      orm_query.item(users, "id") |> expect_item,
      orm_query.item_as(posts, "title", "post_title") |> expect_item,
    ])
    |> select.from(users.table)
    |> orm_query.inner_join_relation(users, "posts", posts)
    |> expect_query
    |> query.Select
    |> galchemy.compile
    |> expect_compiled

  assert compiled.sql
    == "SELECT \"u\".\"id\", \"p\".\"title\" AS \"post_title\" FROM \"public\".\"users\" AS \"u\" INNER JOIN \"public\".\"posts\" AS \"p\" ON (\"u\".\"id\" = \"p\".\"user_id\")"
}

pub fn left_join_relation_metadata_test() {
  let posts =
    post_model()
    |> orm_query.from_model
    |> expect_model_ref
    |> orm_query.as_("p")
  let users =
    user_model()
    |> orm_query.from_model
    |> expect_model_ref
    |> orm_query.as_("u")
  let compiled =
    select.select([
      orm_query.item(posts, "title") |> expect_item,
      orm_query.item_as(users, "email", "author_email") |> expect_item,
    ])
    |> select.from(posts.table)
    |> orm_query.left_join_relation(posts, "user", users)
    |> expect_query
    |> query.Select
    |> galchemy.compile
    |> expect_compiled

  assert compiled.sql
    == "SELECT \"p\".\"title\", \"u\".\"email\" AS \"author_email\" FROM \"public\".\"posts\" AS \"p\" LEFT JOIN \"public\".\"users\" AS \"u\" ON (\"p\".\"user_id\" = \"u\".\"id\")"
}

pub fn join_relation_errors_test() {
  let users = user_model() |> orm_query.from_model |> expect_model_ref
  let accounts =
    declarative.model_(
      "public",
      "accounts",
      [
        declarative.primary_key(declarative.int("id")),
        declarative.text("email"),
      ],
      [],
    )
    |> orm_query.from_model
    |> expect_model_ref
  let base_query = orm_query.select_all(users)

  assert orm_query.inner_join_relation(base_query, users, "comments", accounts)
    == Error(orm_query.UnknownRelation(
      table: relation.table_ref("public", "users"),
      relation_name: "comments",
    ))
  assert orm_query.inner_join_relation(base_query, users, "posts", accounts)
    == Error(orm_query.RelatedTableMismatch(
      relation_name: "posts",
      expected: relation.table_ref("public", "posts"),
      actual: relation.table_ref("public", "accounts"),
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

fn expect_predicate(
  result: Result(ast_expression.Predicate, orm_query.QueryError),
) -> ast_expression.Predicate {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_order(
  result: Result(ast_expression.Order, orm_query.QueryError),
) -> ast_expression.Order {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_item(
  result: Result(ast_expression.SelectItem, orm_query.QueryError),
) -> ast_expression.SelectItem {
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

fn expect_compiled(
  result: Result(compiler.CompiledQuery, compiler.CompileError),
) -> compiler.CompiledQuery {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

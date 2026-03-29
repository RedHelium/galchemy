import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/select
import galchemy/dsl/table
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/loading
import galchemy/session/unit_of_work
import galchemy/sql/compiler
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn eager_join_belongs_to_test() {
  let posts = table.as_(table.table("posts"), "p")
  let post_id = table.int(posts, "id")

  let joined_query = case
    loading.apply_eager(
      select.select([expr.item(expr.col(post_id))])
        |> select.from(posts),
      blog_snapshot(),
      posts,
      "user",
      option.Some("u"),
    )
  {
    Ok(value) -> value
    Error(error) -> {
      let message =
        "Expected eager loading to succeed: " <> string.inspect(error)
      panic as message
    }
  }

  let compiler.CompiledQuery(sql: sql, params: params) = case
    compiler.compile(query.Select(joined_query))
  {
    Ok(compiled) -> compiled
    Error(error) -> {
      let message = "Expected compiler output: " <> string.inspect(error)
      panic as message
    }
  }

  assert sql
    == "SELECT \"p\".\"id\" FROM \"posts\" AS \"p\" LEFT JOIN \"public\".\"users\" AS \"u\" ON (\"p\".\"user_id\" = \"u\".\"id\")"
  assert params == []
}

pub fn eager_join_has_many_test() {
  let users = table.as_(table.table("users"), "u")
  let user_id = table.int(users, "id")

  let joined_query = case
    loading.apply_eager(
      select.select([expr.item(expr.col(user_id))])
        |> select.from(users),
      blog_snapshot(),
      users,
      "posts",
      option.Some("p"),
    )
  {
    Ok(value) -> value
    Error(error) -> {
      let message =
        "Expected eager loading to succeed: " <> string.inspect(error)
      panic as message
    }
  }

  let compiler.CompiledQuery(sql: sql, params: params) = case
    compiler.compile(query.Select(joined_query))
  {
    Ok(compiled) -> compiled
    Error(error) -> {
      let message = "Expected compiler output: " <> string.inspect(error)
      panic as message
    }
  }

  assert sql
    == "SELECT \"u\".\"id\" FROM \"users\" AS \"u\" LEFT JOIN \"public\".\"posts\" AS \"p\" ON (\"u\".\"id\" = \"p\".\"user_id\")"
  assert params == []
}

pub fn lazy_query_belongs_to_test() {
  let lazy_load = case
    loading.lazy_query(
      blog_snapshot(),
      relation.table_ref("public", "posts"),
      "user",
      [
        unit_of_work.identity([
          unit_of_work.field("user_id", ast_expression.Int(1)),
        ]),
        unit_of_work.identity([
          unit_of_work.field("user_id", ast_expression.Int(2)),
        ]),
      ],
    )
  {
    Ok(value) -> value
    Error(error) -> {
      let message =
        "Expected lazy loading to succeed: " <> string.inspect(error)
      panic as message
    }
  }

  let compiler.CompiledQuery(sql: sql, params: params) = case
    compiler.compile(lazy_load.query)
  {
    Ok(compiled) -> compiled
    Error(error) -> {
      let message = "Expected compiler output: " <> string.inspect(error)
      panic as message
    }
  }

  assert sql
    == "SELECT * FROM \"public\".\"users\" WHERE ((\"public\".\"users\".\"id\" = $1) OR (\"public\".\"users\".\"id\" = $2))"
  assert params == [ast_expression.Int(1), ast_expression.Int(2)]
}

pub fn lazy_query_has_many_composite_identity_test() {
  let lazy_load = case
    loading.lazy_query(
      composite_snapshot(),
      relation.table_ref("public", "company_identities"),
      "memberships",
      [
        unit_of_work.identity([
          unit_of_work.field("user_id", ast_expression.Int(10)),
          unit_of_work.field("company_id", ast_expression.Int(7)),
        ]),
        unit_of_work.identity([
          unit_of_work.field("user_id", ast_expression.Int(11)),
          unit_of_work.field("company_id", ast_expression.Int(8)),
        ]),
      ],
    )
  {
    Ok(value) -> value
    Error(error) -> {
      let message =
        "Expected lazy loading to succeed: " <> string.inspect(error)
      panic as message
    }
  }

  let compiler.CompiledQuery(sql: sql, params: params) = case
    compiler.compile(lazy_load.query)
  {
    Ok(compiled) -> compiled
    Error(error) -> {
      let message = "Expected compiler output: " <> string.inspect(error)
      panic as message
    }
  }

  assert sql
    == "SELECT * FROM \"public\".\"memberships\" WHERE (((\"public\".\"memberships\".\"user_id\" = $1) AND (\"public\".\"memberships\".\"company_id\" = $2)) OR ((\"public\".\"memberships\".\"user_id\" = $3) AND (\"public\".\"memberships\".\"company_id\" = $4)))"
  assert params
    == [
      ast_expression.Int(10),
      ast_expression.Int(7),
      ast_expression.Int(11),
      ast_expression.Int(8),
    ]
}

pub fn lazy_query_missing_identity_field_test() {
  assert loading.lazy_query(
      blog_snapshot(),
      relation.table_ref("public", "posts"),
      "user",
      [unit_of_work.identity([])],
    )
    == Error(loading.MissingIdentityField(
      table: relation.table_ref("public", "posts"),
      relation_name: "user",
      column: "user_id",
    ))
}

fn blog_snapshot() -> model.SchemaSnapshot {
  model.SchemaSnapshot(tables: [
    model.TableSchema(
      schema: "public",
      name: "users",
      columns: [column("id", model.IntegerType, False, option.None, 1)],
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

fn composite_snapshot() -> model.SchemaSnapshot {
  model.SchemaSnapshot(tables: [
    model.TableSchema(
      schema: "public",
      name: "company_identities",
      columns: [
        column("user_id", model.IntegerType, False, option.None, 1),
        column("company_id", model.IntegerType, False, option.None, 2),
      ],
      primary_key: option.None,
      unique_constraints: [],
      foreign_keys: [],
      indexes: [],
    ),
    model.TableSchema(
      schema: "public",
      name: "memberships",
      columns: [
        column("user_id", model.IntegerType, False, option.None, 1),
        column("company_id", model.IntegerType, False, option.None, 2),
      ],
      primary_key: option.None,
      unique_constraints: [],
      foreign_keys: [
        model.ForeignKey(
          name: "memberships_user_company_fkey",
          columns: ["user_id", "company_id"],
          referenced_schema: "public",
          referenced_table: "company_identities",
          referenced_columns: ["user_id", "company_id"],
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

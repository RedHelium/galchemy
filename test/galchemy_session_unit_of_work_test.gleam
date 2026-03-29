import galchemy/ast/expression as ast_expression
import galchemy/ast/query
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

pub fn flush_plan_orders_inserts_by_dependency_test() {
  let snapshot = blog_snapshot()

  let session =
    unit_of_work.new(snapshot)
    |> unit_of_work.register_new(relation.table_ref("public", "posts"), [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("First post")),
    ])
    |> unit_of_work.register_new(relation.table_ref("public", "users"), [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])

  case unit_of_work.flush_plan(session) {
    Error(error) -> {
      let message =
        "Expected successful flush plan, got: " <> string.inspect(error)
      panic as message
    }
    Ok(plan) -> {
      assert compiled_queries(unit_of_work.queries(plan))
        == [
          #(
            "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES ($1, $2)",
            [ast_expression.Int(1), ast_expression.Text("Ann")],
          ),
          #(
            "INSERT INTO \"public\".\"posts\" (\"id\", \"user_id\", \"title\") VALUES ($1, $2, $3)",
            [
              ast_expression.Int(10),
              ast_expression.Int(1),
              ast_expression.Text("First post"),
            ],
          ),
        ]
    }
  }
}

pub fn flush_plan_orders_deletes_in_reverse_dependency_order_test() {
  let snapshot = blog_snapshot()

  let session =
    unit_of_work.new(snapshot)
    |> unit_of_work.register_deleted(
      relation.table_ref("public", "users"),
      unit_of_work.identity([unit_of_work.field("id", ast_expression.Int(1))]),
    )
    |> unit_of_work.register_deleted(
      relation.table_ref("public", "posts"),
      unit_of_work.identity([unit_of_work.field("id", ast_expression.Int(10))]),
    )

  case unit_of_work.flush_plan(session) {
    Error(error) -> {
      let message =
        "Expected successful flush plan, got: " <> string.inspect(error)
      panic as message
    }
    Ok(plan) -> {
      assert compiled_queries(unit_of_work.queries(plan))
        == [
          #(
            "DELETE FROM \"public\".\"posts\" WHERE (\"public\".\"posts\".\"id\" = $1)",
            [ast_expression.Int(10)],
          ),
          #(
            "DELETE FROM \"public\".\"users\" WHERE (\"public\".\"users\".\"id\" = $1)",
            [ast_expression.Int(1)],
          ),
        ]
    }
  }
}

pub fn flush_plan_builds_updates_test() {
  let snapshot = blog_snapshot()

  let session =
    unit_of_work.new(snapshot)
    |> unit_of_work.register_dirty(
      relation.table_ref("public", "users"),
      unit_of_work.identity([unit_of_work.field("id", ast_expression.Int(1))]),
      [unit_of_work.field("name", ast_expression.Text("Bob"))],
    )

  case unit_of_work.flush_plan(session) {
    Error(error) -> {
      let message =
        "Expected successful flush plan, got: " <> string.inspect(error)
      panic as message
    }
    Ok(plan) -> {
      assert compiled_queries(unit_of_work.queries(plan))
        == [
          #(
            "UPDATE \"public\".\"users\" SET \"name\" = $1 WHERE (\"public\".\"users\".\"id\" = $2)",
            [ast_expression.Text("Bob"), ast_expression.Int(1)],
          ),
        ]
    }
  }
}

pub fn flush_plan_rejects_unknown_column_test() {
  let session =
    unit_of_work.new(blog_snapshot())
    |> unit_of_work.register_new(relation.table_ref("public", "users"), [
      unit_of_work.field("unknown", ast_expression.Text("oops")),
    ])

  assert unit_of_work.flush_plan(session)
    == Error(unit_of_work.UnknownColumn(
      table: relation.table_ref("public", "users"),
      column: "unknown",
    ))
}

pub fn flush_plan_rejects_empty_identity_test() {
  let session =
    unit_of_work.new(blog_snapshot())
    |> unit_of_work.register_deleted(
      relation.table_ref("public", "users"),
      unit_of_work.identity([]),
    )

  assert unit_of_work.flush_plan(session)
    == Error(unit_of_work.EmptyIdentity(relation.table_ref("public", "users")))
}

fn compiled_queries(
  queries: List(query.Query),
) -> List(#(String, List(ast_expression.SqlValue))) {
  case queries {
    [] -> []
    [next_query, ..rest] -> {
      let compiler.CompiledQuery(sql: sql, params: params) = case
        compiler.compile(next_query)
      {
        Ok(compiled) -> compiled
        Error(error) -> {
          let message =
            "Expected successful compiler output: " <> string.inspect(error)
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

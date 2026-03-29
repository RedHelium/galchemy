import galchemy
import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/sql/compiler
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/execution
import galchemy/session/unit_of_work
import gleam/list
import gleam/option
import gleam/string
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn execute_flush_in_dependency_order_test() {
  let snapshot = blog_snapshot()
  let users = relation.table_ref("public", "users")
  let posts = relation.table_ref("public", "posts")
  let session =
    unit_of_work.new(snapshot)
    |> unit_of_work.register_new(posts, [
      unit_of_work.field("id", ast_expression.Int(10)),
      unit_of_work.field("user_id", ast_expression.Int(1)),
      unit_of_work.field("title", ast_expression.Text("Hello")),
    ])
    |> unit_of_work.register_new(users, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])
    |> unit_of_work.register_dirty(
      users,
      unit_of_work.identity([unit_of_work.field("id", ast_expression.Int(1))]),
      [unit_of_work.field("name", ast_expression.Text("Annie"))],
    )
    |> unit_of_work.register_deleted(
      posts,
      unit_of_work.identity([unit_of_work.field("id", ast_expression.Int(10))]),
    )

  let #(execution_result, cleared_session) =
    execution.execute(session, galchemy.compile)
    |> expect_execution

  let executed_sql =
    execution.queries(execution_result)
    |> list.map(fn(next) {
      let compiler.CompiledQuery(sql: sql, ..) = next.result
      sql
    })

  assert executed_sql == [
    "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES ($1, $2)",
    "INSERT INTO \"public\".\"posts\" (\"id\", \"user_id\", \"title\") VALUES ($1, $2, $3)",
    "UPDATE \"public\".\"users\" SET \"name\" = $1 WHERE (\"public\".\"users\".\"id\" = $2)",
    "DELETE FROM \"public\".\"posts\" WHERE (\"public\".\"posts\".\"id\" = $1)",
  ]
  assert is_empty_session(cleared_session)
}

pub fn execute_flush_returns_query_error_test() {
  let snapshot = blog_snapshot()
  let users = relation.table_ref("public", "users")
  let session =
    unit_of_work.new(snapshot)
    |> unit_of_work.register_new(users, [
      unit_of_work.field("id", ast_expression.Int(1)),
      unit_of_work.field("name", ast_expression.Text("Ann")),
    ])

  assert execution.execute(session, failing_executor)
    == Error(execution.QueryError("boom"))
}

pub fn execute_flush_returns_session_error_test() {
  let session =
    unit_of_work.new(blog_snapshot())
    |> unit_of_work.register_new(
      relation.table_ref("public", "users"),
      [],
    )

  assert execution.execute(session, galchemy.compile)
    == Error(
      execution.SessionError(
        unit_of_work.EmptyInsertValues(relation.table_ref("public", "users")),
      ),
    )
}

fn failing_executor(
  _query: query.Query,
) -> Result(compiler.CompiledQuery, String) {
  Error("boom")
}

fn expect_execution(
  result: Result(
    #(
      execution.FlushExecution(compiler.CompiledQuery),
      unit_of_work.Session,
    ),
    execution.ExecutionError(compiler.CompileError),
  ),
) -> #(
  execution.FlushExecution(compiler.CompiledQuery),
  unit_of_work.Session,
) {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn is_empty_session(session: unit_of_work.Session) -> Bool {
  case unit_of_work.flush_plan(session) {
    Ok(plan) -> unit_of_work.queries(plan) == []
    Error(_) -> False
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

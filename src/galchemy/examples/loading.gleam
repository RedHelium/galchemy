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
import gleam/io
import gleam/option
import gleam/string

pub fn main() -> Nil {
  let users = table.as_(table.table("users"), "u")
  let user_id = table.int(users, "id")

  let eager_query = case
    loading.apply_eager(
      select.select([expr.item(expr.col(user_id))])
        |> select.from(users),
      snapshot(),
      users,
      "posts",
      option.Some("p"),
    )
  {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }

  let lazy_load = case
    loading.lazy_query(
      snapshot(),
      relation.table_ref("public", "users"),
      "posts",
      [
        unit_of_work.identity([unit_of_work.field("id", ast_expression.Int(1))]),
        unit_of_work.identity([unit_of_work.field("id", ast_expression.Int(2))]),
      ],
    )
  {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }

  io.println("Loading example")
  io.println("Eager loading query:")
  io.println(string.inspect(compiler.compile(query.Select(eager_query))))
  io.println("Lazy loading query:")
  io.println(string.inspect(compiler.compile(lazy_load.query)))
}

fn snapshot() -> model.SchemaSnapshot {
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

fn column(
  name: String,
  data_type: model.ColumnType,
  nullable: Bool,
  default: option.Option(String),
  ordinal_position: Int,
) -> model.ColumnSchema {
  model.ColumnSchema(name:, data_type:, nullable:, default:, ordinal_position:)
}

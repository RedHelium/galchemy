import galchemy/ast/expression
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import galchemy/sql/compiler
import gleam/int
import gleam/io
import gleam/list

pub fn main() -> Nil {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")
  let active = table.bool(users, "active")

  let q =
    select.select([expr.item(expr.col(id)), expr.item(expr.col(name))])
    |> select.from(users)
    |> select.where_(predicate.and(
      predicate.eq(expr.col(active), expr.bool(True)),
      predicate.ilike(expr.col(name), expr.text("A%")),
    ))
    |> select.order_by(select.asc(expr.col(id)))
    |> select.limit(20)
    |> select.offset(0)

  case compiler.compile(query.Select(q)) {
    Ok(compiler.CompiledQuery(sql: sql, params: params)) -> {
      io.println("SQL:")
      io.println(sql)
      io.println("Параметров: " <> int.to_string(list.length(params)))
      io.println("Значения параметров: " <> params_to_string(params))
    }
    Error(error) -> {
      io.println("Ошибка компиляции: " <> compile_error_to_string(error))
    }
  }
}

fn params_to_string(params: List(expression.SqlValue)) -> String {
  case params {
    [] -> "[]"
    _ -> "[" <> join_params(params) <> "]"
  }
}

fn join_params(params: List(expression.SqlValue)) -> String {
  case params {
    [] -> ""
    [first, ..rest] -> join_params_loop(rest, sql_value_to_string(first))
  }
}

fn join_params_loop(rest: List(expression.SqlValue), acc: String) -> String {
  case rest {
    [] -> acc
    [x, ..xs] -> join_params_loop(xs, acc <> ", " <> sql_value_to_string(x))
  }
}

fn sql_value_to_string(value: expression.SqlValue) -> String {
  case value {
    expression.Text(v) -> "Text(" <> v <> ")"
    expression.Int(v) -> "Int(" <> int.to_string(v) <> ")"
    expression.Bool(v) -> {
      case v {
        True -> "Bool(True)"
        False -> "Bool(False)"
      }
    }
    expression.Null -> "Null"
  }
}

fn compile_error_to_string(error: compiler.CompileError) -> String {
  case error {
    compiler.MissingFrom -> "MissingFrom"
    compiler.EmptyInList -> "EmptyInList"
    compiler.EmptyInsertValues -> "EmptyInsertValues"
    compiler.EmptyUpdateAssignments -> "EmptyUpdateAssignments"
    compiler.InvalidLimit(v) -> "InvalidLimit(" <> int.to_string(v) <> ")"
    compiler.InvalidOffset(v) -> "InvalidOffset(" <> int.to_string(v) <> ")"
    compiler.Unsupported(message) -> "Unsupported(" <> message <> ")"
  }
}

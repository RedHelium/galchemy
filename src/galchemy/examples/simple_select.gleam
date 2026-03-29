import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import galchemy/sql/postgres
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/option
import gleam/string
import pog

pub fn main() -> Nil {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")
  let active = table.bool(users, "active")

  let select_query =
    select.select([expr.item(expr.col(id)), expr.item(expr.col(name))])
    |> select.from(users)
    |> select.where_(predicate.and(
      predicate.eq(expr.col(active), expr.bool(True)),
      predicate.ilike(expr.col(name), expr.text("A%")),
    ))
    |> select.order_by(select.asc(expr.col(id)))
    |> select.limit(20)
    |> select.offset(0)

  case start_connection() {
    Ok(connection) -> {
      let decoder =
        decode.at([0], decode.int)
        |> decode.then(fn(id) {
          decode.at([1], decode.string)
          |> decode.map(fn(name) { #(id, name) })
        })

      case
        postgres.execute_with_decoder(
          query.Select(select_query),
          decoder,
          connection,
        )
      {
        Ok(pog.Returned(count: count, rows: rows)) -> {
          io.println("Запрос выполнен успешно.")
          io.println("Количество строк: " <> int.to_string(count))
          io.println("Результат: " <> string.inspect(rows))
        }
        Error(error) -> {
          io.println("Ошибка выполнения запроса: " <> string.inspect(error))
        }
      }
    }
    Error(error) -> {
      io.println("Ошибка подключения к PostgreSQL: " <> error)
    }
  }
}

fn start_connection() -> Result(pog.Connection, String) {
  let config =
    pog.default_config(pool_name: process.new_name("galchemy_example_pool"))
    |> pog.host("localhost")
    |> pog.port(5432)
    |> pog.database("galchemy")
    |> pog.user("postgres")
    |> pog.password(option.Some("123"))
    |> pog.ssl(pog.SslDisabled)

  case pog.start(config) {
    Ok(started) -> Ok(started.data)
    Error(error) -> Error(string.inspect(error))
  }
}

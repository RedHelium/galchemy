import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import galchemy/sql/postgres
import gleam/dynamic/decode
import gleam/io
import gleam/string
import pog

pub fn build_query() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")
  let active = table.bool(users, "active")

  select.select([
    expr.item(expr.col(id)),
    expr.item(expr.col(name)),
  ])
  |> select.from(users)
  |> select.where_(predicate.and(
    predicate.eq(expr.col(active), expr.bool(True)),
    predicate.ilike(expr.col(name), expr.text("A%")),
  ))
  |> select.order_by(select.asc(expr.col(id)))
  |> select.limit(20)
}

pub fn row_decoder() -> decode.Decoder(#(Int, String)) {
  decode.at([0], decode.int)
  |> decode.then(fn(id) {
    decode.at([1], decode.string)
    |> decode.map(fn(name) { #(id, name) })
  })
}

pub fn run(connection: pog.Connection) {
  postgres.execute_with(query.Select(build_query()), row_decoder(), connection)
}

pub fn main() -> Nil {
  io.println("Simple select example")
  io.println(string.inspect(galchemy.compile(query.Select(build_query()))))
}

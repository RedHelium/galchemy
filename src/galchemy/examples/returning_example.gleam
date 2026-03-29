import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/insert
import galchemy/dsl/table
import galchemy/dsl/update
import gleam/io
import gleam/string

fn users_table() {
  table.table("users")
}

fn users_id() {
  table.int(users_table(), "id")
}

fn users_name() {
  table.text(users_table(), "name")
}

pub fn build_insert() {
  insert.insert_into(users_table())
  |> insert.value(users_name(), expr.text("Ann"))
  |> insert.returning([
    expr.item(expr.col(users_id())),
    expr.item(expr.col(users_name())),
  ])
}

pub fn build_update() {
  update.update(users_table())
  |> update.set(users_name(), expr.text("Bob"))
  |> update.returning([
    expr.item(expr.col(users_id())),
    expr.item(expr.col(users_name())),
  ])
}

pub fn main() -> Nil {
  io.println("Returning example")
  io.println(string.inspect(galchemy.compile(query.Insert(build_insert()))))
  io.println(string.inspect(galchemy.compile(query.Update(build_update()))))
}

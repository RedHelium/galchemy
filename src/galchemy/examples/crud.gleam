import galchemy
import galchemy/ast/query
import galchemy/dsl/delete
import galchemy/dsl/expr
import galchemy/dsl/insert
import galchemy/dsl/predicate
import galchemy/dsl/select
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

fn users_active() {
  table.bool(users_table(), "active")
}

pub fn build_select() {
  select.select([
    expr.item(expr.col(users_id())),
    expr.item(expr.col(users_name())),
  ])
  |> select.from(users_table())
  |> select.where_(predicate.eq(expr.col(users_active()), expr.bool(True)))
}

pub fn build_insert() {
  insert.insert_into(users_table())
  |> insert.value(users_id(), expr.int(1))
  |> insert.value(users_name(), expr.text("Ann"))
  |> insert.value(users_active(), expr.bool(True))
  |> insert.returning([expr.item(expr.col(users_id()))])
}

pub fn build_update() {
  update.update(users_table())
  |> update.set(users_name(), expr.text("Bob"))
  |> update.where_(predicate.eq(expr.col(users_id()), expr.int(1)))
  |> update.returning([
    expr.item(expr.col(users_id())),
    expr.item(expr.col(users_name())),
  ])
}

pub fn build_delete() {
  delete.delete_from(users_table())
  |> delete.where_(predicate.eq(expr.col(users_id()), expr.int(1)))
  |> delete.returning([expr.item(expr.col(users_id()))])
}

pub fn main() -> Nil {
  io.println("CRUD example")
  io.println(string.inspect(galchemy.compile(query.Select(build_select()))))
  io.println(string.inspect(galchemy.compile(query.Insert(build_insert()))))
  io.println(string.inspect(galchemy.compile(query.Update(build_update()))))
  io.println(string.inspect(galchemy.compile(query.Delete(build_delete()))))
}

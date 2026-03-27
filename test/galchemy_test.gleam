import galchemy/ast/expression as ast_expression
import galchemy/ast/query
import galchemy/dsl/delete
import galchemy/dsl/expr
import galchemy/dsl/insert
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import galchemy/dsl/update
import galchemy/sql/compiler
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

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

pub fn compile_select_test() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")

  let select_query =
    select.select([
      expr.item(expr.col(id)),
      expr.as_(expr.col(name), "user_name"),
    ])
    |> select.from(users)
    |> select.where_(predicate.and(
      predicate.eq(expr.col(id), expr.int(42)),
      predicate.like(expr.col(name), expr.text("%Ann%")),
    ))
    |> select.order_by(select.asc(expr.col(id)))
    |> select.limit(10)
    |> select.offset(5)
    |> select.distinct

  let compiled = compiler.compile(query.Select(select_query))

  case compiled {
    Ok(compiler.CompiledQuery(sql: sql, params: params)) -> {
      assert sql
        == "SELECT DISTINCT u.id, u.name AS user_name FROM users AS u WHERE ((u.id = $1) AND (u.name LIKE $2)) ORDER BY u.id ASC LIMIT 10 OFFSET 5"
      assert params == [ast_expression.Int(42), ast_expression.Text("%Ann%")]
    }
    Error(_) -> panic as "Expected successful SELECT compilation"
  }
}

pub fn compile_select_missing_from_test() {
  let select_query = select.select([])

  let compiled = compiler.compile(query.Select(select_query))

  case compiled {
    Ok(_) -> panic as "Expected MissingFrom error"
    Error(error) -> {
      assert error == compiler.MissingFrom
    }
  }
}

pub fn compile_select_empty_in_list_test() {
  let select_query =
    select.select([expr.item(expr.col(users_id()))])
    |> select.from(users_table())
    |> select.where_(predicate.in_list(expr.col(users_id()), []))

  let compiled = compiler.compile(query.Select(select_query))

  case compiled {
    Ok(_) -> panic as "Expected EmptyInList error"
    Error(error) -> {
      assert error == compiler.EmptyInList
    }
  }
}

pub fn compile_select_invalid_limit_test() {
  let select_query =
    select.select([expr.item(expr.col(users_id()))])
    |> select.from(users_table())
    |> select.limit(-1)

  let compiled = compiler.compile(query.Select(select_query))

  case compiled {
    Ok(_) -> panic as "Expected InvalidLimit error"
    Error(error) -> {
      assert error == compiler.InvalidLimit(-1)
    }
  }
}

pub fn compile_select_invalid_offset_test() {
  let select_query =
    select.select([expr.item(expr.col(users_id()))])
    |> select.from(users_table())
    |> select.offset(-2)

  let compiled = compiler.compile(query.Select(select_query))

  case compiled {
    Ok(_) -> panic as "Expected InvalidOffset error"
    Error(error) -> {
      assert error == compiler.InvalidOffset(-2)
    }
  }
}

pub fn compile_insert_test() {
  let insert_query =
    insert.insert_into(users_table())
    |> insert.value(users_id(), expr.int(1))
    |> insert.value(users_name(), expr.text("Ann"))
    |> insert.returning([expr.item(expr.col(users_id()))])

  let compiled = compiler.compile(query.Insert(insert_query))

  case compiled {
    Ok(compiler.CompiledQuery(sql: sql, params: params)) -> {
      assert sql
        == "INSERT INTO users (id, name) VALUES ($1, $2) RETURNING users.id"
      assert params == [ast_expression.Int(1), ast_expression.Text("Ann")]
    }
    Error(_) -> panic as "Expected successful INSERT compilation"
  }
}

pub fn compile_insert_empty_values_test() {
  let insert_query = insert.insert_into(users_table())
  let compiled = compiler.compile(query.Insert(insert_query))

  case compiled {
    Ok(_) -> panic as "Expected EmptyInsertValues error"
    Error(error) -> {
      assert error == compiler.EmptyInsertValues
    }
  }
}

pub fn compile_update_test() {
  let update_query =
    update.update(users_table())
    |> update.set(users_name(), expr.text("Bob"))
    |> update.set(users_active(), expr.bool(True))
    |> update.where_(predicate.eq(expr.col(users_id()), expr.int(7)))
    |> update.returning([
      expr.item(expr.col(users_id())),
      expr.item(expr.col(users_name())),
    ])

  let compiled = compiler.compile(query.Update(update_query))

  case compiled {
    Ok(compiler.CompiledQuery(sql: sql, params: params)) -> {
      assert sql
        == "UPDATE users SET name = $1, active = $2 WHERE (users.id = $3) RETURNING users.id, users.name"
      assert params
        == [
          ast_expression.Text("Bob"),
          ast_expression.Bool(True),
          ast_expression.Int(7),
        ]
    }
    Error(_) -> panic as "Expected successful UPDATE compilation"
  }
}

pub fn compile_update_empty_assignments_test() {
  let update_query = update.update(users_table())
  let compiled = compiler.compile(query.Update(update_query))

  case compiled {
    Ok(_) -> panic as "Expected EmptyUpdateAssignments error"
    Error(error) -> {
      assert error == compiler.EmptyUpdateAssignments
    }
  }
}

pub fn compile_delete_test() {
  let delete_query =
    delete.delete_from(users_table())
    |> delete.where_(predicate.eq(expr.col(users_id()), expr.int(9)))
    |> delete.returning([expr.item(expr.col(users_id()))])

  let compiled = compiler.compile(query.Delete(delete_query))

  case compiled {
    Ok(compiler.CompiledQuery(sql: sql, params: params)) -> {
      assert sql == "DELETE FROM users WHERE (users.id = $1) RETURNING users.id"
      assert params == [ast_expression.Int(9)]
    }
    Error(_) -> panic as "Expected successful DELETE compilation"
  }
}

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
import galchemy/sql/postgres
import gleam/string
import gleam/time/calendar
import gleam/time/timestamp
import gleeunit
import pog

@external(erlang, "galchemy_test_support", "query_sql")
fn query_sql(query: pog.Query(a)) -> String

@external(erlang, "galchemy_test_support", "query_parameters")
fn query_parameters(query: pog.Query(a)) -> List(pog.Value)

@external(erlang, "galchemy_test_support", "query_timeout")
fn query_timeout(query: pog.Query(a)) -> Int

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

fn expect_compiled(value: query.Query) -> compiler.CompiledQuery {
  case compiler.compile(value) {
    Ok(compiled) -> compiled
    Error(error) -> {
      let message =
        "Expected successful compilation, got: " <> string.inspect(error)
      panic as message
    }
  }
}

fn compiled_sql(value: query.Query) -> String {
  let compiler.CompiledQuery(sql: sql, params: _) = expect_compiled(value)
  sql
}

fn assert_sql_snapshot(name: String, actual: String, expected: String) {
  case actual == expected {
    True -> Nil
    False -> {
      let message =
        "SQL snapshot mismatch for "
        <> name
        <> "\nExpected: "
        <> expected
        <> "\nActual: "
        <> actual
      panic as message
    }
  }
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

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Select(select_query))

  assert sql
    == "SELECT DISTINCT \"u\".\"id\", \"u\".\"name\" AS \"user_name\" FROM \"users\" AS \"u\" WHERE ((\"u\".\"id\" = $1) AND (\"u\".\"name\" LIKE $2)) ORDER BY \"u\".\"id\" ASC LIMIT 10 OFFSET 5"
  assert params == [ast_expression.Int(42), ast_expression.Text("%Ann%")]
}

pub fn compile_select_join_test() {
  let users = table.as_(table.table("users"), "u")
  let posts = table.as_(table.table("posts"), "p")
  let user_id = table.int(users, "id")
  let post_user_id = table.int(posts, "user_id")
  let post_published = table.bool(posts, "published")

  let select_query =
    select.select([
      expr.item(expr.col(user_id)),
      expr.as_(expr.col(post_user_id), "post_user_id"),
    ])
    |> select.from(users)
    |> select.inner_join(
      posts,
      predicate.eq(expr.col(user_id), expr.col(post_user_id)),
    )
    |> select.where_(predicate.eq(expr.col(post_published), expr.bool(True)))

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Select(select_query))

  assert sql
    == "SELECT \"u\".\"id\", \"p\".\"user_id\" AS \"post_user_id\" FROM \"users\" AS \"u\" INNER JOIN \"posts\" AS \"p\" ON (\"u\".\"id\" = \"p\".\"user_id\") WHERE (\"p\".\"published\" = $1)"
  assert params == [ast_expression.Bool(True)]
}

pub fn compile_select_schema_qualified_table_test() {
  let users = table.in_schema(table.table("users"), "app")
  let id = table.int(users, "id")

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(
      query.Select(
        select.select([expr.item(expr.col(id))])
        |> select.from(users),
      ),
    )

  assert sql == "SELECT \"app\".\"users\".\"id\" FROM \"app\".\"users\""
  assert params == []
}

pub fn compile_select_schema_qualified_table_with_alias_test() {
  let users =
    table.table("users")
    |> table.in_schema("app")
    |> table.as_("u")
  let id = table.int(users, "id")

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(
      query.Select(
        select.select([expr.item(expr.col(id))])
        |> select.from(users),
      ),
    )

  assert sql == "SELECT \"u\".\"id\" FROM \"app\".\"users\" AS \"u\""
  assert params == []
}

pub fn compile_select_left_join_test() {
  let users = table.as_(table.table("users"), "u")
  let posts = table.as_(table.table("posts"), "p")
  let user_id = table.int(users, "id")
  let post_user_id = table.int(posts, "user_id")

  let select_query =
    select.select([
      expr.item(expr.col(user_id)),
      expr.item(expr.col(post_user_id)),
    ])
    |> select.from(users)
    |> select.left_join(
      posts,
      predicate.eq(expr.col(user_id), expr.col(post_user_id)),
    )

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Select(select_query))

  assert sql
    == "SELECT \"u\".\"id\", \"p\".\"user_id\" FROM \"users\" AS \"u\" LEFT JOIN \"posts\" AS \"p\" ON (\"u\".\"id\" = \"p\".\"user_id\")"
  assert params == []
}

pub fn compile_select_expression_helpers_test() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(
      query.Select(
        select.select([
          expr.as_(expr.count_all(), "total"),
          expr.as_(expr.lower(expr.col(name)), "normalized_name"),
          expr.as_(
            expr.coalesce([expr.col(name), expr.text("unknown")]),
            "display_name",
          ),
          expr.as_(expr.max(expr.col(id)), "max_id"),
        ])
        |> select.from(users),
      ),
    )

  assert sql
    == "SELECT COUNT(*) AS \"total\", LOWER(\"u\".\"name\") AS \"normalized_name\", COALESCE(\"u\".\"name\", $1) AS \"display_name\", MAX(\"u\".\"id\") AS \"max_id\" FROM \"users\" AS \"u\""
  assert params == [ast_expression.Text("unknown")]
}

pub fn compile_select_invalid_function_name_test() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")

  let select_query =
    select.select([expr.item(expr.call("", [expr.col(id)]))])
    |> select.from(users)

  case compiler.compile(query.Select(select_query)) {
    Ok(_) -> panic as "Expected InvalidFunctionName error"
    Error(error) -> {
      assert error == compiler.InvalidFunctionName("")
    }
  }
}

pub fn compile_select_group_by_having_test() {
  let users = table.as_(table.table("users"), "u")
  let active = table.bool(users, "active")
  let id = table.int(users, "id")

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(
      query.Select(
        select.select([
          expr.item(expr.col(active)),
          expr.as_(expr.count(expr.col(id)), "user_count"),
        ])
        |> select.from(users)
        |> select.group_by(expr.col(active))
        |> select.having(predicate.gt(expr.count(expr.col(id)), expr.int(1))),
      ),
    )

  assert sql
    == "SELECT \"u\".\"active\", COUNT(\"u\".\"id\") AS \"user_count\" FROM \"users\" AS \"u\" GROUP BY \"u\".\"active\" HAVING (COUNT(\"u\".\"id\") > $1)"
  assert params == [ast_expression.Int(1)]
}

pub fn compile_select_having_without_group_by_test() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")

  let select_query =
    select.select([expr.item(expr.col(id))])
    |> select.from(users)
    |> select.having(predicate.gt(expr.count(expr.col(id)), expr.int(1)))

  case compiler.compile(query.Select(select_query)) {
    Ok(_) -> panic as "Expected HavingWithoutGroupBy error"
    Error(error) -> {
      assert error == compiler.HavingWithoutGroupBy
    }
  }
}

pub fn compile_select_missing_from_test() {
  let select_query = select.select([])

  case compiler.compile(query.Select(select_query)) {
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

  case compiler.compile(query.Select(select_query)) {
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

  case compiler.compile(query.Select(select_query)) {
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

  case compiler.compile(query.Select(select_query)) {
    Ok(_) -> panic as "Expected InvalidOffset error"
    Error(error) -> {
      assert error == compiler.InvalidOffset(-2)
    }
  }
}

pub fn compile_select_parameter_order_test() {
  let users = table.as_(users_table(), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")
  let active = table.bool(users, "active")

  let select_query =
    select.select([expr.item(expr.col(id))])
    |> select.from(users)
    |> select.where_(predicate.and(
      predicate.or(
        predicate.eq(expr.col(id), expr.int(10)),
        predicate.eq(expr.col(name), expr.text("Ann")),
      ),
      predicate.eq(expr.col(active), expr.bool(True)),
    ))

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Select(select_query))

  assert sql
    == "SELECT \"u\".\"id\" FROM \"users\" AS \"u\" WHERE (((\"u\".\"id\" = $1) OR (\"u\".\"name\" = $2)) AND (\"u\".\"active\" = $3))"
  assert params
    == [
      ast_expression.Int(10),
      ast_expression.Text("Ann"),
      ast_expression.Bool(True),
    ]
}

pub fn compile_insert_test() {
  let insert_query =
    insert.insert_into(users_table())
    |> insert.value(users_id(), expr.int(1))
    |> insert.value(users_name(), expr.text("Ann"))
    |> insert.returning([expr.item(expr.col(users_id()))])

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Insert(insert_query))

  assert sql
    == "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2) RETURNING \"users\".\"id\""
  assert params == [ast_expression.Int(1), ast_expression.Text("Ann")]
}

pub fn compile_insert_multi_row_test() {
  let insert_query =
    insert.insert_into(users_table())
    |> insert.row([
      insert.field(users_id(), expr.int(1)),
      insert.field(users_name(), expr.text("Ann")),
    ])
    |> insert.row([
      insert.field(users_id(), expr.int(2)),
      insert.field(users_name(), expr.text("Bob")),
    ])
    |> insert.returning([expr.item(expr.col(users_id()))])

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Insert(insert_query))

  assert sql
    == "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2), ($3, $4) RETURNING \"users\".\"id\""
  assert params
    == [
      ast_expression.Int(1),
      ast_expression.Text("Ann"),
      ast_expression.Int(2),
      ast_expression.Text("Bob"),
    ]
}

pub fn compile_insert_values_batch_test() {
  let insert_query =
    insert.insert_into(users_table())
    |> insert.values([
      [
        insert.field(users_id(), expr.int(10)),
        insert.field(users_name(), expr.text("Alice")),
      ],
      [
        insert.field(users_id(), expr.int(11)),
        insert.field(users_name(), expr.text("Carol")),
      ],
    ])

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Insert(insert_query))

  assert sql == "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2), ($3, $4)"
  assert params
    == [
      ast_expression.Int(10),
      ast_expression.Text("Alice"),
      ast_expression.Int(11),
      ast_expression.Text("Carol"),
    ]
}

pub fn compile_insert_empty_values_test() {
  let insert_query = insert.insert_into(users_table())

  case compiler.compile(query.Insert(insert_query)) {
    Ok(_) -> panic as "Expected EmptyInsertValues error"
    Error(error) -> {
      assert error == compiler.EmptyInsertValues
    }
  }
}

pub fn compile_insert_inconsistent_row_shape_test() {
  let insert_query =
    insert.insert_into(users_table())
    |> insert.values([
      [insert.field(users_id(), expr.int(1))],
      [
        insert.field(users_id(), expr.int(2)),
        insert.field(users_name(), expr.text("Bob")),
      ],
    ])

  case compiler.compile(query.Insert(insert_query)) {
    Ok(_) -> panic as "Expected InconsistentInsertRowShape error"
    Error(error) -> {
      assert error == compiler.InconsistentInsertRowShape
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

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Update(update_query))

  assert sql
    == "UPDATE \"users\" SET \"name\" = $1, \"active\" = $2 WHERE (\"users\".\"id\" = $3) RETURNING \"users\".\"id\", \"users\".\"name\""
  assert params
    == [
      ast_expression.Text("Bob"),
      ast_expression.Bool(True),
      ast_expression.Int(7),
    ]
}

pub fn compile_update_empty_assignments_test() {
  let update_query = update.update(users_table())

  case compiler.compile(query.Update(update_query)) {
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

  let compiler.CompiledQuery(sql: sql, params: params) =
    expect_compiled(query.Delete(delete_query))

  assert sql == "DELETE FROM \"users\" WHERE (\"users\".\"id\" = $1) RETURNING \"users\".\"id\""
  assert params == [ast_expression.Int(9)]
}

pub fn compile_to_query_test() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")

  let select_query =
    select.select([expr.item(expr.col(id))])
    |> select.from(users)
    |> select.where_(predicate.eq(expr.col(id), expr.int(42)))

  case postgres.compile_to_query(query.Select(select_query)) {
    Ok(pog_query) -> {
      assert query_sql(pog_query)
        == "SELECT \"u\".\"id\" FROM \"users\" AS \"u\" WHERE (\"u\".\"id\" = $1)"
      assert query_parameters(pog_query) == [pog.int(42)]
      assert query_timeout(pog_query) == 5000
    }
    Error(_) -> panic as "Expected successful compile_to_query result"
  }
}

pub fn compile_to_query_group_by_having_test() {
  let users = table.as_(table.table("users"), "u")
  let active = table.bool(users, "active")
  let id = table.int(users, "id")

  let select_query =
    select.select([
      expr.item(expr.col(active)),
      expr.as_(expr.count(expr.col(id)), "user_count"),
    ])
    |> select.from(users)
    |> select.group_by(expr.col(active))
    |> select.having(predicate.gt(expr.count(expr.col(id)), expr.int(2)))

  case postgres.compile_to_query(query.Select(select_query)) {
    Ok(pog_query) -> {
      assert query_sql(pog_query)
        == "SELECT \"u\".\"active\", COUNT(\"u\".\"id\") AS \"user_count\" FROM \"users\" AS \"u\" GROUP BY \"u\".\"active\" HAVING (COUNT(\"u\".\"id\") > $1)"
      assert query_parameters(pog_query) == [pog.int(2)]
    }
    Error(_) -> panic as "Expected successful compile_to_query result"
  }
}

pub fn compile_to_query_multi_row_insert_test() {
  let insert_query =
    insert.insert_into(users_table())
    |> insert.values([
      [
        insert.field(users_id(), expr.int(1)),
        insert.field(users_name(), expr.text("Ann")),
      ],
      [
        insert.field(users_id(), expr.int(2)),
        insert.field(users_name(), expr.text("Bob")),
      ],
    ])

  case postgres.compile_to_query(query.Insert(insert_query)) {
    Ok(pog_query) -> {
      assert query_sql(pog_query)
        == "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2), ($3, $4)"
      assert query_parameters(pog_query)
        == [pog.int(1), pog.text("Ann"), pog.int(2), pog.text("Bob")]
    }
    Error(_) -> panic as "Expected successful compile_to_query result"
  }
}

pub fn compile_to_query_error_test() {
  case postgres.compile_to_query(query.Select(select.select([]))) {
    Ok(_) -> panic as "Expected compile_to_query to propagate compile errors"
    Error(error) -> {
      assert error == compiler.MissingFrom
    }
  }
}

pub fn to_pog_value_test() {
  assert postgres.to_pog_value(ast_expression.Int(7)) == pog.int(7)
  assert postgres.to_pog_value(ast_expression.Float(1.5)) == pog.float(1.5)
  assert postgres.to_pog_value(ast_expression.Text("Ann")) == pog.text("Ann")
  assert postgres.to_pog_value(ast_expression.Bool(True)) == pog.bool(True)
  assert
    postgres.to_pog_value(
      ast_expression.Timestamp(
        timestamp.from_unix_seconds_and_nanoseconds(
          seconds: 1_700_000_000,
          nanoseconds: 123_000_000,
        ),
      ),
    )
    == pog.timestamp(
      timestamp.from_unix_seconds_and_nanoseconds(
        seconds: 1_700_000_000,
        nanoseconds: 123_000_000,
      ),
    )
  assert
    postgres.to_pog_value(
      ast_expression.Date(
        calendar.Date(year: 2026, month: calendar.March, day: 29),
      ),
    )
    == pog.calendar_date(
      calendar.Date(year: 2026, month: calendar.March, day: 29),
    )
  assert
    postgres.to_pog_value(
      ast_expression.TimeOfDay(
        calendar.TimeOfDay(
          hours: 12,
          minutes: 34,
          seconds: 56,
          nanoseconds: 123_000_000,
        ),
      ),
    )
    == pog.calendar_time_of_day(
      calendar.TimeOfDay(
        hours: 12,
        minutes: 34,
        seconds: 56,
        nanoseconds: 123_000_000,
      ),
    )
  assert postgres.to_pog_value(ast_expression.Null) == pog.null()
}

pub fn from_compiled_test() {
  let compiled =
    compiler.CompiledQuery(
      sql: "SELECT users.id FROM users WHERE users.id = $1 AND users.name = $2",
      params: [ast_expression.Int(7), ast_expression.Text("Ann")],
    )

  let pog_query = postgres.from_compiled(compiled)

  assert query_sql(pog_query)
    == "SELECT users.id FROM users WHERE users.id = $1 AND users.name = $2"
  assert query_parameters(pog_query) == [pog.int(7), pog.text("Ann")]
  assert query_timeout(pog_query) == 5000
}

pub fn from_compiled_extended_values_test() {
  let ts =
    timestamp.from_unix_seconds_and_nanoseconds(
      seconds: 1_700_000_000,
      nanoseconds: 123_000_000,
    )
  let date = calendar.Date(year: 2026, month: calendar.March, day: 29)
  let time =
    calendar.TimeOfDay(
      hours: 12,
      minutes: 34,
      seconds: 56,
      nanoseconds: 123_000_000,
    )

  let compiled =
    compiler.CompiledQuery(
      sql: "SELECT $1, $2, $3, $4",
      params: [
        ast_expression.Float(1.5),
        ast_expression.Timestamp(ts),
        ast_expression.Date(date),
        ast_expression.TimeOfDay(time),
      ],
    )

  let pog_query = postgres.from_compiled(compiled)

  assert query_sql(pog_query) == "SELECT $1, $2, $3, $4"
  assert query_parameters(pog_query)
    == [
      pog.float(1.5),
      pog.timestamp(ts),
      pog.calendar_date(date),
      pog.calendar_time_of_day(time),
    ]
}

pub fn snapshot_select_sql_test() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")

  let sql =
    query.Select(
      select.select([
        expr.item(expr.col(id)),
        expr.as_(expr.col(name), "user_name"),
      ])
      |> select.from(users)
      |> select.where_(predicate.eq(expr.col(id), expr.int(1)))
      |> select.order_by(select.asc(expr.col(name))),
    )
    |> compiled_sql

  assert_sql_snapshot(
    "select_basic",
    sql,
    "SELECT \"u\".\"id\", \"u\".\"name\" AS \"user_name\" FROM \"users\" AS \"u\" WHERE (\"u\".\"id\" = $1) ORDER BY \"u\".\"name\" ASC",
  )
}

pub fn snapshot_schema_qualified_sql_test() {
  let users =
    table.table("users")
    |> table.in_schema("analytics")
    |> table.as_("u")
  let id = table.int(users, "id")

  let sql =
    query.Select(
      select.select([expr.item(expr.col(id))])
      |> select.from(users),
    )
    |> compiled_sql

  assert_sql_snapshot(
    "select_schema_qualified",
    sql,
    "SELECT \"u\".\"id\" FROM \"analytics\".\"users\" AS \"u\"",
  )
}

pub fn snapshot_insert_sql_test() {
  let sql =
    query.Insert(
      insert.insert_into(users_table())
      |> insert.value(users_id(), expr.int(1))
      |> insert.value(users_name(), expr.text("Ann"))
      |> insert.returning([expr.item(expr.col(users_id()))]),
    )
    |> compiled_sql

  assert_sql_snapshot(
    "insert_basic",
    sql,
    "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2) RETURNING \"users\".\"id\"",
  )
}

pub fn snapshot_insert_multi_row_sql_test() {
  let sql =
    query.Insert(
      insert.insert_into(users_table())
      |> insert.values([
        [
          insert.field(users_id(), expr.int(1)),
          insert.field(users_name(), expr.text("Ann")),
        ],
        [
          insert.field(users_id(), expr.int(2)),
          insert.field(users_name(), expr.text("Bob")),
        ],
      ])
      |> insert.returning([expr.item(expr.col(users_id()))]),
    )
    |> compiled_sql

  assert_sql_snapshot(
    "insert_multi_row",
    sql,
    "INSERT INTO \"users\" (\"id\", \"name\") VALUES ($1, $2), ($3, $4) RETURNING \"users\".\"id\"",
  )
}

pub fn snapshot_update_sql_test() {
  let sql =
    query.Update(
      update.update(users_table())
      |> update.set(users_name(), expr.text("Bob"))
      |> update.where_(predicate.eq(expr.col(users_id()), expr.int(7)))
      |> update.returning([expr.item(expr.col(users_name()))]),
    )
    |> compiled_sql

  assert_sql_snapshot(
    "update_basic",
    sql,
    "UPDATE \"users\" SET \"name\" = $1 WHERE (\"users\".\"id\" = $2) RETURNING \"users\".\"name\"",
  )
}

pub fn snapshot_delete_sql_test() {
  let sql =
    query.Delete(
      delete.delete_from(users_table())
      |> delete.where_(predicate.eq(expr.col(users_id()), expr.int(9)))
      |> delete.returning([expr.item(expr.col(users_id()))]),
    )
    |> compiled_sql

  assert_sql_snapshot(
    "delete_basic",
    sql,
    "DELETE FROM \"users\" WHERE (\"users\".\"id\" = $1) RETURNING \"users\".\"id\"",
  )
}

pub fn identifier_policy_quotes_identifiers_test() {
  let sql =
    query.Select(
      select.select([])
      |> select.from(table.as_(table.table("UserAccounts"), "UA")),
    )
    |> compiled_sql

  assert sql == "SELECT * FROM \"UserAccounts\" AS \"UA\""
}

pub fn identifier_policy_escapes_quotes_test() {
  let accounts =
    table.table("user\"accounts")
    |> table.as_("u\"1")
  let account_name = table.text(accounts, "display\"name")

  let sql =
    query.Select(
      select.select([expr.item(expr.col(account_name))])
      |> select.from(accounts),
    )
    |> compiled_sql

  assert sql
    == "SELECT \"u\"\"1\".\"display\"\"name\" FROM \"user\"\"accounts\" AS \"u\"\"1\""
}

import galchemy/ast/expression
import galchemy/ast/join
import galchemy/ast/order
import galchemy/ast/predicate
import galchemy/ast/query
import galchemy/ast/schema
import gleam/int
import gleam/option

/// Represents a compiled SQL statement and its positional parameters.
pub type CompiledQuery {
  CompiledQuery(sql: String, params: List(expression.SqlValue))
}

/// Represents all supported compilation errors.
pub type CompileError {
  MissingFrom
  EmptyInList
  EmptyInsertValues
  EmptyUpdateAssignments
  InvalidLimit(Int)
  InvalidOffset(Int)
  Unsupported(String)
}

/// Carries mutable compilation state in an immutable way.
type CompileState {
  CompileState(next_param: Int, params: List(expression.SqlValue))
}

/// Provides `use <-` chaining for `Result` values.
fn result_try(result: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case result {
    Ok(value) -> next(value)
    Error(error) -> Error(error)
  }
}

/// Creates a fresh compilation state.
fn new_state() -> CompileState {
  CompileState(next_param: 1, params: [])
}

/// Builds the final compiled query from SQL text and collected parameters.
fn finalize(sql: String, state: CompileState) -> CompiledQuery {
  CompiledQuery(sql: sql, params: reverse(state.params))
}

/// Compiles any top-level query variant into SQL.
pub fn compile(q: query.Query) -> Result(CompiledQuery, CompileError) {
  case q {
    query.Select(s) -> compile_select(s)
    query.Insert(i) -> compile_insert(i)
    query.Update(u) -> compile_update(u)
    query.Delete(d) -> compile_delete(d)
  }
}

/// Compiles a SELECT query into SQL with positional parameters.
pub fn compile_select(
  q: query.SelectQuery,
) -> Result(CompiledQuery, CompileError) {
  let query.SelectQuery(
    items: items,
    from: from,
    joins: joins,
    where_: where_,
    order_by: order_by,
    limit: limit,
    offset: offset,
    distinct: distinct,
  ) = q

  case from {
    option.None -> Error(MissingFrom)
    option.Some(table) -> {
      let state0 = new_state()

      use #(items_sql, state1) <- result_try(compile_select_items(items, state0))
      use #(joins_sql, state2) <- result_try(compile_joins(joins, state1))
      use #(where_sql, state3) <- result_try(compile_where(where_, state2))
      use #(order_sql, state4) <- result_try(compile_order_by(order_by, state3))
      use limit_sql <- result_try(compile_limit(limit))
      use offset_sql <- result_try(compile_offset(offset))

      let distinct_sql = case distinct {
        True -> "DISTINCT "
        False -> ""
      }

      let sql =
        "SELECT "
        <> distinct_sql
        <> items_sql
        <> " FROM "
        <> compile_table_ref(table)
        <> joins_sql
        <> where_sql
        <> order_sql
        <> limit_sql
        <> offset_sql

      Ok(finalize(sql, state4))
    }
  }
}

/// Compiles an INSERT query into SQL with positional parameters.
pub fn compile_insert(
  q: query.InsertQuery,
) -> Result(CompiledQuery, CompileError) {
  let query.InsertQuery(table: table, values: values, returning: returning) = q
  let state0 = new_state()

  use #(columns_sql, values_sql, state1) <- result_try(compile_insert_values(
    values,
    state0,
  ))
  use #(returning_sql, state2) <- result_try(compile_returning(
    returning,
    state1,
  ))

  let sql =
    "INSERT INTO "
    <> compile_table_ref(table)
    <> " ("
    <> columns_sql
    <> ") VALUES ("
    <> values_sql
    <> ")"
    <> returning_sql

  Ok(finalize(sql, state2))
}

/// Compiles an UPDATE query into SQL with positional parameters.
pub fn compile_update(
  q: query.UpdateQuery,
) -> Result(CompiledQuery, CompileError) {
  let query.UpdateQuery(
    table: table,
    assignments: assignments,
    where_: where_,
    returning: returning,
  ) = q
  let state0 = new_state()

  use #(set_sql, state1) <- result_try(compile_assignments(assignments, state0))
  use #(where_sql, state2) <- result_try(compile_where(where_, state1))
  use #(returning_sql, state3) <- result_try(compile_returning(
    returning,
    state2,
  ))

  let sql =
    "UPDATE "
    <> compile_table_ref(table)
    <> " SET "
    <> set_sql
    <> where_sql
    <> returning_sql

  Ok(finalize(sql, state3))
}

/// Compiles a DELETE query into SQL with positional parameters.
pub fn compile_delete(
  q: query.DeleteQuery,
) -> Result(CompiledQuery, CompileError) {
  let query.DeleteQuery(table: table, where_: where_, returning: returning) = q
  let state0 = new_state()

  use #(where_sql, state1) <- result_try(compile_where(where_, state0))
  use #(returning_sql, state2) <- result_try(compile_returning(
    returning,
    state1,
  ))

  let sql =
    "DELETE FROM " <> compile_table_ref(table) <> where_sql <> returning_sql

  Ok(finalize(sql, state2))
}

/// Compiles a list of select items, defaulting to `*` when the list is empty.
fn compile_select_items(
  items: List(expression.SelectItem),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case items {
    [] -> Ok(#("*", state))
    _ -> compile_select_items_loop(items, [], state)
  }
}

/// Recursively compiles select items and joins them with commas.
fn compile_select_items_loop(
  items: List(expression.SelectItem),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case items {
    [] -> Ok(#(join_strings(reverse(acc), ", "), state))
    [item, ..rest] -> {
      use #(item_sql, next_state) <- result_try(compile_select_item(item, state))
      compile_select_items_loop(rest, [item_sql, ..acc], next_state)
    }
  }
}

/// Compiles a single select item and optional alias.
fn compile_select_item(
  item: expression.SelectItem,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let expression.SelectItem(expression: expr, alias: alias) = item
  use #(expr_sql, state1) <- result_try(compile_expression(expr, state))

  let sql = case alias {
    option.None -> expr_sql
    option.Some(a) -> expr_sql <> " AS " <> a
  }

  Ok(#(sql, state1))
}

/// Compiles a list of insert pairs into `(columns_sql, values_sql)`.
fn compile_insert_values(
  values: List(#(schema.ColumnMeta, expression.Expression)),
  state: CompileState,
) -> Result(#(String, String, CompileState), CompileError) {
  case values {
    [] -> Error(EmptyInsertValues)
    _ -> compile_insert_values_loop(values, [], [], state)
  }
}

/// Recursively compiles insert pairs.
fn compile_insert_values_loop(
  values: List(#(schema.ColumnMeta, expression.Expression)),
  columns_acc: List(String),
  values_acc: List(String),
  state: CompileState,
) -> Result(#(String, String, CompileState), CompileError) {
  case values {
    [] -> {
      let columns_sql = join_strings(reverse(columns_acc), ", ")
      let values_sql = join_strings(reverse(values_acc), ", ")
      Ok(#(columns_sql, values_sql, state))
    }

    [#(column, expr), ..rest] -> {
      use #(expr_sql, next_state) <- result_try(compile_expression(expr, state))
      let column_sql = compile_column_name(column)
      compile_insert_values_loop(
        rest,
        [column_sql, ..columns_acc],
        [expr_sql, ..values_acc],
        next_state,
      )
    }
  }
}

/// Compiles a list of update assignments into `col = expr` fragments.
fn compile_assignments(
  assignments: List(#(schema.ColumnMeta, expression.Expression)),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case assignments {
    [] -> Error(EmptyUpdateAssignments)
    _ -> compile_assignments_loop(assignments, [], state)
  }
}

/// Recursively compiles update assignments.
fn compile_assignments_loop(
  assignments: List(#(schema.ColumnMeta, expression.Expression)),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case assignments {
    [] -> Ok(#(join_strings(reverse(acc), ", "), state))
    [#(column, expr), ..rest] -> {
      use #(expr_sql, next_state) <- result_try(compile_expression(expr, state))
      let assignment_sql = compile_column_name(column) <> " = " <> expr_sql
      compile_assignments_loop(rest, [assignment_sql, ..acc], next_state)
    }
  }
}

/// Compiles an optional RETURNING clause.
fn compile_returning(
  returning: List(expression.SelectItem),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case returning {
    [] -> Ok(#("", state))
    _ -> {
      use #(items_sql, next_state) <- result_try(compile_select_items_loop(
        returning,
        [],
        state,
      ))
      Ok(#(" RETURNING " <> items_sql, next_state))
    }
  }
}

/// Compiles an expression and updates parameter state if needed.
fn compile_expression(
  expr: expression.Expression,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case expr {
    expression.ColumnExpr(meta) -> Ok(#(compile_column_ref(meta), state))
    expression.ValueExpr(expression.Null) -> Ok(#("NULL", state))
    expression.ValueExpr(value) -> {
      let #(placeholder, next_state) = push_param(value, state)
      Ok(#(placeholder, next_state))
    }
  }
}

/// Allocates the next positional placeholder and stores the parameter.
fn push_param(
  value: expression.SqlValue,
  state: CompileState,
) -> #(String, CompileState) {
  let placeholder = "$" <> int.to_string(state.next_param)
  let next_state =
    CompileState(next_param: state.next_param + 1, params: [
      value,
      ..state.params
    ])
  #(placeholder, next_state)
}

/// Compiles all JOIN clauses for a SELECT query.
fn compile_joins(
  joins: List(join.Join),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  compile_joins_loop(joins, [], state)
}

/// Recursively compiles JOIN clauses.
fn compile_joins_loop(
  joins: List(join.Join),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case joins {
    [] -> Ok(#(concat_strings(reverse(acc)), state))
    [j, ..rest] -> {
      use #(join_sql, next_state) <- result_try(compile_join(j, state))
      compile_joins_loop(rest, [join_sql, ..acc], next_state)
    }
  }
}

/// Compiles a single JOIN clause.
fn compile_join(
  j: join.Join,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let join.Join(kind: kind, table: table, on: on) = j
  let kind_sql = case kind {
    join.InnerJoin -> " INNER JOIN "
    join.LeftJoin -> " LEFT JOIN "
  }

  use #(on_sql, state1) <- result_try(compile_predicate(on, state))
  Ok(#(kind_sql <> compile_table_ref(table) <> " ON " <> on_sql, state1))
}

/// Compiles an optional WHERE clause.
fn compile_where(
  where_: option.Option(predicate.Predicate),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case where_ {
    option.None -> Ok(#("", state))
    option.Some(pred) -> {
      use #(pred_sql, next_state) <- result_try(compile_predicate(pred, state))
      Ok(#(" WHERE " <> pred_sql, next_state))
    }
  }
}

/// Compiles the ORDER BY clause.
fn compile_order_by(
  order_by: List(order.Order),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case order_by {
    [] -> Ok(#("", state))
    _ -> {
      use #(items_sql, next_state) <- result_try(compile_order_items(
        order_by,
        state,
      ))
      Ok(#(" ORDER BY " <> items_sql, next_state))
    }
  }
}

/// Compiles all ORDER BY items.
fn compile_order_items(
  items: List(order.Order),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  compile_order_items_loop(items, [], state)
}

/// Recursively compiles ORDER BY items.
fn compile_order_items_loop(
  items: List(order.Order),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case items {
    [] -> Ok(#(join_strings(reverse(acc), ", "), state))
    [item, ..rest] -> {
      use #(item_sql, next_state) <- result_try(compile_order_item(item, state))
      compile_order_items_loop(rest, [item_sql, ..acc], next_state)
    }
  }
}

/// Compiles a single ORDER BY item.
fn compile_order_item(
  item: order.Order,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let order.Order(expression: expr, direction: direction) = item
  use #(expr_sql, state1) <- result_try(compile_expression(expr, state))
  let dir_sql = case direction {
    order.Asc -> "ASC"
    order.Desc -> "DESC"
  }
  Ok(#(expr_sql <> " " <> dir_sql, state1))
}

/// Compiles a predicate tree into SQL.
fn compile_predicate(
  pred: predicate.Predicate,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case pred {
    predicate.Comparison(lhs: lhs, op: op, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_expression(rhs, state1))
      Ok(#(
        "(" <> lhs_sql <> " " <> op_to_sql(op) <> " " <> rhs_sql <> ")",
        state2,
      ))
    }

    predicate.And(left: left, right: right) -> {
      use #(left_sql, state1) <- result_try(compile_predicate(left, state))
      use #(right_sql, state2) <- result_try(compile_predicate(right, state1))
      Ok(#("(" <> left_sql <> " AND " <> right_sql <> ")", state2))
    }

    predicate.Or(left: left, right: right) -> {
      use #(left_sql, state1) <- result_try(compile_predicate(left, state))
      use #(right_sql, state2) <- result_try(compile_predicate(right, state1))
      Ok(#("(" <> left_sql <> " OR " <> right_sql <> ")", state2))
    }

    predicate.Not(predicate: inner) -> {
      use #(inner_sql, state1) <- result_try(compile_predicate(inner, state))
      Ok(#("(NOT " <> inner_sql <> ")", state1))
    }

    predicate.InList(lhs: lhs, rhs: rhs) -> {
      case rhs {
        [] -> Error(EmptyInList)
        _ -> {
          use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
          use #(rhs_sql, state2) <- result_try(compile_expression_list(
            rhs,
            state1,
          ))
          Ok(#("(" <> lhs_sql <> " IN (" <> rhs_sql <> "))", state2))
        }
      }
    }

    predicate.IsNull(expression: expr) -> {
      use #(expr_sql, state1) <- result_try(compile_expression(expr, state))
      Ok(#("(" <> expr_sql <> " IS NULL)", state1))
    }

    predicate.IsNotNull(expression: expr) -> {
      use #(expr_sql, state1) <- result_try(compile_expression(expr, state))
      Ok(#("(" <> expr_sql <> " IS NOT NULL)", state1))
    }

    predicate.Like(lhs: lhs, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_expression(rhs, state1))
      Ok(#("(" <> lhs_sql <> " LIKE " <> rhs_sql <> ")", state2))
    }

    predicate.Ilike(lhs: lhs, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_expression(rhs, state1))
      Ok(#("(" <> lhs_sql <> " ILIKE " <> rhs_sql <> ")", state2))
    }
  }
}

/// Compiles a list of expressions separated by commas.
fn compile_expression_list(
  exprs: List(expression.Expression),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  compile_expression_list_loop(exprs, [], state)
}

/// Recursively compiles expression lists.
fn compile_expression_list_loop(
  exprs: List(expression.Expression),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case exprs {
    [] -> Ok(#(join_strings(reverse(acc), ", "), state))
    [expr, ..rest] -> {
      use #(expr_sql, next_state) <- result_try(compile_expression(expr, state))
      compile_expression_list_loop(rest, [expr_sql, ..acc], next_state)
    }
  }
}

/// Compiles LIMIT clause and validates non-negative values.
fn compile_limit(limit: option.Option(Int)) -> Result(String, CompileError) {
  case limit {
    option.None -> Ok("")
    option.Some(v) -> {
      case v < 0 {
        True -> Error(InvalidLimit(v))
        False -> Ok(" LIMIT " <> int.to_string(v))
      }
    }
  }
}

/// Compiles OFFSET clause and validates non-negative values.
fn compile_offset(offset: option.Option(Int)) -> Result(String, CompileError) {
  case offset {
    option.None -> Ok("")
    option.Some(v) -> {
      case v < 0 {
        True -> Error(InvalidOffset(v))
        False -> Ok(" OFFSET " <> int.to_string(v))
      }
    }
  }
}

/// Compiles table reference with optional alias.
fn compile_table_ref(table: schema.Table) -> String {
  let schema.Table(name: name, alias: alias) = table
  case alias {
    option.None -> name
    option.Some(a) -> name <> " AS " <> a
  }
}

/// Compiles fully-qualified column reference using table name or alias.
fn compile_column_ref(column: schema.ColumnMeta) -> String {
  let schema.ColumnMeta(table: table, name: column_name) = column
  let schema.Table(name: table_name, alias: alias) = table
  let qualifier = case alias {
    option.None -> table_name
    option.Some(a) -> a
  }
  qualifier <> "." <> column_name
}

/// Compiles column name for INSERT/UPDATE targets.
fn compile_column_name(column: schema.ColumnMeta) -> String {
  let schema.ColumnMeta(table: _, name: name) = column
  name
}

/// Maps comparison operators to SQL text.
fn op_to_sql(op: predicate.ComparisonOp) -> String {
  case op {
    predicate.Eq -> "="
    predicate.Neq -> "!="
    predicate.Gt -> ">"
    predicate.Gte -> ">="
    predicate.Lt -> "<"
    predicate.Lte -> "<="
  }
}

/// Joins strings with a separator.
fn join_strings(parts: List(String), sep: String) -> String {
  case parts {
    [] -> ""
    [first, ..rest] -> join_strings_loop(rest, first, sep)
  }
}

/// Recursively joins strings with a separator.
fn join_strings_loop(parts: List(String), acc: String, sep: String) -> String {
  case parts {
    [] -> acc
    [part, ..rest] -> join_strings_loop(rest, acc <> sep <> part, sep)
  }
}

/// Concatenates strings without any separator.
fn concat_strings(parts: List(String)) -> String {
  case parts {
    [] -> ""
    [p, ..rest] -> p <> concat_strings(rest)
  }
}

/// Reverses a list.
fn reverse(items: List(a)) -> List(a) {
  reverse_loop(items, [])
}

/// Tail-recursive reverse helper.
fn reverse_loop(items: List(a), acc: List(a)) -> List(a) {
  case items {
    [] -> acc
    [x, ..xs] -> reverse_loop(xs, [x, ..acc])
  }
}

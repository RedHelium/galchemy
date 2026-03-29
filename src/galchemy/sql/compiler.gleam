import galchemy/ast/expression
import galchemy/ast/query
import galchemy/ast/schema
import gleam/int
import gleam/list
import gleam/option
import gleam/string

pub type CompiledQuery {
  CompiledQuery(sql: String, params: List(expression.SqlValue))
}

pub type CompileError {
  MissingFrom
  EmptyInList
  EmptyInsertValues
  EmptyUpdateAssignments
  InconsistentInsertRowShape
  HavingWithoutGroupBy
  InvalidFunctionName(String)
  InvalidLimit(Int)
  InvalidOffset(Int)
}

pub type CompilerConfig {
  CompilerConfig(
    render_identifier: fn(String) -> String,
    validate_function_name: fn(String) -> Result(String, CompileError),
  )
}

type CompileState {
  CompileState(
    next_param: Int,
    params: List(expression.SqlValue),
    config: CompilerConfig,
  )
}

fn result_try(result: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case result {
    Ok(value) -> next(value)
    Error(error) -> Error(error)
  }
}

pub fn default_config() -> CompilerConfig {
  CompilerConfig(
    render_identifier: default_render_identifier,
    validate_function_name: default_validate_function_name,
  )
}

fn new_state(config: CompilerConfig) -> CompileState {
  CompileState(next_param: 1, params: [], config: config)
}

fn finalize(sql: String, state: CompileState) -> CompiledQuery {
  CompiledQuery(sql: sql, params: reverse(state.params))
}

pub fn compile(q: query.Query) -> Result(CompiledQuery, CompileError) {
  compile_with(q, default_config())
}

pub fn compile_with(
  q: query.Query,
  config: CompilerConfig,
) -> Result(CompiledQuery, CompileError) {
  case q {
    query.Select(s) -> compile_select_with(s, config)
    query.Insert(i) -> compile_insert_with(i, config)
    query.Update(u) -> compile_update_with(u, config)
    query.Delete(d) -> compile_delete_with(d, config)
  }
}

pub fn compile_select(
  q: expression.SelectQuery,
) -> Result(CompiledQuery, CompileError) {
  compile_select_with(q, default_config())
}

pub fn compile_select_with(
  q: expression.SelectQuery,
  config: CompilerConfig,
) -> Result(CompiledQuery, CompileError) {
  let state0 = new_state(config)
  use #(sql, state1) <- result_try(compile_select_query(q, state0))
  Ok(finalize(sql, state1))
}

fn compile_select_query(
  q: expression.SelectQuery,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let expression.SelectQuery(
    ctes: ctes,
    items: items,
    from: from,
    joins: joins,
    where_: where_,
    group_by: group_by,
    having_: having_,
    unions: unions,
    order_by: order_by,
    limit: limit,
    offset: offset,
    distinct: distinct,
  ) = q

  case from {
    option.None -> Error(MissingFrom)
    option.Some(source) -> {
      use #(ctes_sql, state1) <- result_try(compile_ctes(ctes, state))
      use #(items_sql, state2) <- result_try(compile_select_items(items, state1))
      use #(from_sql, state3) <- result_try(compile_source(source, state2))
      use #(joins_sql, state4) <- result_try(compile_joins(joins, state3))
      use #(where_sql, state5) <- result_try(compile_where(where_, state4))
      use #(group_by_sql, state6) <- result_try(compile_group_by(
        group_by,
        state5,
      ))
      use #(having_sql, state7) <- result_try(compile_having(
        group_by,
        having_,
        state6,
      ))
      use #(order_sql, state8) <- result_try(compile_order_by(order_by, state7))
      use limit_sql <- result_try(compile_limit(limit))
      use offset_sql <- result_try(compile_offset(offset))
      use #(unions_sql, state9) <- result_try(compile_set_operations(
        unions,
        state8,
      ))

      let distinct_sql = case distinct {
        True -> "DISTINCT "
        False -> ""
      }

      let base_sql =
        "SELECT "
        <> distinct_sql
        <> items_sql
        <> " FROM "
        <> from_sql
        <> joins_sql
        <> where_sql
        <> group_by_sql
        <> having_sql
        <> order_sql
        <> limit_sql
        <> offset_sql

      Ok(#(ctes_sql <> base_sql <> unions_sql, state9))
    }
  }
}

pub fn compile_insert(
  q: query.InsertQuery,
) -> Result(CompiledQuery, CompileError) {
  compile_insert_with(q, default_config())
}

pub fn compile_insert_with(
  q: query.InsertQuery,
  config: CompilerConfig,
) -> Result(CompiledQuery, CompileError) {
  let query.InsertQuery(table: table, rows: rows, returning: returning) = q
  let state0 = new_state(config)

  use #(columns_sql, rows_sql, state1) <- result_try(compile_insert_rows(
    rows,
    state0,
  ))
  use #(returning_sql, state2) <- result_try(compile_returning(
    returning,
    state1,
  ))

  let sql =
    "INSERT INTO "
    <> compile_table_ref(table, state0.config)
    <> " ("
    <> columns_sql
    <> ") VALUES "
    <> rows_sql
    <> returning_sql

  Ok(finalize(sql, state2))
}

pub fn compile_update(
  q: query.UpdateQuery,
) -> Result(CompiledQuery, CompileError) {
  compile_update_with(q, default_config())
}

pub fn compile_update_with(
  q: query.UpdateQuery,
  config: CompilerConfig,
) -> Result(CompiledQuery, CompileError) {
  let query.UpdateQuery(
    table: table,
    assignments: assignments,
    where_: where_,
    returning: returning,
  ) = q
  let state0 = new_state(config)

  use #(set_sql, state1) <- result_try(compile_assignments(assignments, state0))
  use #(where_sql, state2) <- result_try(compile_where(where_, state1))
  use #(returning_sql, state3) <- result_try(compile_returning(
    returning,
    state2,
  ))

  let sql =
    "UPDATE "
    <> compile_table_ref(table, state0.config)
    <> " SET "
    <> set_sql
    <> where_sql
    <> returning_sql

  Ok(finalize(sql, state3))
}

pub fn compile_delete(
  q: query.DeleteQuery,
) -> Result(CompiledQuery, CompileError) {
  compile_delete_with(q, default_config())
}

pub fn compile_delete_with(
  q: query.DeleteQuery,
  config: CompilerConfig,
) -> Result(CompiledQuery, CompileError) {
  let query.DeleteQuery(table: table, where_: where_, returning: returning) = q
  let state0 = new_state(config)

  use #(where_sql, state1) <- result_try(compile_where(where_, state0))
  use #(returning_sql, state2) <- result_try(compile_returning(
    returning,
    state1,
  ))

  let sql =
    "DELETE FROM "
    <> compile_table_ref(table, state0.config)
    <> where_sql
    <> returning_sql

  Ok(finalize(sql, state2))
}

fn compile_ctes(
  ctes: List(expression.Cte),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case ctes {
    [] -> Ok(#("", state))
    _ -> {
      use #(ctes_sql, next_state) <- result_try(compile_ctes_loop(
        ctes,
        [],
        state,
      ))
      Ok(#("WITH " <> ctes_sql <> " ", next_state))
    }
  }
}

fn compile_ctes_loop(
  ctes: List(expression.Cte),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case ctes {
    [] -> Ok(#(join_strings(reverse(acc), ", "), state))
    [cte, ..rest] -> {
      let expression.Cte(name: name, query: cte_query) = cte
      use #(cte_sql, next_state) <- result_try(compile_select_query(
        cte_query,
        state,
      ))
      let next_item =
        compile_identifier(name, state.config) <> " AS (" <> cte_sql <> ")"
      compile_ctes_loop(rest, [next_item, ..acc], next_state)
    }
  }
}

fn compile_set_operations(
  operations: List(expression.SetOperation),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  compile_set_operations_loop(operations, [], state)
}

fn compile_set_operations_loop(
  operations: List(expression.SetOperation),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case operations {
    [] -> Ok(#(concat_strings(reverse(acc)), state))
    [operation, ..rest] -> {
      let expression.SetOperation(kind: kind, query: union_query) = operation
      let operator_sql = case kind {
        expression.Union -> " UNION "
        expression.UnionAll -> " UNION ALL "
      }

      use #(union_sql, next_state) <- result_try(compile_select_query(
        union_query,
        state,
      ))

      compile_set_operations_loop(
        rest,
        [operator_sql <> union_sql, ..acc],
        next_state,
      )
    }
  }
}

fn compile_select_items(
  items: List(expression.SelectItem),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case items {
    [] -> Ok(#("*", state))
    _ -> compile_select_items_loop(items, [], state)
  }
}

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

fn compile_select_item(
  item: expression.SelectItem,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let expression.SelectItem(expression: expr, alias: alias) = item
  use #(expr_sql, state1) <- result_try(compile_expression(expr, state))

  let sql = case alias {
    option.None -> expr_sql
    option.Some(a) -> expr_sql <> " AS " <> compile_identifier(a, state.config)
  }

  Ok(#(sql, state1))
}

fn compile_insert_rows(
  rows: List(List(#(schema.ColumnMeta, expression.Expression))),
  state: CompileState,
) -> Result(#(String, String, CompileState), CompileError) {
  case rows {
    [] -> Error(EmptyInsertValues)
    [first_row, ..rest_rows] -> {
      case first_row {
        [] -> Error(EmptyInsertValues)
        _ -> {
          use #(columns_sql, first_row_sql, state1) <- result_try(
            compile_insert_row(first_row, state),
          )
          use #(rest_rows_sql, state2) <- result_try(compile_insert_rows_loop(
            rest_rows,
            first_row,
            [first_row_sql],
            state1,
          ))
          Ok(#(columns_sql, join_strings(rest_rows_sql, ", "), state2))
        }
      }
    }
  }
}

fn compile_insert_rows_loop(
  rows: List(List(#(schema.ColumnMeta, expression.Expression))),
  first_row: List(#(schema.ColumnMeta, expression.Expression)),
  acc: List(String),
  state: CompileState,
) -> Result(#(List(String), CompileState), CompileError) {
  case rows {
    [] -> Ok(#(acc, state))
    [row, ..rest] -> {
      case same_insert_shape(first_row, row) {
        False -> Error(InconsistentInsertRowShape)
        True -> {
          use #(_, row_sql, next_state) <- result_try(compile_insert_row(
            row,
            state,
          ))
          compile_insert_rows_loop(
            rest,
            first_row,
            list.append(acc, [row_sql]),
            next_state,
          )
        }
      }
    }
  }
}

fn compile_insert_row(
  row: List(#(schema.ColumnMeta, expression.Expression)),
  state: CompileState,
) -> Result(#(String, String, CompileState), CompileError) {
  compile_insert_row_loop(row, [], [], state)
}

fn compile_insert_row_loop(
  row: List(#(schema.ColumnMeta, expression.Expression)),
  columns_acc: List(String),
  values_acc: List(String),
  state: CompileState,
) -> Result(#(String, String, CompileState), CompileError) {
  case row {
    [] -> {
      let columns_sql = join_strings(reverse(columns_acc), ", ")
      let values_sql = join_strings(reverse(values_acc), ", ")
      Ok(#(columns_sql, "(" <> values_sql <> ")", state))
    }
    [#(column, expr), ..rest] -> {
      use #(expr_sql, next_state) <- result_try(compile_expression(expr, state))
      let column_sql = compile_column_name(column, state.config)
      compile_insert_row_loop(
        rest,
        [column_sql, ..columns_acc],
        [expr_sql, ..values_acc],
        next_state,
      )
    }
  }
}

fn same_insert_shape(
  one: List(#(schema.ColumnMeta, expression.Expression)),
  other: List(#(schema.ColumnMeta, expression.Expression)),
) -> Bool {
  case one, other {
    [], [] -> True
    [#(left_column, _), ..left_rest], [#(right_column, _), ..right_rest] ->
      left_column == right_column && same_insert_shape(left_rest, right_rest)
    _, _ -> False
  }
}

fn compile_assignments(
  assignments: List(#(schema.ColumnMeta, expression.Expression)),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case assignments {
    [] -> Error(EmptyUpdateAssignments)
    _ -> compile_assignments_loop(assignments, [], state)
  }
}

fn compile_assignments_loop(
  assignments: List(#(schema.ColumnMeta, expression.Expression)),
  acc: List(String),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case assignments {
    [] -> Ok(#(join_strings(reverse(acc), ", "), state))
    [#(column, expr), ..rest] -> {
      use #(expr_sql, next_state) <- result_try(compile_expression(expr, state))
      let assignment_sql =
        compile_column_name(column, state.config) <> " = " <> expr_sql
      compile_assignments_loop(rest, [assignment_sql, ..acc], next_state)
    }
  }
}

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

fn compile_expression(
  expr: expression.Expression,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case expr {
    expression.ColumnExpr(meta) ->
      Ok(#(compile_column_ref(meta, state.config), state))
    expression.ValueExpr(expression.Null) -> Ok(#("NULL", state))
    expression.StarExpr -> Ok(#("*", state))
    expression.FunctionExpr(name: name, arguments: arguments) -> {
      use function_name <- result_try(compile_function_name(name, state.config))
      use #(arguments_sql, next_state) <- result_try(compile_expression_list(
        arguments,
        state,
      ))
      Ok(#(function_name <> "(" <> arguments_sql <> ")", next_state))
    }
    expression.UnaryOpExpr(operator: operator, operand: operand) -> {
      use #(operand_sql, next_state) <- result_try(compile_expression(
        operand,
        state,
      ))
      Ok(#(
        "(" <> unary_operator_to_sql(operator) <> operand_sql <> ")",
        next_state,
      ))
    }
    expression.BinaryOpExpr(lhs: lhs, operator: operator, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_expression(rhs, state1))
      Ok(#(
        "("
          <> lhs_sql
          <> " "
          <> binary_operator_to_sql(operator)
          <> " "
          <> rhs_sql
          <> ")",
        state2,
      ))
    }
    expression.WindowExpr(function: function, window: window) -> {
      use #(function_sql, state1) <- result_try(compile_expression(
        function,
        state,
      ))
      use #(window_sql, state2) <- result_try(compile_window_definition(
        window,
        state1,
      ))
      Ok(#(function_sql <> " OVER (" <> window_sql <> ")", state2))
    }
    expression.SubqueryExpr(select_query) -> {
      use #(subquery_sql, next_state) <- result_try(compile_select_query(
        select_query,
        state,
      ))
      Ok(#("(" <> subquery_sql <> ")", next_state))
    }
    expression.ValueExpr(value) -> {
      let #(placeholder, next_state) = push_param(value, state)
      Ok(#(placeholder, next_state))
    }
  }
}

fn compile_window_definition(
  window: expression.WindowDefinition,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let expression.WindowDefinition(
    partition_by: partition_by,
    order_by: order_by,
  ) = window

  use #(partition_sql, state1) <- result_try(compile_window_partition(
    partition_by,
    state,
  ))
  use #(order_sql, state2) <- result_try(compile_window_order(order_by, state1))

  case partition_sql, order_sql {
    "", "" -> Ok(#("", state2))
    "", _ -> Ok(#(order_sql, state2))
    _, "" -> Ok(#(partition_sql, state2))
    _, _ -> Ok(#(partition_sql <> " " <> order_sql, state2))
  }
}

fn compile_window_partition(
  partition_by: List(expression.Expression),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case partition_by {
    [] -> Ok(#("", state))
    _ -> {
      use #(partition_sql, next_state) <- result_try(compile_expression_list(
        partition_by,
        state,
      ))
      Ok(#("PARTITION BY " <> partition_sql, next_state))
    }
  }
}

fn compile_window_order(
  order_by: List(expression.Order),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case order_by {
    [] -> Ok(#("", state))
    _ -> {
      use #(order_sql, next_state) <- result_try(compile_order_items(
        order_by,
        state,
      ))
      Ok(#("ORDER BY " <> order_sql, next_state))
    }
  }
}

fn push_param(
  value: expression.SqlValue,
  state: CompileState,
) -> #(String, CompileState) {
  let placeholder = "$" <> int.to_string(state.next_param)
  let next_state =
    CompileState(
      next_param: state.next_param + 1,
      params: [value, ..state.params],
      config: state.config,
    )
  #(placeholder, next_state)
}

fn compile_source(
  source: expression.Source,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case source {
    expression.TableSource(table) ->
      Ok(#(compile_table_ref(table, state.config), state))
    expression.DerivedSource(query: derived_query, alias: alias) -> {
      use #(query_sql, next_state) <- result_try(compile_select_query(
        derived_query,
        state,
      ))
      Ok(#(
        "(" <> query_sql <> ") AS " <> compile_identifier(alias, state.config),
        next_state,
      ))
    }
  }
}

fn compile_joins(
  joins: List(expression.Join),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  compile_joins_loop(joins, [], state)
}

fn compile_joins_loop(
  joins: List(expression.Join),
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

fn compile_join(
  j: expression.Join,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let expression.Join(kind: kind, source: source, on: on) = j
  let kind_sql = case kind {
    expression.InnerJoin -> " INNER JOIN "
    expression.LeftJoin -> " LEFT JOIN "
  }

  use #(source_sql, state1) <- result_try(compile_source(source, state))
  use #(on_sql, state2) <- result_try(compile_predicate(on, state1))
  Ok(#(kind_sql <> source_sql <> " ON " <> on_sql, state2))
}

fn compile_where(
  where_: option.Option(expression.Predicate),
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

fn compile_group_by(
  group_by: List(expression.Expression),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case group_by {
    [] -> Ok(#("", state))
    _ -> {
      use #(items_sql, next_state) <- result_try(compile_expression_list(
        group_by,
        state,
      ))
      Ok(#(" GROUP BY " <> items_sql, next_state))
    }
  }
}

fn compile_having(
  group_by: List(expression.Expression),
  having_: option.Option(expression.Predicate),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case having_ {
    option.None -> Ok(#("", state))
    option.Some(pred) -> {
      case list.is_empty(group_by) {
        True -> Error(HavingWithoutGroupBy)
        False -> {
          use #(pred_sql, next_state) <- result_try(compile_predicate(
            pred,
            state,
          ))
          Ok(#(" HAVING " <> pred_sql, next_state))
        }
      }
    }
  }
}

fn compile_order_by(
  order_by: List(expression.Order),
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

fn compile_order_items(
  items: List(expression.Order),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  compile_order_items_loop(items, [], state)
}

fn compile_order_items_loop(
  items: List(expression.Order),
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

fn compile_order_item(
  item: expression.Order,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  let expression.Order(expression: expr, direction: direction) = item
  use #(expr_sql, state1) <- result_try(compile_expression(expr, state))
  let dir_sql = case direction {
    expression.Asc -> "ASC"
    expression.Desc -> "DESC"
  }
  Ok(#(expr_sql <> " " <> dir_sql, state1))
}

fn compile_predicate(
  pred: expression.Predicate,
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  case pred {
    expression.Comparison(lhs: lhs, op: op, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_expression(rhs, state1))
      Ok(#(
        "(" <> lhs_sql <> " " <> op_to_sql(op) <> " " <> rhs_sql <> ")",
        state2,
      ))
    }

    expression.And(left: left, right: right) -> {
      use #(left_sql, state1) <- result_try(compile_predicate(left, state))
      use #(right_sql, state2) <- result_try(compile_predicate(right, state1))
      Ok(#("(" <> left_sql <> " AND " <> right_sql <> ")", state2))
    }

    expression.Or(left: left, right: right) -> {
      use #(left_sql, state1) <- result_try(compile_predicate(left, state))
      use #(right_sql, state2) <- result_try(compile_predicate(right, state1))
      Ok(#("(" <> left_sql <> " OR " <> right_sql <> ")", state2))
    }

    expression.Not(predicate: inner) -> {
      use #(inner_sql, state1) <- result_try(compile_predicate(inner, state))
      Ok(#("(NOT " <> inner_sql <> ")", state1))
    }

    expression.InList(lhs: lhs, rhs: rhs) -> {
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

    expression.InSubquery(lhs: lhs, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_select_query(rhs, state1))
      Ok(#("(" <> lhs_sql <> " IN (" <> rhs_sql <> "))", state2))
    }

    expression.IsNull(expression: expr) -> {
      use #(expr_sql, state1) <- result_try(compile_expression(expr, state))
      Ok(#("(" <> expr_sql <> " IS NULL)", state1))
    }

    expression.IsNotNull(expression: expr) -> {
      use #(expr_sql, state1) <- result_try(compile_expression(expr, state))
      Ok(#("(" <> expr_sql <> " IS NOT NULL)", state1))
    }

    expression.Like(lhs: lhs, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_expression(rhs, state1))
      Ok(#("(" <> lhs_sql <> " LIKE " <> rhs_sql <> ")", state2))
    }

    expression.Ilike(lhs: lhs, rhs: rhs) -> {
      use #(lhs_sql, state1) <- result_try(compile_expression(lhs, state))
      use #(rhs_sql, state2) <- result_try(compile_expression(rhs, state1))
      Ok(#("(" <> lhs_sql <> " ILIKE " <> rhs_sql <> ")", state2))
    }
  }
}

fn compile_expression_list(
  exprs: List(expression.Expression),
  state: CompileState,
) -> Result(#(String, CompileState), CompileError) {
  compile_expression_list_loop(exprs, [], state)
}

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

fn compile_table_ref(table: schema.Table, config: CompilerConfig) -> String {
  let schema.Table(schema: schema_name, name: name, alias: alias) = table
  let qualified_name = case schema_name {
    option.None -> compile_identifier(name, config)
    option.Some(schema_name) ->
      compile_identifier(schema_name, config)
      <> "."
      <> compile_identifier(name, config)
  }

  case alias {
    option.None -> qualified_name
    option.Some(a) -> qualified_name <> " AS " <> compile_identifier(a, config)
  }
}

fn compile_column_ref(
  column: schema.ColumnMeta,
  config: CompilerConfig,
) -> String {
  let schema.ColumnMeta(table: table, name: column_name) = column
  let schema.Table(schema: schema_name, name: table_name, alias: alias) = table
  let qualifier = case alias {
    option.None -> {
      case schema_name {
        option.None -> compile_identifier(table_name, config)
        option.Some(schema_name) ->
          compile_identifier(schema_name, config)
          <> "."
          <> compile_identifier(table_name, config)
      }
    }
    option.Some(a) -> compile_identifier(a, config)
  }
  qualifier <> "." <> compile_identifier(column_name, config)
}

fn compile_column_name(
  column: schema.ColumnMeta,
  config: CompilerConfig,
) -> String {
  let schema.ColumnMeta(table: _, name: name) = column
  compile_identifier(name, config)
}

fn compile_identifier(identifier: String, config: CompilerConfig) -> String {
  let CompilerConfig(
    render_identifier: render_identifier,
    validate_function_name: _,
  ) = config
  render_identifier(identifier)
}

fn compile_function_name(
  name: String,
  config: CompilerConfig,
) -> Result(String, CompileError) {
  let CompilerConfig(
    render_identifier: _,
    validate_function_name: validate_function_name,
  ) = config
  validate_function_name(name)
}

fn default_render_identifier(identifier: String) -> String {
  "\"" <> string.replace(in: identifier, each: "\"", with: "\"\"") <> "\""
}

fn default_validate_function_name(name: String) -> Result(String, CompileError) {
  case string.is_empty(name) {
    True -> Error(InvalidFunctionName(name))
    False -> Ok(name)
  }
}

fn op_to_sql(op: expression.ComparisonOp) -> String {
  case op {
    expression.Eq -> "="
    expression.Neq -> "!="
    expression.Gt -> ">"
    expression.Gte -> ">="
    expression.Lt -> "<"
    expression.Lte -> "<="
  }
}

fn unary_operator_to_sql(operator: expression.UnaryOperator) -> String {
  case operator {
    expression.Negate -> "-"
  }
}

fn binary_operator_to_sql(operator: expression.BinaryOperator) -> String {
  case operator {
    expression.Add -> "+"
    expression.Subtract -> "-"
    expression.Multiply -> "*"
    expression.Divide -> "/"
    expression.Concat -> "||"
  }
}

fn join_strings(parts: List(String), sep: String) -> String {
  case parts {
    [] -> ""
    [first, ..rest] -> join_strings_loop(rest, first, sep)
  }
}

fn join_strings_loop(parts: List(String), acc: String, sep: String) -> String {
  case parts {
    [] -> acc
    [part, ..rest] -> join_strings_loop(rest, acc <> sep <> part, sep)
  }
}

fn concat_strings(parts: List(String)) -> String {
  case parts {
    [] -> ""
    [p, ..rest] -> p <> concat_strings(rest)
  }
}

fn reverse(items: List(a)) -> List(a) {
  reverse_loop(items, [])
}

fn reverse_loop(items: List(a), acc: List(a)) -> List(a) {
  case items {
    [] -> acc
    [x, ..xs] -> reverse_loop(xs, [x, ..acc])
  }
}

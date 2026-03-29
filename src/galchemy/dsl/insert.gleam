import galchemy/ast/expression
import galchemy/ast/query
import galchemy/ast/schema
import gleam/list

// Creates an empty INSERT query for the target table.
pub fn insert_into(table: schema.Table) -> query.InsertQuery {
  query.InsertQuery(table: table, rows: [], returning: [])
}

// Creates a single column-value pair for insert builders.
pub fn field(
  column: schema.Column(a),
  expr: expression.Expression,
) -> #(schema.ColumnMeta, expression.Expression) {
  #(column.meta, expr)
}

// Appends a single column value assignment to the INSERT query.
pub fn value(
  query: query.InsertQuery,
  column: schema.Column(a),
  expr: expression.Expression,
) -> query.InsertQuery {
  append_field(query, field(column, expr))
}

// Appends a full row to the INSERT query.
pub fn row(
  query: query.InsertQuery,
  values: List(#(schema.ColumnMeta, expression.Expression)),
) -> query.InsertQuery {
  query.InsertQuery(..query, rows: list.append(query.rows, [values]))
}

// Replaces the INSERT rows with a prepared batch of row values.
pub fn values(
  query: query.InsertQuery,
  rows: List(List(#(schema.ColumnMeta, expression.Expression))),
) -> query.InsertQuery {
  query.InsertQuery(..query, rows: rows)
}

// Sets the RETURNING clause for the INSERT query.
pub fn returning(
  query: query.InsertQuery,
  items: List(expression.SelectItem),
) -> query.InsertQuery {
  query.InsertQuery(..query, returning: items)
}

fn append_field(
  query: query.InsertQuery,
  pair: #(schema.ColumnMeta, expression.Expression),
) -> query.InsertQuery {
  query.InsertQuery(..query, rows: append_field_to_last_row(query.rows, pair))
}

fn append_field_to_last_row(
  rows: List(List(#(schema.ColumnMeta, expression.Expression))),
  pair: #(schema.ColumnMeta, expression.Expression),
) -> List(List(#(schema.ColumnMeta, expression.Expression))) {
  case rows {
    [] -> [[pair]]
    [last_row] -> [list.append(last_row, [pair])]
    [first, ..rest] -> [first, ..append_field_to_last_row(rest, pair)]
  }
}

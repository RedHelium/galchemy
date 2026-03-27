import galchemy/ast/expression
import galchemy/ast/query
import galchemy/ast/schema
import gleam/list

// Creates an empty INSERT query for the target table.
pub fn insert_into(table: schema.Table) -> query.InsertQuery {
  query.InsertQuery(table: table, values: [], returning: [])
}

// Appends a single column value assignment to the INSERT query.
pub fn value(
  query: query.InsertQuery,
  column: schema.Column(a),
  expr: expression.Expression,
) -> query.InsertQuery {
  query.InsertQuery(
    ..query,
    values: list.append(query.values, [#(column.meta, expr)]),
  )
}

// Replaces the INSERT values with a prepared list of assignments.
pub fn values(
  query: query.InsertQuery,
  pairs: List(#(schema.ColumnMeta, expression.Expression)),
) -> query.InsertQuery {
  query.InsertQuery(..query, values: pairs)
}

// Sets the RETURNING clause for the INSERT query.
pub fn returning(
  query: query.InsertQuery,
  items: List(expression.SelectItem),
) -> query.InsertQuery {
  query.InsertQuery(..query, returning: items)
}

import galchemy/ast/expression
import galchemy/ast/predicate
import galchemy/ast/query
import galchemy/ast/schema
import gleam/list
import gleam/option

// Creates an empty UPDATE query for the target table.
pub fn update(table: schema.Table) -> query.UpdateQuery {
  query.UpdateQuery(
    table: table,
    assignments: [],
    where_: option.None,
    returning: [],
  )
}

// Appends a single column assignment to the UPDATE query.
pub fn set(
  query: query.UpdateQuery,
  column: schema.Column(a),
  expr: expression.Expression,
) -> query.UpdateQuery {
  query.UpdateQuery(
    ..query,
    assignments: list.append(query.assignments, [#(column.meta, expr)]),
  )
}

// Sets the WHERE clause for the UPDATE query.
pub fn where_(
  query: query.UpdateQuery,
  predicate: predicate.Predicate,
) -> query.UpdateQuery {
  query.UpdateQuery(..query, where_: option.Some(predicate))
}

// Sets the RETURNING clause for the UPDATE query.
pub fn returning(
  query: query.UpdateQuery,
  items: List(expression.SelectItem),
) -> query.UpdateQuery {
  query.UpdateQuery(..query, returning: items)
}

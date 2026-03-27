import galchemy/ast/expression
import galchemy/ast/predicate
import galchemy/ast/query
import galchemy/ast/schema
import gleam/option

// Creates an empty DELETE query for the target table.
pub fn delete_from(table: schema.Table) -> query.DeleteQuery {
  query.DeleteQuery(table: table, where_: option.None, returning: [])
}

// Sets the WHERE clause for the DELETE query.
pub fn where_(
  query: query.DeleteQuery,
  predicate: predicate.Predicate,
) -> query.DeleteQuery {
  query.DeleteQuery(..query, where_: option.Some(predicate))
}

// Sets the RETURNING clause for the DELETE query.
pub fn returning(
  query: query.DeleteQuery,
  items: List(expression.SelectItem),
) -> query.DeleteQuery {
  query.DeleteQuery(..query, returning: items)
}

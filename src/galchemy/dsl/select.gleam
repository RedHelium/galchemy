import galchemy/ast/expression
import galchemy/ast/join
import galchemy/ast/order
import galchemy/ast/predicate
import galchemy/ast/query
import galchemy/ast/schema
import gleam/list
import gleam/option

// Creates an empty SELECT query with the provided select items.
pub fn select(items: List(expression.SelectItem)) -> query.SelectQuery {
  query.SelectQuery(
    items: items,
    from: option.None,
    joins: [],
    where_: option.None,
    order_by: [],
    limit: option.None,
    offset: option.None,
    distinct: False,
  )
}

// Sets the source table for the SELECT query.
pub fn from(query: query.SelectQuery, table: schema.Table) -> query.SelectQuery {
  query.SelectQuery(..query, from: option.Some(table))
}

// Appends an INNER JOIN clause to the SELECT query.
pub fn inner_join(
  query: query.SelectQuery,
  table: schema.Table,
  on: predicate.Predicate,
) -> query.SelectQuery {
  let join = join.Join(kind: join.InnerJoin, table: table, on: on)
  query.SelectQuery(..query, joins: list.append(query.joins, [join]))
}

// Appends a LEFT JOIN clause to the SELECT query.
pub fn left_join(
  query: query.SelectQuery,
  table: schema.Table,
  on: predicate.Predicate,
) -> query.SelectQuery {
  let join = join.Join(kind: join.LeftJoin, table: table, on: on)
  query.SelectQuery(..query, joins: list.append(query.joins, [join]))
}

// Sets the WHERE clause for the SELECT query.
pub fn where_(
  query: query.SelectQuery,
  predicate: predicate.Predicate,
) -> query.SelectQuery {
  query.SelectQuery(..query, where_: option.Some(predicate))
}

// Creates an ascending ORDER BY item.
pub fn asc(expr: expression.Expression) -> order.Order {
  order.Order(expression: expr, direction: order.Asc)
}

// Creates a descending ORDER BY item.
pub fn desc(expr: expression.Expression) -> order.Order {
  order.Order(expression: expr, direction: order.Desc)
}

// Appends an ORDER BY item to the SELECT query.
pub fn order_by(
  query: query.SelectQuery,
  item: order.Order,
) -> query.SelectQuery {
  query.SelectQuery(..query, order_by: list.append(query.order_by, [item]))
}

// Sets the LIMIT value for the SELECT query.
pub fn limit(query: query.SelectQuery, value: Int) -> query.SelectQuery {
  query.SelectQuery(..query, limit: option.Some(value))
}

// Sets the OFFSET value for the SELECT query.
pub fn offset(query: query.SelectQuery, value: Int) -> query.SelectQuery {
  query.SelectQuery(..query, offset: option.Some(value))
}

// Marks the SELECT query as DISTINCT.
pub fn distinct(query: query.SelectQuery) -> query.SelectQuery {
  query.SelectQuery(..query, distinct: True)
}

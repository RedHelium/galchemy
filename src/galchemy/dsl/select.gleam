import galchemy/ast/expression
import galchemy/ast/schema
import gleam/list
import gleam/option

pub fn select(
  items: List(expression.SelectItem),
) -> expression.SelectQuery {
  expression.SelectQuery(
    ctes: [],
    items: items,
    from: option.None,
    joins: [],
    where_: option.None,
    group_by: [],
    having_: option.None,
    unions: [],
    order_by: [],
    limit: option.None,
    offset: option.None,
    distinct: False,
  )
}

pub fn from(
  query: expression.SelectQuery,
  table: schema.Table,
) -> expression.SelectQuery {
  expression.SelectQuery(..query, from: option.Some(expression.TableSource(table)))
}

pub fn from_derived(
  query: expression.SelectQuery,
  derived_query: expression.SelectQuery,
  alias: String,
) -> expression.SelectQuery {
  expression.SelectQuery(
    ..query,
    from: option.Some(expression.DerivedSource(query: derived_query, alias: alias)),
  )
}

pub fn inner_join(
  query: expression.SelectQuery,
  table: schema.Table,
  on: expression.Predicate,
) -> expression.SelectQuery {
  let next_join =
    expression.Join(
      kind: expression.InnerJoin,
      source: expression.TableSource(table),
      on: on,
    )

  expression.SelectQuery(..query, joins: list.append(query.joins, [next_join]))
}

pub fn inner_join_derived(
  query: expression.SelectQuery,
  derived_query: expression.SelectQuery,
  alias: String,
  on: expression.Predicate,
) -> expression.SelectQuery {
  let next_join =
    expression.Join(
      kind: expression.InnerJoin,
      source: expression.DerivedSource(query: derived_query, alias: alias),
      on: on,
    )

  expression.SelectQuery(..query, joins: list.append(query.joins, [next_join]))
}

pub fn left_join(
  query: expression.SelectQuery,
  table: schema.Table,
  on: expression.Predicate,
) -> expression.SelectQuery {
  let next_join =
    expression.Join(
      kind: expression.LeftJoin,
      source: expression.TableSource(table),
      on: on,
    )

  expression.SelectQuery(..query, joins: list.append(query.joins, [next_join]))
}

pub fn left_join_derived(
  query: expression.SelectQuery,
  derived_query: expression.SelectQuery,
  alias: String,
  on: expression.Predicate,
) -> expression.SelectQuery {
  let next_join =
    expression.Join(
      kind: expression.LeftJoin,
      source: expression.DerivedSource(query: derived_query, alias: alias),
      on: on,
    )

  expression.SelectQuery(..query, joins: list.append(query.joins, [next_join]))
}

pub fn where_(
  query: expression.SelectQuery,
  predicate: expression.Predicate,
) -> expression.SelectQuery {
  expression.SelectQuery(..query, where_: option.Some(predicate))
}

pub fn with_cte(
  query: expression.SelectQuery,
  name: String,
  cte_query: expression.SelectQuery,
) -> expression.SelectQuery {
  let cte = expression.Cte(name: name, query: cte_query)
  expression.SelectQuery(..query, ctes: list.append(query.ctes, [cte]))
}

pub fn union(
  query: expression.SelectQuery,
  other: expression.SelectQuery,
) -> expression.SelectQuery {
  let operation = expression.SetOperation(kind: expression.Union, query: other)
  expression.SelectQuery(..query, unions: list.append(query.unions, [operation]))
}

pub fn union_all(
  query: expression.SelectQuery,
  other: expression.SelectQuery,
) -> expression.SelectQuery {
  let operation =
    expression.SetOperation(kind: expression.UnionAll, query: other)
  expression.SelectQuery(..query, unions: list.append(query.unions, [operation]))
}

pub fn group_by(
  query: expression.SelectQuery,
  expr: expression.Expression,
) -> expression.SelectQuery {
  expression.SelectQuery(..query, group_by: list.append(query.group_by, [expr]))
}

pub fn having(
  query: expression.SelectQuery,
  predicate: expression.Predicate,
) -> expression.SelectQuery {
  expression.SelectQuery(..query, having_: option.Some(predicate))
}

pub fn asc(expr: expression.Expression) -> expression.Order {
  expression.Order(expression: expr, direction: expression.Asc)
}

pub fn desc(expr: expression.Expression) -> expression.Order {
  expression.Order(expression: expr, direction: expression.Desc)
}

pub fn order_by(
  query: expression.SelectQuery,
  item: expression.Order,
) -> expression.SelectQuery {
  expression.SelectQuery(
    ..query,
    order_by: list.append(query.order_by, [item]),
  )
}

pub fn limit(query: expression.SelectQuery, value: Int) -> expression.SelectQuery {
  expression.SelectQuery(..query, limit: option.Some(value))
}

pub fn offset(
  query: expression.SelectQuery,
  value: Int,
) -> expression.SelectQuery {
  expression.SelectQuery(..query, offset: option.Some(value))
}

pub fn distinct(query: expression.SelectQuery) -> expression.SelectQuery {
  expression.SelectQuery(..query, distinct: True)
}

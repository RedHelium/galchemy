import galchemy/ast/expression

pub fn eq(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Comparison(lhs: lhs, rhs: rhs, op: expression.Eq)
}

pub fn neq(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Comparison(lhs: lhs, rhs: rhs, op: expression.Neq)
}

pub fn gt(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Comparison(lhs: lhs, rhs: rhs, op: expression.Gt)
}

pub fn gte(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Comparison(lhs: lhs, rhs: rhs, op: expression.Gte)
}

pub fn lt(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Comparison(lhs: lhs, rhs: rhs, op: expression.Lt)
}

pub fn lte(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Comparison(lhs: lhs, rhs: rhs, op: expression.Lte)
}

pub fn and(
  left: expression.Predicate,
  right: expression.Predicate,
) -> expression.Predicate {
  expression.And(left: left, right: right)
}

pub fn or(
  left: expression.Predicate,
  right: expression.Predicate,
) -> expression.Predicate {
  expression.Or(left: left, right: right)
}

pub fn not(value: expression.Predicate) -> expression.Predicate {
  expression.Not(value)
}

pub fn in_list(
  lhs: expression.Expression,
  rhs: List(expression.Expression),
) -> expression.Predicate {
  expression.InList(lhs: lhs, rhs: rhs)
}

pub fn in_subquery(
  lhs: expression.Expression,
  rhs: expression.SelectQuery,
) -> expression.Predicate {
  expression.InSubquery(lhs: lhs, rhs: rhs)
}

pub fn is_null(expr: expression.Expression) -> expression.Predicate {
  expression.IsNull(expression: expr)
}

pub fn is_not_null(expr: expression.Expression) -> expression.Predicate {
  expression.IsNotNull(expression: expr)
}

pub fn like(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Like(lhs: lhs, rhs: rhs)
}

pub fn ilike(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Predicate {
  expression.Ilike(lhs: lhs, rhs: rhs)
}

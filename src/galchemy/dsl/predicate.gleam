import galchemy/ast/expression
import galchemy/ast/predicate

// Builds an equality comparison predicate.
pub fn eq(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Comparison(lhs: lhs, rhs: rhs, op: predicate.Eq)
}

// Builds a non-equality comparison predicate.
pub fn neq(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Comparison(lhs: lhs, rhs: rhs, op: predicate.Neq)
}

// Builds a greater-than comparison predicate.
pub fn gt(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Comparison(lhs: lhs, rhs: rhs, op: predicate.Gt)
}

// Builds a greater-than-or-equal comparison predicate.
pub fn gte(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Comparison(lhs: lhs, rhs: rhs, op: predicate.Gte)
}

// Builds a less-than comparison predicate.
pub fn lt(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Comparison(lhs: lhs, rhs: rhs, op: predicate.Lt)
}

// Builds a less-than-or-equal comparison predicate.
pub fn lte(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Comparison(lhs: lhs, rhs: rhs, op: predicate.Lte)
}

// Combines two predicates with logical AND.
pub fn and(
  left: predicate.Predicate,
  right: predicate.Predicate,
) -> predicate.Predicate {
  predicate.And(left: left, right: right)
}

// Combines two predicates with logical OR.
pub fn or(
  left: predicate.Predicate,
  right: predicate.Predicate,
) -> predicate.Predicate {
  predicate.Or(left: left, right: right)
}

// Negates a predicate.
pub fn not(value: predicate.Predicate) -> predicate.Predicate {
  predicate.Not(value)
}

// Builds an IN predicate from an expression and a list of expressions.
pub fn in_list(
  lhs: expression.Expression,
  rhs: List(expression.Expression),
) -> predicate.Predicate {
  predicate.InList(lhs: lhs, rhs: rhs)
}

// Builds an IS NULL predicate.
pub fn is_null(expr: expression.Expression) -> predicate.Predicate {
  predicate.IsNull(expression: expr)
}

// Builds an IS NOT NULL predicate.
pub fn is_not_null(expr: expression.Expression) -> predicate.Predicate {
  predicate.IsNotNull(expression: expr)
}

// Builds a LIKE predicate.
pub fn like(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Like(lhs: lhs, rhs: rhs)
}

// Builds an ILIKE predicate.
pub fn ilike(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> predicate.Predicate {
  predicate.Ilike(lhs: lhs, rhs: rhs)
}

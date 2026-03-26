import galchemy/ast/expression.{type Expression}

pub type ComparisonOp {
  Eq
  Neq
  Gt
  Gte
  Lt
  Lte
}

pub type Predicate {
  Comparison(lhs: Expression, op: ComparisonOp, rhs: Expression)
  And(left: Predicate, right: Predicate)
  Or(left: Predicate, right: Predicate)
  Not(predicate: Predicate)
  InList(lhs: Expression, rhs: List(Expression))
  IsNull(expression: Expression)
  IsNotNull(expression: Expression)
  Like(lhs: Expression, rhs: Expression)
  Ilike(lhs: Expression, rhs: Expression)
}

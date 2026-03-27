import galchemy/ast/expression.{type Expression}

/// Represents an ORDER BY item with its expression and direction.
pub type Order {
  Order(expression: Expression, direction: OrderDirection)
}

/// Defines the available sort directions for ORDER BY clauses.
pub type OrderDirection {
  Asc
  Desc
}

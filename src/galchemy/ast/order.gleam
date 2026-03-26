import galchemy/ast/expression.{type Expression}

pub type Order {
  Order(expression: Expression, direction: OrderDirection)
}

pub type OrderDirection {
  Asc
  Desc
}

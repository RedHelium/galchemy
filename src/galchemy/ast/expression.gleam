import galchemy/ast/schema.{type ColumnMeta}
import gleam/option.{type Option}

/// Represents a projected expression in a SELECT list, optionally aliased.
pub type SelectItem {
  SelectItem(expression: Expression, alias: Option(String))
}

/// Represents a literal SQL value that can be embedded into an expression.
pub type SqlValue {
  Text(String)
  Int(Int)
  Bool(Bool)
  Null
}

/// Represents the minimal expression forms supported by the query AST.
pub type Expression {
  ColumnExpr(ColumnMeta)
  ValueExpr(SqlValue)
}

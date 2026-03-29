import galchemy/ast/schema.{type ColumnMeta}
import gleam/option.{type Option}
import gleam/time/calendar.{type Date, type TimeOfDay}
import gleam/time/timestamp.{type Timestamp}

/// Represents a projected expression in a SELECT list, optionally aliased.
pub type SelectItem {
  SelectItem(expression: Expression, alias: Option(String))
}

/// Represents a literal SQL value that can be embedded into an expression.
pub type SqlValue {
  Text(String)
  Int(Int)
  Float(Float)
  Bool(Bool)
  Timestamp(Timestamp)
  Date(Date)
  TimeOfDay(TimeOfDay)
  Null
}

/// Represents the minimal expression forms supported by the query AST.
pub type Expression {
  ColumnExpr(ColumnMeta)
  ValueExpr(SqlValue)
}

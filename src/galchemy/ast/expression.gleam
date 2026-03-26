import galchemy/ast/schema.{type ColumnMeta}
import gleam/option.{type Option}

pub type SelectItem {
  SelectItem(expression: Expression, alias: Option(String))
}

pub type SqlValue {
  Text(String)
  Int(Int)
  Bool(Bool)
  Null
}

pub type Expression {
  ColumnExpr(ColumnMeta)
  ValueExpr(SqlValue)
}

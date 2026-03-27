import galchemy/ast/expression
import galchemy/ast/schema
import gleam/option

// Converts a typed schema column into a generic SQL expression.
pub fn col(column: schema.Column(a)) -> expression.Expression {
  expression.ColumnExpr(column.meta)
}

// Wraps an integer literal as a SQL expression.
pub fn int(value: Int) -> expression.Expression {
  expression.ValueExpr(expression.Int(value))
}

// Wraps a text literal as a SQL expression.
pub fn text(value: String) -> expression.Expression {
  expression.ValueExpr(expression.Text(value))
}

// Wraps a boolean literal as a SQL expression.
pub fn bool(value: Bool) -> expression.Expression {
  expression.ValueExpr(expression.Bool(value))
}

// Creates a SQL NULL expression.
pub fn null() -> expression.Expression {
  expression.ValueExpr(expression.Null)
}

// Turns an expression into a select item without an alias.
pub fn item(expr: expression.Expression) -> expression.SelectItem {
  expression.SelectItem(expression: expr, alias: option.None)
}

// Turns an expression into a select item with an alias.
pub fn as_(expr: expression.Expression, alias: String) -> expression.SelectItem {
  expression.SelectItem(expression: expr, alias: option.Some(alias))
}

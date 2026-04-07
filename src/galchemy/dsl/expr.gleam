import galchemy/ast/expression
import galchemy/ast/schema
import gleam/option
import gleam/time/calendar.{type Date, type TimeOfDay}
import gleam/time/timestamp.{type Timestamp}

// Converts a typed schema column into a generic SQL expression.
pub fn col(column: schema.Column(a)) -> expression.Expression {
  expression.ColumnExpr(column.meta)
}

pub fn star() -> expression.Expression {
  expression.StarExpr
}

pub fn call(
  name: String,
  arguments: List(expression.Expression),
) -> expression.Expression {
  expression.FunctionExpr(name: name, arguments: arguments)
}

pub fn neg(value: expression.Expression) -> expression.Expression {
  expression.UnaryOpExpr(operator: expression.Negate, operand: value)
}

pub fn add(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Expression {
  expression.BinaryOpExpr(lhs: lhs, operator: expression.Add, rhs: rhs)
}

pub fn subtract(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Expression {
  expression.BinaryOpExpr(lhs: lhs, operator: expression.Subtract, rhs: rhs)
}

pub fn multiply(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Expression {
  expression.BinaryOpExpr(lhs: lhs, operator: expression.Multiply, rhs: rhs)
}

pub fn divide(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Expression {
  expression.BinaryOpExpr(lhs: lhs, operator: expression.Divide, rhs: rhs)
}

pub fn concat(
  lhs: expression.Expression,
  rhs: expression.Expression,
) -> expression.Expression {
  expression.BinaryOpExpr(lhs: lhs, operator: expression.Concat, rhs: rhs)
}

pub fn over(
  function: expression.Expression,
  partition_by: List(expression.Expression),
  order_by: List(expression.Order),
) -> expression.Expression {
  expression.WindowExpr(
    function: function,
    window: expression.WindowDefinition(
      partition_by: partition_by,
      order_by: order_by,
    ),
  )
}

pub fn subquery(query: expression.SelectQuery) -> expression.Expression {
  expression.SubqueryExpr(query)
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

// Wraps a float literal as a SQL expression.
pub fn float(value: Float) -> expression.Expression {
  expression.ValueExpr(expression.Float(value))
}

pub fn bytea(value: BitArray) -> expression.Expression {
  expression.ValueExpr(expression.Bytea(value))
}

pub fn uuid(value: String) -> expression.Expression {
  expression.ValueExpr(expression.Uuid(value))
}

pub fn numeric(value: String) -> expression.Expression {
  expression.ValueExpr(expression.Numeric(value))
}

pub fn json(value: String) -> expression.Expression {
  expression.ValueExpr(expression.Json(value))
}

pub fn jsonb(value: String) -> expression.Expression {
  expression.ValueExpr(expression.Jsonb(value))
}

pub fn enum_(type_name: String, value: String) -> expression.Expression {
  expression.ValueExpr(expression.Enum(type_name: type_name, value: value))
}

pub fn array(values: List(expression.SqlValue)) -> expression.Expression {
  expression.ValueExpr(expression.Array(values))
}

// Wraps a timestamp literal as a SQL expression.
pub fn timestamp(value: Timestamp) -> expression.Expression {
  expression.ValueExpr(expression.Timestamp(value))
}

// Wraps a calendar date literal as a SQL expression.
pub fn date(value: Date) -> expression.Expression {
  expression.ValueExpr(expression.Date(value))
}

// Wraps a time-of-day literal as a SQL expression.
pub fn time_of_day(value: TimeOfDay) -> expression.Expression {
  expression.ValueExpr(expression.TimeOfDay(value))
}

pub fn count(value: expression.Expression) -> expression.Expression {
  call("COUNT", [value])
}

pub fn count_all() -> expression.Expression {
  call("COUNT", [star()])
}

pub fn sum(value: expression.Expression) -> expression.Expression {
  call("SUM", [value])
}

pub fn avg(value: expression.Expression) -> expression.Expression {
  call("AVG", [value])
}

pub fn min(value: expression.Expression) -> expression.Expression {
  call("MIN", [value])
}

pub fn max(value: expression.Expression) -> expression.Expression {
  call("MAX", [value])
}

pub fn row_number() -> expression.Expression {
  call("ROW_NUMBER", [])
}

pub fn rank() -> expression.Expression {
  call("RANK", [])
}

pub fn dense_rank() -> expression.Expression {
  call("DENSE_RANK", [])
}

pub fn lower(value: expression.Expression) -> expression.Expression {
  call("LOWER", [value])
}

pub fn upper(value: expression.Expression) -> expression.Expression {
  call("UPPER", [value])
}

pub fn coalesce(arguments: List(expression.Expression)) -> expression.Expression {
  call("COALESCE", arguments)
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

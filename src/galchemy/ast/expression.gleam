import galchemy/ast/schema.{type ColumnMeta, type Table}
import gleam/option.{type Option}
import gleam/time/calendar.{type Date, type TimeOfDay}
import gleam/time/timestamp.{type Timestamp}

pub type SelectItem {
  SelectItem(expression: Expression, alias: Option(String))
}

pub type SqlValue {
  Text(String)
  Int(Int)
  Float(Float)
  Bool(Bool)
  Bytea(BitArray)
  Uuid(String)
  Numeric(String)
  Json(String)
  Jsonb(String)
  Enum(type_name: String, value: String)
  Array(List(SqlValue))
  Timestamp(Timestamp)
  Date(Date)
  TimeOfDay(TimeOfDay)
  Null
}

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
  InSubquery(lhs: Expression, rhs: SelectQuery)
  IsNull(expression: Expression)
  IsNotNull(expression: Expression)
  Like(lhs: Expression, rhs: Expression)
  Ilike(lhs: Expression, rhs: Expression)
}

pub type OrderDirection {
  Asc
  Desc
}

pub type Order {
  Order(expression: Expression, direction: OrderDirection)
}

pub type UnaryOperator {
  Negate
}

pub type BinaryOperator {
  Add
  Subtract
  Multiply
  Divide
  Concat
}

pub type WindowDefinition {
  WindowDefinition(partition_by: List(Expression), order_by: List(Order))
}

pub type JoinKind {
  InnerJoin
  LeftJoin
}

pub type Source {
  TableSource(Table)
  DerivedSource(query: SelectQuery, alias: String)
}

pub type Join {
  Join(kind: JoinKind, source: Source, on: Predicate)
}

pub type SetOperationKind {
  Union
  UnionAll
}

pub type SetOperation {
  SetOperation(kind: SetOperationKind, query: SelectQuery)
}

pub type Cte {
  Cte(name: String, query: SelectQuery)
}

pub type SelectQuery {
  SelectQuery(
    ctes: List(Cte),
    items: List(SelectItem),
    from: Option(Source),
    joins: List(Join),
    where_: Option(Predicate),
    group_by: List(Expression),
    having_: Option(Predicate),
    unions: List(SetOperation),
    order_by: List(Order),
    limit: Option(Int),
    offset: Option(Int),
    distinct: Bool,
  )
}

pub type Expression {
  ColumnExpr(ColumnMeta)
  ValueExpr(SqlValue)
  StarExpr
  FunctionExpr(name: String, arguments: List(Expression))
  UnaryOpExpr(operator: UnaryOperator, operand: Expression)
  BinaryOpExpr(lhs: Expression, operator: BinaryOperator, rhs: Expression)
  WindowExpr(function: Expression, window: WindowDefinition)
  SubqueryExpr(SelectQuery)
}

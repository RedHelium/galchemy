import galchemy/ast/expression.{type Expression, type SelectItem}
import galchemy/ast/join.{type Join}
import galchemy/ast/order.{type Order}
import galchemy/ast/predicate.{type Predicate}
import galchemy/ast/schema.{type ColumnMeta, type Table}
import gleam/option.{type Option}

/// Represents any top-level query variant supported by the AST.
pub type Query {
  Select(SelectQuery)
  Insert(InsertQuery)
  Update(UpdateQuery)
  Delete(DeleteQuery)
}

/// Represents a SELECT query with projection, source, filtering, and pagination.
pub type SelectQuery {
  SelectQuery(
    items: List(SelectItem),
    from: Option(Table),
    joins: List(Join),
    where_: Option(Predicate),
    order_by: List(Order),
    limit: Option(Int),
    offset: Option(Int),
    distinct: Bool,
  )
}

/// Represents an INSERT query with explicit column-value pairs and RETURNING items.
pub type InsertQuery {
  InsertQuery(
    table: Table,
    values: List(#(ColumnMeta, Expression)),
    returning: List(SelectItem),
  )
}

/// Represents a DELETE query with an optional filter and RETURNING items.
pub type DeleteQuery {
  DeleteQuery(
    table: Table,
    where_: Option(Predicate),
    returning: List(SelectItem),
  )
}

/// Represents an UPDATE query with assignments, filtering, and RETURNING items.
pub type UpdateQuery {
  UpdateQuery(
    table: Table,
    assignments: List(#(ColumnMeta, Expression)),
    where_: Option(Predicate),
    returning: List(SelectItem),
  )
}

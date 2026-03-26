import galchemy/ast/expression.{type Expression, type SelectItem}
import galchemy/ast/join.{type Join}
import galchemy/ast/order.{type Order}
import galchemy/ast/predicate.{type Predicate}
import galchemy/ast/schema.{type ColumnMeta, type Table}
import gleam/option.{type Option}

pub type Query {
  Select(SelectQuery)
  Insert(InsertQuery)
  Update(UpdateQuery)
  Delete(DeleteQuery)
}

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

pub type InsertQuery {
  InsertQuery(
    table: Table,
    values: List(#(ColumnMeta, Expression)),
    returning: List(SelectItem),
  )
}

pub type DeleteQuery {
  DeleteQuery(
    table: Table,
    where_: Option(Predicate),
    returning: List(SelectItem),
  )
}

pub type UpdateQuery {
  UpdateQuery(
    table: Table,
    assignments: List(#(ColumnMeta, Expression)),
    where_: Option(Predicate),
    returning: List(SelectItem),
  )
}

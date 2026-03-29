import galchemy/ast/expression
import galchemy/ast/schema.{type ColumnMeta, type Table}
import gleam/option.{type Option}

pub type Query {
  Select(expression.SelectQuery)
  Insert(InsertQuery)
  Update(UpdateQuery)
  Delete(DeleteQuery)
}

pub type InsertQuery {
  InsertQuery(
    table: Table,
    rows: List(List(#(ColumnMeta, expression.Expression))),
    returning: List(expression.SelectItem),
  )
}

pub type DeleteQuery {
  DeleteQuery(
    table: Table,
    where_: Option(expression.Predicate),
    returning: List(expression.SelectItem),
  )
}

pub type UpdateQuery {
  UpdateQuery(
    table: Table,
    assignments: List(#(ColumnMeta, expression.Expression)),
    where_: Option(expression.Predicate),
    returning: List(expression.SelectItem),
  )
}

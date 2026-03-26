import galchemy/ast/predicate.{type Predicate}
import galchemy/ast/schema.{type Table}

pub type Join {
  Join(kind: JoinKind, table: Table, on: Predicate)
}

pub type JoinKind {
  InnerJoin
  LeftJoin
}

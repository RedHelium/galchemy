import galchemy/ast/predicate.{type Predicate}
import galchemy/ast/schema.{type Table}

/// Represents a table join with its join kind and join predicate.
pub type Join {
  Join(kind: JoinKind, table: Table, on: Predicate)
}

/// Enumerates the join kinds currently supported by the AST.
pub type JoinKind {
  InnerJoin
  LeftJoin
}

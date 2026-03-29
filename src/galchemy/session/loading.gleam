import galchemy/ast/expression
import galchemy/ast/query
import galchemy/ast/schema
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/unit_of_work
import gleam/list
import gleam/option

pub type LoadError {
  UnknownTable(relation.TableRef)
  UnknownRelation(table: relation.TableRef, relation_name: String)
  EmptyParentIdentities(table: relation.TableRef, relation_name: String)
  MissingIdentityField(
    table: relation.TableRef,
    relation_name: String,
    column: String,
  )
}

pub type EagerLoad {
  EagerLoad(
    relation: relation.Relation,
    related_table: schema.Table,
    join: expression.Join,
  )
}

pub type LazyLoad {
  LazyLoad(relation: relation.Relation, query: query.Query)
}

pub fn eager_join(
  snapshot: model.SchemaSnapshot,
  source_table: schema.Table,
  relation_name: String,
  alias: option.Option(String),
) -> Result(EagerLoad, LoadError) {
  let source_ref = relation_ref_from_table(source_table)
  use next_relation <- result_try(find_relation(
    snapshot,
    source_ref,
    relation_name,
  ))
  let related_table =
    schema.Table(
      schema: option.Some(next_relation.related_table.schema),
      name: next_relation.related_table.name,
      alias: alias,
    )

  Ok(EagerLoad(
    relation: next_relation,
    related_table: related_table,
    join: expression.Join(
      kind: expression.LeftJoin,
      source: expression.TableSource(related_table),
      on: join_predicate(
        source_table,
        related_table,
        next_relation.column_pairs,
      ),
    ),
  ))
}

pub fn apply_eager(
  select_query: expression.SelectQuery,
  snapshot: model.SchemaSnapshot,
  source_table: schema.Table,
  relation_name: String,
  alias: option.Option(String),
) -> Result(expression.SelectQuery, LoadError) {
  use eager <- result_try(eager_join(
    snapshot,
    source_table,
    relation_name,
    alias,
  ))

  Ok(
    expression.SelectQuery(
      ..select_query,
      joins: list.append(select_query.joins, [eager.join]),
    ),
  )
}

pub fn lazy_query(
  snapshot: model.SchemaSnapshot,
  source_table: relation.TableRef,
  relation_name: String,
  identities: List(unit_of_work.Identity),
) -> Result(LazyLoad, LoadError) {
  case identities {
    [] ->
      Error(EmptyParentIdentities(
        table: source_table,
        relation_name: relation_name,
      ))
    _ -> {
      use next_relation <- result_try(find_relation(
        snapshot,
        source_table,
        relation_name,
      ))
      use where_ <- result_try(lazy_predicate(
        source_table,
        relation_name,
        next_relation,
        identities,
      ))

      let related_table =
        schema.Table(
          schema: option.Some(next_relation.related_table.schema),
          name: next_relation.related_table.name,
          alias: option.None,
        )

      Ok(LazyLoad(
        relation: next_relation,
        query: query.Select(expression.SelectQuery(
          ctes: [],
          items: [],
          from: option.Some(expression.TableSource(related_table)),
          joins: [],
          where_: option.Some(where_),
          group_by: [],
          having_: option.None,
          unions: [],
          order_by: [],
          limit: option.None,
          offset: option.None,
          distinct: False,
        )),
      ))
    }
  }
}

fn find_relation(
  snapshot: model.SchemaSnapshot,
  table: relation.TableRef,
  relation_name: String,
) -> Result(relation.Relation, LoadError) {
  case relation.for_table(snapshot, table.schema, table.name) {
    option.None -> Error(UnknownTable(table))
    option.Some(table_relations) ->
      case find_relation_in(table_relations.relations, relation_name) {
        option.Some(next_relation) -> Ok(next_relation)
        option.None ->
          Error(UnknownRelation(table: table, relation_name: relation_name))
      }
  }
}

fn find_relation_in(
  relations: List(relation.Relation),
  relation_name: String,
) -> option.Option(relation.Relation) {
  case relations {
    [] -> option.None
    [next_relation, ..rest] -> {
      case next_relation.name == relation_name {
        True -> option.Some(next_relation)
        False -> find_relation_in(rest, relation_name)
      }
    }
  }
}

fn join_predicate(
  source_table: schema.Table,
  related_table: schema.Table,
  column_pairs: List(relation.ColumnPair),
) -> expression.Predicate {
  case column_pairs {
    [] ->
      expression.Comparison(
        lhs: expression.ValueExpr(expression.Bool(True)),
        op: expression.Eq,
        rhs: expression.ValueExpr(expression.Bool(True)),
      )

    [first, ..rest] -> {
      let initial = join_pair_predicate(source_table, related_table, first)
      join_predicate_rest(source_table, related_table, rest, initial)
    }
  }
}

fn join_predicate_rest(
  source_table: schema.Table,
  related_table: schema.Table,
  pairs: List(relation.ColumnPair),
  acc: expression.Predicate,
) -> expression.Predicate {
  case pairs {
    [] -> acc
    [pair, ..rest] ->
      join_predicate_rest(
        source_table,
        related_table,
        rest,
        expression.And(
          left: acc,
          right: join_pair_predicate(source_table, related_table, pair),
        ),
      )
  }
}

fn join_pair_predicate(
  source_table: schema.Table,
  related_table: schema.Table,
  pair: relation.ColumnPair,
) -> expression.Predicate {
  expression.Comparison(
    lhs: expression.ColumnExpr(schema.ColumnMeta(
      table: source_table,
      name: pair.local_column,
    )),
    op: expression.Eq,
    rhs: expression.ColumnExpr(schema.ColumnMeta(
      table: related_table,
      name: pair.related_column,
    )),
  )
}

fn lazy_predicate(
  source_table: relation.TableRef,
  relation_name: String,
  next_relation: relation.Relation,
  identities: List(unit_of_work.Identity),
) -> Result(expression.Predicate, LoadError) {
  case identities {
    [] ->
      Error(EmptyParentIdentities(
        table: source_table,
        relation_name: relation_name,
      ))
    [first, ..rest] -> {
      use initial <- result_try(identity_predicate(
        source_table,
        relation_name,
        next_relation,
        first,
      ))
      lazy_predicate_rest(
        source_table,
        relation_name,
        next_relation,
        rest,
        initial,
      )
    }
  }
}

fn lazy_predicate_rest(
  source_table: relation.TableRef,
  relation_name: String,
  next_relation: relation.Relation,
  identities: List(unit_of_work.Identity),
  acc: expression.Predicate,
) -> Result(expression.Predicate, LoadError) {
  case identities {
    [] -> Ok(acc)
    [next_identity, ..rest] -> {
      use next_predicate <- result_try(identity_predicate(
        source_table,
        relation_name,
        next_relation,
        next_identity,
      ))

      lazy_predicate_rest(
        source_table,
        relation_name,
        next_relation,
        rest,
        expression.Or(left: acc, right: next_predicate),
      )
    }
  }
}

fn identity_predicate(
  source_table: relation.TableRef,
  relation_name: String,
  next_relation: relation.Relation,
  next_identity: unit_of_work.Identity,
) -> Result(expression.Predicate, LoadError) {
  case next_relation.column_pairs {
    [] ->
      Error(EmptyParentIdentities(
        table: source_table,
        relation_name: relation_name,
      ))
    [first, ..rest] -> {
      use initial <- result_try(identity_pair_predicate(
        source_table,
        relation_name,
        next_relation.related_table,
        next_identity,
        first,
      ))

      identity_predicate_rest(
        source_table,
        relation_name,
        next_relation.related_table,
        next_identity,
        rest,
        initial,
      )
    }
  }
}

fn identity_predicate_rest(
  source_table: relation.TableRef,
  relation_name: String,
  related_table: relation.TableRef,
  next_identity: unit_of_work.Identity,
  pairs: List(relation.ColumnPair),
  acc: expression.Predicate,
) -> Result(expression.Predicate, LoadError) {
  case pairs {
    [] -> Ok(acc)
    [pair, ..rest] -> {
      use next_predicate <- result_try(identity_pair_predicate(
        source_table,
        relation_name,
        related_table,
        next_identity,
        pair,
      ))

      identity_predicate_rest(
        source_table,
        relation_name,
        related_table,
        next_identity,
        rest,
        expression.And(left: acc, right: next_predicate),
      )
    }
  }
}

fn identity_pair_predicate(
  source_table: relation.TableRef,
  relation_name: String,
  related_table: relation.TableRef,
  next_identity: unit_of_work.Identity,
  pair: relation.ColumnPair,
) -> Result(expression.Predicate, LoadError) {
  use value <- result_try(identity_value(
    source_table,
    relation_name,
    next_identity,
    pair.local_column,
  ))

  let related_ast_table =
    schema.Table(
      schema: option.Some(related_table.schema),
      name: related_table.name,
      alias: option.None,
    )

  Ok(expression.Comparison(
    lhs: expression.ColumnExpr(schema.ColumnMeta(
      table: related_ast_table,
      name: pair.related_column,
    )),
    op: expression.Eq,
    rhs: expression.ValueExpr(value),
  ))
}

fn identity_value(
  source_table: relation.TableRef,
  relation_name: String,
  next_identity: unit_of_work.Identity,
  column_name: String,
) -> Result(expression.SqlValue, LoadError) {
  case find_field_value(next_identity.fields, column_name) {
    option.Some(value) -> Ok(value)
    option.None ->
      Error(MissingIdentityField(
        table: source_table,
        relation_name: relation_name,
        column: column_name,
      ))
  }
}

fn find_field_value(
  fields: List(unit_of_work.FieldValue),
  column_name: String,
) -> option.Option(expression.SqlValue) {
  case fields {
    [] -> option.None
    [field_value, ..rest] -> {
      case field_value.column == column_name {
        True -> option.Some(field_value.value)
        False -> find_field_value(rest, column_name)
      }
    }
  }
}

fn relation_ref_from_table(table: schema.Table) -> relation.TableRef {
  relation.table_ref(
    case table.schema {
      option.Some(schema_name) -> schema_name
      option.None -> "public"
    },
    table.name,
  )
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

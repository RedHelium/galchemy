import galchemy/ast/expression
import galchemy/ast/query as ast_query
import galchemy/ast/schema
import galchemy/orm/entity
import galchemy/orm/metadata
import galchemy/orm/query as orm_query
import galchemy/schema/relation
import galchemy/session/unit_of_work
import gleam/list
import gleam/option

pub type LoaderOption {
  JoinedLoad(relation_name: String, related_model: orm_query.ModelRef)
  SelectInLoad(relation_name: String, related_model: orm_query.ModelRef)
}

pub type SelectInPlan {
  SelectInPlan(
    parent: orm_query.ModelRef,
    relation: relation.Relation,
    related_model: orm_query.ModelRef,
  )
}

pub type AppliedOptions {
  AppliedOptions(query: expression.SelectQuery, select_in: List(SelectInPlan))
}

pub type LoadingError {
  QueryError(orm_query.QueryError)
  MissingParentField(
    table: relation.TableRef,
    relation_name: String,
    column: String,
  )
}

pub fn joinedload(
  relation_name: String,
  related_model: orm_query.ModelRef,
) -> LoaderOption {
  JoinedLoad(relation_name: relation_name, related_model: related_model)
}

pub fn selectinload(
  relation_name: String,
  related_model: orm_query.ModelRef,
) -> LoaderOption {
  SelectInLoad(relation_name: relation_name, related_model: related_model)
}

pub fn apply(
  query: expression.SelectQuery,
  parent_model: orm_query.ModelRef,
  options: List(LoaderOption),
) -> Result(AppliedOptions, LoadingError) {
  apply_loop(query, parent_model, options, [])
}

pub fn query(applied: AppliedOptions) -> expression.SelectQuery {
  applied.query
}

pub fn selectin_queries(
  applied: AppliedOptions,
  parents: List(entity.Entity),
) -> Result(List(ast_query.Query), LoadingError) {
  build_selectin_queries(applied.select_in, parents, [])
}

fn apply_loop(
  query: expression.SelectQuery,
  parent_model: orm_query.ModelRef,
  options: List(LoaderOption),
  select_in: List(SelectInPlan),
) -> Result(AppliedOptions, LoadingError) {
  case options {
    [] ->
      Ok(AppliedOptions(query: query, select_in: list.reverse(select_in)))
    [next_option, ..rest] -> {
      case next_option {
        JoinedLoad(relation_name: relation_name, related_model: related_model) -> {
          use next_query <- result_try(
            orm_query.left_join_relation(
              query,
              parent_model,
              relation_name,
              related_model,
            )
            |> map_query_error,
          )

          apply_loop(next_query, parent_model, rest, select_in)
        }
        SelectInLoad(relation_name: relation_name, related_model: related_model) -> {
          use next_relation <- result_try(
            relation_plan(parent_model, relation_name, related_model)
            |> map_query_error,
          )

          apply_loop(query, parent_model, rest, [
            SelectInPlan(
              parent: parent_model,
              relation: next_relation,
              related_model: related_model,
            ),
            ..select_in
          ])
        }
      }
    }
  }
}

fn build_selectin_queries(
  plans: List(SelectInPlan),
  parents: List(entity.Entity),
  acc: List(ast_query.Query),
) -> Result(List(ast_query.Query), LoadingError) {
  case plans {
    [] -> Ok(list.reverse(acc))
    [next_plan, ..rest] -> {
      use next_query <- result_try(selectin_query(next_plan, parents))
      build_selectin_queries(rest, parents, [next_query, ..acc])
    }
  }
}

fn selectin_query(
  plan: SelectInPlan,
  parents: List(entity.Entity),
) -> Result(ast_query.Query, LoadingError) {
  case parents {
    [] ->
      Ok(ast_query.Select(orm_query.select_all(plan.related_model)))
    _ -> {
      use where_ <- result_try(selectin_predicate(
        plan.parent,
        plan.relation,
        plan.related_model,
        parents,
      ))

      Ok(
        ast_query.Select(
          orm_query.select_all(plan.related_model)
          |> orm_query.where_(where_),
        ),
      )
    }
  }
}

fn selectin_predicate(
  parent_model: orm_query.ModelRef,
  next_relation: relation.Relation,
  related_model: orm_query.ModelRef,
  parents: List(entity.Entity),
) -> Result(expression.Predicate, LoadingError) {
  case parents {
    [] -> panic as "selectin predicate requires at least one parent"
    [first_parent, ..rest] -> {
      use initial <- result_try(parent_predicate(
        parent_model,
        next_relation,
        related_model,
        first_parent,
      ))

      selectin_predicate_rest(
        parent_model,
        next_relation,
        related_model,
        rest,
        initial,
      )
    }
  }
}

fn selectin_predicate_rest(
  parent_model: orm_query.ModelRef,
  next_relation: relation.Relation,
  related_model: orm_query.ModelRef,
  parents: List(entity.Entity),
  acc: expression.Predicate,
) -> Result(expression.Predicate, LoadingError) {
  case parents {
    [] -> Ok(acc)
    [next_parent, ..rest] -> {
      use next_predicate <- result_try(parent_predicate(
        parent_model,
        next_relation,
        related_model,
        next_parent,
      ))

      selectin_predicate_rest(
        parent_model,
        next_relation,
        related_model,
        rest,
        expression.Or(left: acc, right: next_predicate),
      )
    }
  }
}

fn parent_predicate(
  parent_model: orm_query.ModelRef,
  next_relation: relation.Relation,
  related_model: orm_query.ModelRef,
  parent: entity.Entity,
) -> Result(expression.Predicate, LoadingError) {
  case next_relation.column_pairs {
    [] -> panic as "relation metadata requires at least one column pair"
    [first_pair, ..rest] -> {
      use initial <- result_try(pair_predicate(
        parent_model,
        next_relation,
        related_model,
        parent,
        first_pair,
      ))

      parent_predicate_rest(
        parent_model,
        next_relation,
        related_model,
        parent,
        rest,
        initial,
      )
    }
  }
}

fn parent_predicate_rest(
  parent_model: orm_query.ModelRef,
  next_relation: relation.Relation,
  related_model: orm_query.ModelRef,
  parent: entity.Entity,
  pairs: List(relation.ColumnPair),
  acc: expression.Predicate,
) -> Result(expression.Predicate, LoadingError) {
  case pairs {
    [] -> Ok(acc)
    [next_pair, ..rest] -> {
      use next_predicate <- result_try(pair_predicate(
        parent_model,
        next_relation,
        related_model,
        parent,
        next_pair,
      ))

      parent_predicate_rest(
        parent_model,
        next_relation,
        related_model,
        parent,
        rest,
        expression.And(left: acc, right: next_predicate),
      )
    }
  }
}

fn pair_predicate(
  parent_model: orm_query.ModelRef,
  next_relation: relation.Relation,
  related_model: orm_query.ModelRef,
  parent: entity.Entity,
  next_pair: relation.ColumnPair,
) -> Result(expression.Predicate, LoadingError) {
  use value <- result_try(parent_field_value(
    parent_model,
    next_relation.name,
    parent,
    next_pair.local_column,
  ))

  Ok(expression.Comparison(
    lhs: expression.ColumnExpr(
      schema.ColumnMeta(
        table: related_model.table,
        name: next_pair.related_column,
      ),
    ),
    op: expression.Eq,
    rhs: expression.ValueExpr(value),
  ))
}

fn parent_field_value(
  parent_model: orm_query.ModelRef,
  relation_name: String,
  parent: entity.Entity,
  column_name: String,
) -> Result(expression.SqlValue, LoadingError) {
  case find_field(entity.fields(parent), column_name) {
    option.Some(value) -> Ok(value)
    option.None ->
      Error(MissingParentField(
        table: parent_model.metadata.table,
        relation_name: relation_name,
        column: column_name,
      ))
  }
}

fn relation_plan(
  parent_model: orm_query.ModelRef,
  relation_name: String,
  related_model: orm_query.ModelRef,
) -> Result(relation.Relation, orm_query.QueryError) {
  case metadata.relation_named(parent_model.metadata, relation_name) {
    option.None ->
      Error(orm_query.UnknownRelation(
        parent_model.metadata.table,
        relation_name,
      ))
    option.Some(next_relation) ->
      case next_relation.related_table == related_model.metadata.table {
        True -> Ok(next_relation)
        False ->
          Error(orm_query.RelatedTableMismatch(
            relation_name: next_relation.name,
            expected: next_relation.related_table,
            actual: related_model.metadata.table,
          ))
      }
  }
}

fn find_field(
  fields: List(unit_of_work.FieldValue),
  column_name: String,
) -> option.Option(expression.SqlValue) {
  case fields {
    [] -> option.None
    [field_value, ..rest] -> {
      case field_value.column == column_name {
        True -> option.Some(field_value.value)
        False -> find_field(rest, column_name)
      }
    }
  }
}

fn map_query_error(
  value: Result(a, orm_query.QueryError),
) -> Result(a, LoadingError) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(QueryError(error))
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

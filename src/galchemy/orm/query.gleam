import galchemy/ast/expression
import galchemy/ast/schema
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/orm/declarative
import galchemy/orm/metadata
import galchemy/schema/relation
import gleam/list
import gleam/option

pub type ModelRef {
  ModelRef(metadata: metadata.ModelMetadata, table: schema.Table)
}

pub type Selection {
  Selection(column: String, alias: option.Option(String))
}

pub type QueryError {
  DeclarativeError(declarative.DeclarativeError)
  UnknownColumn(table: relation.TableRef, column: String)
  UnknownRelation(table: relation.TableRef, relation_name: String)
  RelatedTableMismatch(
    relation_name: String,
    expected: relation.TableRef,
    actual: relation.TableRef,
  )
}

pub fn from_model(
  next_model: declarative.Model,
) -> Result(ModelRef, QueryError) {
  case declarative.to_metadata(next_model) {
    Ok(next_metadata) -> Ok(from_metadata(next_metadata))
    Error(error) -> Error(DeclarativeError(error))
  }
}

pub fn from_metadata(next_metadata: metadata.ModelMetadata) -> ModelRef {
  ModelRef(
    metadata: next_metadata,
    table:
      schema.Table(
        schema: option.Some(next_metadata.table.schema),
        name: next_metadata.table.name,
        alias: option.None,
      ),
  )
}

pub fn as_(model_ref: ModelRef, alias: String) -> ModelRef {
  ModelRef(..model_ref, table: schema.Table(..model_ref.table, alias: option.Some(alias)))
}

pub fn field(column: String) -> Selection {
  Selection(column: column, alias: option.None)
}

pub fn field_as(column: String, alias: String) -> Selection {
  Selection(column: column, alias: option.Some(alias))
}

pub fn item(
  model_ref: ModelRef,
  column_name: String,
) -> Result(expression.SelectItem, QueryError) {
  use next_column <- result_try(col(model_ref, column_name))
  Ok(expr.item(next_column))
}

pub fn item_as(
  model_ref: ModelRef,
  column_name: String,
  alias: String,
) -> Result(expression.SelectItem, QueryError) {
  use next_column <- result_try(col(model_ref, column_name))
  Ok(expr.as_(next_column, alias))
}

pub fn select_all(model_ref: ModelRef) -> expression.SelectQuery {
  select.select(
    list.map(model_ref.metadata.columns, fn(column_name) {
      expr.item(column_expression(model_ref, column_name))
    }),
  )
  |> select.from(model_ref.table)
}

pub fn select_fields(
  model_ref: ModelRef,
  selections: List(Selection),
) -> Result(expression.SelectQuery, QueryError) {
  use items <- result_try(select_items(model_ref, selections, []))

  Ok(
    select.select(items)
    |> select.from(model_ref.table),
  )
}

pub fn col(
  model_ref: ModelRef,
  column_name: String,
) -> Result(expression.Expression, QueryError) {
  case metadata.has_column(model_ref.metadata, column_name) {
    True -> Ok(column_expression(model_ref, column_name))
    False -> Error(UnknownColumn(model_ref.metadata.table, column_name))
  }
}

pub fn eq(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.eq)
}

pub fn neq(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.neq)
}

pub fn gt(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.gt)
}

pub fn gte(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.gte)
}

pub fn lt(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.lt)
}

pub fn lte(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.lte)
}

pub fn like(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.like)
}

pub fn ilike(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
) -> Result(expression.Predicate, QueryError) {
  compare(model_ref, column_name, value, predicate.ilike)
}

pub fn is_null(
  model_ref: ModelRef,
  column_name: String,
) -> Result(expression.Predicate, QueryError) {
  use column <- result_try(col(model_ref, column_name))
  Ok(predicate.is_null(column))
}

pub fn is_not_null(
  model_ref: ModelRef,
  column_name: String,
) -> Result(expression.Predicate, QueryError) {
  use column <- result_try(col(model_ref, column_name))
  Ok(predicate.is_not_null(column))
}

pub fn in_list(
  model_ref: ModelRef,
  column_name: String,
  values: List(expression.Expression),
) -> Result(expression.Predicate, QueryError) {
  use column <- result_try(col(model_ref, column_name))
  Ok(predicate.in_list(column, values))
}

pub fn asc(
  model_ref: ModelRef,
  column_name: String,
) -> Result(expression.Order, QueryError) {
  use column <- result_try(col(model_ref, column_name))
  Ok(select.asc(column))
}

pub fn desc(
  model_ref: ModelRef,
  column_name: String,
) -> Result(expression.Order, QueryError) {
  use column <- result_try(col(model_ref, column_name))
  Ok(select.desc(column))
}

pub fn where_(
  query: expression.SelectQuery,
  next_predicate: expression.Predicate,
) -> expression.SelectQuery {
  select.where_(query, next_predicate)
}

pub fn order_by(
  query: expression.SelectQuery,
  next_order: expression.Order,
) -> expression.SelectQuery {
  select.order_by(query, next_order)
}

pub fn limit(
  query: expression.SelectQuery,
  value: Int,
) -> expression.SelectQuery {
  select.limit(query, value)
}

pub fn offset(
  query: expression.SelectQuery,
  value: Int,
) -> expression.SelectQuery {
  select.offset(query, value)
}

pub fn distinct(query: expression.SelectQuery) -> expression.SelectQuery {
  select.distinct(query)
}

pub fn inner_join_relation(
  query: expression.SelectQuery,
  model_ref: ModelRef,
  relation_name: String,
  related_model: ModelRef,
) -> Result(expression.SelectQuery, QueryError) {
  join_relation(
    query,
    model_ref,
    relation_name,
    related_model,
    select.inner_join,
  )
}

pub fn left_join_relation(
  query: expression.SelectQuery,
  model_ref: ModelRef,
  relation_name: String,
  related_model: ModelRef,
) -> Result(expression.SelectQuery, QueryError) {
  join_relation(
    query,
    model_ref,
    relation_name,
    related_model,
    select.left_join,
  )
}

fn select_items(
  model_ref: ModelRef,
  selections: List(Selection),
  acc: List(expression.SelectItem),
) -> Result(List(expression.SelectItem), QueryError) {
  case selections {
    [] -> Ok(list.reverse(acc))
    [selection, ..rest] -> {
      use column <- result_try(col(model_ref, selection.column))
      let next_item = case selection.alias {
        option.Some(alias) -> expr.as_(column, alias)
        option.None -> expr.item(column)
      }

      select_items(model_ref, rest, [next_item, ..acc])
    }
  }
}

fn compare(
  model_ref: ModelRef,
  column_name: String,
  value: expression.Expression,
  builder: fn(expression.Expression, expression.Expression) -> expression.Predicate,
) -> Result(expression.Predicate, QueryError) {
  use column <- result_try(col(model_ref, column_name))
  Ok(builder(column, value))
}

fn join_relation(
  query: expression.SelectQuery,
  model_ref: ModelRef,
  relation_name: String,
  related_model: ModelRef,
  join_builder: fn(
    expression.SelectQuery,
    schema.Table,
    expression.Predicate,
  ) -> expression.SelectQuery,
) -> Result(expression.SelectQuery, QueryError) {
  use next_relation <- result_try(relation_named(model_ref, relation_name))
  use _ <- result_try(validate_related_model(next_relation, related_model))
  use on <- result_try(join_predicate(model_ref, related_model, next_relation))

  Ok(join_builder(query, related_model.table, on))
}

fn relation_named(
  model_ref: ModelRef,
  relation_name: String,
) -> Result(relation.Relation, QueryError) {
  case metadata.relation_named(model_ref.metadata, relation_name) {
    option.Some(next_relation) -> Ok(next_relation)
    option.None ->
      Error(UnknownRelation(model_ref.metadata.table, relation_name))
  }
}

fn validate_related_model(
  next_relation: relation.Relation,
  related_model: ModelRef,
) -> Result(Nil, QueryError) {
  case next_relation.related_table == related_model.metadata.table {
    True -> Ok(Nil)
    False ->
      Error(RelatedTableMismatch(
        relation_name: next_relation.name,
        expected: next_relation.related_table,
        actual: related_model.metadata.table,
      ))
  }
}

fn join_predicate(
  model_ref: ModelRef,
  related_model: ModelRef,
  next_relation: relation.Relation,
) -> Result(expression.Predicate, QueryError) {
  case next_relation.column_pairs {
    [] -> panic as "relation metadata requires at least one column pair"
    [first_pair, ..rest] -> {
      use first_predicate <- result_try(pair_predicate(
        model_ref,
        related_model,
        first_pair,
      ))

      join_predicate_rest(
        model_ref,
        related_model,
        rest,
        first_predicate,
      )
    }
  }
}

fn join_predicate_rest(
  model_ref: ModelRef,
  related_model: ModelRef,
  pairs: List(relation.ColumnPair),
  acc: expression.Predicate,
) -> Result(expression.Predicate, QueryError) {
  case pairs {
    [] -> Ok(acc)
    [next_pair, ..rest] -> {
      use next_predicate <- result_try(pair_predicate(
        model_ref,
        related_model,
        next_pair,
      ))

      join_predicate_rest(
        model_ref,
        related_model,
        rest,
        predicate.and(acc, next_predicate),
      )
    }
  }
}

fn pair_predicate(
  model_ref: ModelRef,
  related_model: ModelRef,
  next_pair: relation.ColumnPair,
) -> Result(expression.Predicate, QueryError) {
  use left <- result_try(col(model_ref, next_pair.local_column))
  use right <- result_try(col(related_model, next_pair.related_column))
  Ok(predicate.eq(left, right))
}

fn column_expression(
  model_ref: ModelRef,
  column_name: String,
) -> expression.Expression {
  expression.ColumnExpr(
    schema.ColumnMeta(table: model_ref.table, name: column_name),
  )
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

import galchemy/ast/expression
import galchemy/ast/query
import galchemy/ast/schema
import galchemy/schema/model
import galchemy/schema/relation
import gleam/list
import gleam/option

pub type FieldValue {
  FieldValue(column: String, value: expression.SqlValue)
}

pub type Identity {
  Identity(fields: List(FieldValue))
}

pub type PendingInsert {
  PendingInsert(table: relation.TableRef, values: List(FieldValue))
}

pub type PendingUpdate {
  PendingUpdate(
    table: relation.TableRef,
    identity: Identity,
    changes: List(FieldValue),
  )
}

pub type PendingDelete {
  PendingDelete(table: relation.TableRef, identity: Identity)
}

pub type Session {
  Session(
    snapshot: model.SchemaSnapshot,
    inserts: List(PendingInsert),
    updates: List(PendingUpdate),
    deletes: List(PendingDelete),
  )
}

pub type FlushPlan {
  FlushPlan(
    inserts: List(query.Query),
    updates: List(query.Query),
    deletes: List(query.Query),
  )
}

pub type SessionError {
  UnknownTable(relation.TableRef)
  UnknownColumn(table: relation.TableRef, column: String)
  EmptyInsertValues(relation.TableRef)
  EmptyChanges(relation.TableRef)
  EmptyIdentity(relation.TableRef)
}

pub fn new(snapshot: model.SchemaSnapshot) -> Session {
  Session(snapshot: snapshot, inserts: [], updates: [], deletes: [])
}

pub fn field(column: String, value: expression.SqlValue) -> FieldValue {
  FieldValue(column: column, value: value)
}

pub fn identity(fields: List(FieldValue)) -> Identity {
  Identity(fields: fields)
}

pub fn register_new(
  session: Session,
  table: relation.TableRef,
  values: List(FieldValue),
) -> Session {
  Session(..session, inserts: list.append(session.inserts, [PendingInsert(table:, values: values)]))
}

pub fn register_dirty(
  session: Session,
  table: relation.TableRef,
  identity: Identity,
  changes: List(FieldValue),
) -> Session {
  Session(
    ..session,
    updates:
      list.append(
        session.updates,
        [PendingUpdate(table:, identity: identity, changes: changes)],
      ),
  )
}

pub fn register_deleted(
  session: Session,
  table: relation.TableRef,
  identity: Identity,
) -> Session {
  Session(
    ..session,
    deletes: list.append(session.deletes, [PendingDelete(table:, identity: identity)]),
  )
}

pub fn flush_plan(session: Session) -> Result(FlushPlan, SessionError) {
  let insert_order = insert_table_order(session.snapshot)
  let delete_order = list.reverse(insert_order)

  use insert_queries <- result_try(build_insert_queries(session, insert_order))
  use update_queries <- result_try(build_update_queries(session))
  use delete_queries <- result_try(build_delete_queries(session, delete_order))

  Ok(
    FlushPlan(
      inserts: insert_queries,
      updates: update_queries,
      deletes: delete_queries,
    ),
  )
}

pub fn queries(plan: FlushPlan) -> List(query.Query) {
  plan.inserts
  |> list.append(plan.updates)
  |> list.append(plan.deletes)
}

fn build_insert_queries(
  session: Session,
  table_order: List(relation.TableRef),
) -> Result(List(query.Query), SessionError) {
  build_insert_queries_loop(order_inserts(session.inserts, table_order), session.snapshot, [])
}

fn build_insert_queries_loop(
  inserts: List(PendingInsert),
  snapshot: model.SchemaSnapshot,
  acc: List(query.Query),
) -> Result(List(query.Query), SessionError) {
  case inserts {
    [] -> Ok(list.reverse(acc))
    [insert_change, ..rest] -> {
      use next_query <- result_try(insert_query(insert_change, snapshot))
      build_insert_queries_loop(rest, snapshot, [next_query, ..acc])
    }
  }
}

fn build_update_queries(session: Session) -> Result(List(query.Query), SessionError) {
  build_update_queries_loop(session.updates, session.snapshot, [])
}

fn build_update_queries_loop(
  updates: List(PendingUpdate),
  snapshot: model.SchemaSnapshot,
  acc: List(query.Query),
) -> Result(List(query.Query), SessionError) {
  case updates {
    [] -> Ok(list.reverse(acc))
    [update_change, ..rest] -> {
      use next_query <- result_try(update_query(update_change, snapshot))
      build_update_queries_loop(rest, snapshot, [next_query, ..acc])
    }
  }
}

fn build_delete_queries(
  session: Session,
  table_order: List(relation.TableRef),
) -> Result(List(query.Query), SessionError) {
  build_delete_queries_loop(order_deletes(session.deletes, table_order), session.snapshot, [])
}

fn build_delete_queries_loop(
  deletes: List(PendingDelete),
  snapshot: model.SchemaSnapshot,
  acc: List(query.Query),
) -> Result(List(query.Query), SessionError) {
  case deletes {
    [] -> Ok(list.reverse(acc))
    [delete_change, ..rest] -> {
      use next_query <- result_try(delete_query(delete_change, snapshot))
      build_delete_queries_loop(rest, snapshot, [next_query, ..acc])
    }
  }
}

fn insert_query(
  insert_change: PendingInsert,
  snapshot: model.SchemaSnapshot,
) -> Result(query.Query, SessionError) {
  case insert_change.values {
    [] -> Error(EmptyInsertValues(insert_change.table))
    _ -> {
      use table_schema <- result_try(find_table_schema(snapshot, insert_change.table))
      use assignments <- result_try(assignments_for(insert_change.table, table_schema, insert_change.values))

      Ok(
        query.Insert(
          query.InsertQuery(
            table: ast_table(insert_change.table),
            rows: [assignments],
            returning: [],
          ),
        ),
      )
    }
  }
}

fn update_query(
  update_change: PendingUpdate,
  snapshot: model.SchemaSnapshot,
) -> Result(query.Query, SessionError) {
  case update_change.changes {
    [] -> Error(EmptyChanges(update_change.table))
    _ -> {
      use table_schema <- result_try(find_table_schema(snapshot, update_change.table))
      use assignments <- result_try(assignments_for(update_change.table, table_schema, update_change.changes))
      use where_ <- result_try(predicate_for_identity(update_change.table, table_schema, update_change.identity))

      Ok(
        query.Update(
          query.UpdateQuery(
            table: ast_table(update_change.table),
            assignments: assignments,
            where_: option.Some(where_),
            returning: [],
          ),
        ),
      )
    }
  }
}

fn delete_query(
  delete_change: PendingDelete,
  snapshot: model.SchemaSnapshot,
) -> Result(query.Query, SessionError) {
  use table_schema <- result_try(find_table_schema(snapshot, delete_change.table))
  use where_ <- result_try(predicate_for_identity(delete_change.table, table_schema, delete_change.identity))

  Ok(
    query.Delete(
      query.DeleteQuery(
        table: ast_table(delete_change.table),
        where_: option.Some(where_),
        returning: [],
      ),
    ),
  )
}

fn predicate_for_identity(
  table: relation.TableRef,
  table_schema: model.TableSchema,
  identity: Identity,
) -> Result(expression.Predicate, SessionError) {
  case identity.fields {
    [] -> Error(EmptyIdentity(table))
    [first, ..rest] -> {
      use first_predicate <- result_try(predicate_for_field(table, table_schema, first))
      predicate_for_identity_rest(table, table_schema, rest, first_predicate)
    }
  }
}

fn predicate_for_identity_rest(
  table: relation.TableRef,
  table_schema: model.TableSchema,
  fields: List(FieldValue),
  acc: expression.Predicate,
) -> Result(expression.Predicate, SessionError) {
  case fields {
    [] -> Ok(acc)
    [field_value, ..rest] -> {
      use next_predicate <- result_try(predicate_for_field(table, table_schema, field_value))
      predicate_for_identity_rest(
        table,
        table_schema,
        rest,
        expression.And(left: acc, right: next_predicate),
      )
    }
  }
}

fn predicate_for_field(
  table: relation.TableRef,
  table_schema: model.TableSchema,
  field_value: FieldValue,
) -> Result(expression.Predicate, SessionError) {
  use column_meta <- result_try(column_meta_for(table, table_schema, field_value.column))

  Ok(
    expression.Comparison(
      lhs: expression.ColumnExpr(column_meta),
      op: expression.Eq,
      rhs: expression.ValueExpr(field_value.value),
    ),
  )
}

fn assignments_for(
  table: relation.TableRef,
  table_schema: model.TableSchema,
  fields: List(FieldValue),
) -> Result(List(#(schema.ColumnMeta, expression.Expression)), SessionError) {
  assignments_for_loop(table, table_schema, fields, [])
}

fn assignments_for_loop(
  table: relation.TableRef,
  table_schema: model.TableSchema,
  fields: List(FieldValue),
  acc: List(#(schema.ColumnMeta, expression.Expression)),
) -> Result(List(#(schema.ColumnMeta, expression.Expression)), SessionError) {
  case fields {
    [] -> Ok(list.reverse(acc))
    [field_value, ..rest] -> {
      use column_meta <- result_try(column_meta_for(table, table_schema, field_value.column))
      assignments_for_loop(
        table,
        table_schema,
        rest,
        [#(column_meta, expression.ValueExpr(field_value.value)), ..acc],
      )
    }
  }
}

fn column_meta_for(
  table: relation.TableRef,
  table_schema: model.TableSchema,
  column_name: String,
) -> Result(schema.ColumnMeta, SessionError) {
  case has_column(table_schema.columns, column_name) {
    True -> Ok(schema.ColumnMeta(table: ast_table(table), name: column_name))
    False -> Error(UnknownColumn(table: table, column: column_name))
  }
}

fn has_column(columns: List(model.ColumnSchema), column_name: String) -> Bool {
  case columns {
    [] -> False
    [column_schema, ..rest] -> {
      case column_schema.name == column_name {
        True -> True
        False -> has_column(rest, column_name)
      }
    }
  }
}

fn find_table_schema(
  snapshot: model.SchemaSnapshot,
  table: relation.TableRef,
) -> Result(model.TableSchema, SessionError) {
  case find_table_schema_in(snapshot.tables, table) {
    option.Some(table_schema) -> Ok(table_schema)
    option.None -> Error(UnknownTable(table))
  }
}

fn find_table_schema_in(
  tables: List(model.TableSchema),
  table: relation.TableRef,
) -> option.Option(model.TableSchema) {
  case tables {
    [] -> option.None
    [table_schema, ..rest] -> {
      case table_schema.schema == table.schema && table_schema.name == table.name {
        True -> option.Some(table_schema)
        False -> find_table_schema_in(rest, table)
      }
    }
  }
}

fn ast_table(table: relation.TableRef) -> schema.Table {
  schema.Table(schema: option.Some(table.schema), name: table.name, alias: option.None)
}

fn order_inserts(
  inserts: List(PendingInsert),
  table_order: List(relation.TableRef),
) -> List(PendingInsert) {
  let ordered = order_inserts_for_tables(inserts, table_order, [])

  list.append(
    ordered,
    list.filter(inserts, keeping: fn(insert_change) {
      !contains_insert(ordered, insert_change)
    }),
  )
}

fn order_inserts_for_tables(
  inserts: List(PendingInsert),
  table_order: List(relation.TableRef),
  acc: List(PendingInsert),
) -> List(PendingInsert) {
  case table_order {
    [] -> acc
    [table_ref, ..rest] -> {
      order_inserts_for_tables(
        inserts,
        rest,
        list.append(acc, list.filter(inserts, keeping: fn(insert_change) {
          insert_change.table == table_ref
        })),
      )
    }
  }
}

fn contains_insert(
  inserts: List(PendingInsert),
  target: PendingInsert,
) -> Bool {
  case inserts {
    [] -> False
    [insert_change, ..rest] -> {
      case insert_change == target {
        True -> True
        False -> contains_insert(rest, target)
      }
    }
  }
}

fn order_deletes(
  deletes: List(PendingDelete),
  table_order: List(relation.TableRef),
) -> List(PendingDelete) {
  let ordered = order_deletes_for_tables(deletes, table_order, [])

  list.append(
    ordered,
    list.filter(deletes, keeping: fn(delete_change) {
      !contains_delete(ordered, delete_change)
    }),
  )
}

fn order_deletes_for_tables(
  deletes: List(PendingDelete),
  table_order: List(relation.TableRef),
  acc: List(PendingDelete),
) -> List(PendingDelete) {
  case table_order {
    [] -> acc
    [table_ref, ..rest] -> {
      order_deletes_for_tables(
        deletes,
        rest,
        list.append(acc, list.filter(deletes, keeping: fn(delete_change) {
          delete_change.table == table_ref
        })),
      )
    }
  }
}

fn contains_delete(
  deletes: List(PendingDelete),
  target: PendingDelete,
) -> Bool {
  case deletes {
    [] -> False
    [delete_change, ..rest] -> {
      case delete_change == target {
        True -> True
        False -> contains_delete(rest, target)
      }
    }
  }
}

fn insert_table_order(snapshot: model.SchemaSnapshot) -> List(relation.TableRef) {
  let all_tables =
    list.map(snapshot.tables, fn(table_schema) {
      relation.table_ref(table_schema.schema, table_schema.name)
    })

  resolve_insert_order(all_tables, relation.infer(snapshot), [])
}

fn resolve_insert_order(
  remaining: List(relation.TableRef),
  relations_by_table: List(relation.TableRelations),
  resolved: List(relation.TableRef),
) -> List(relation.TableRef) {
  case remaining {
    [] -> resolved
    _ -> {
      let ready =
        list.filter(remaining, keeping: fn(table_ref) {
          dependencies_resolved(table_ref, remaining, relations_by_table)
        })
      let blocked =
        list.filter(remaining, keeping: fn(table_ref) {
          !list.contains(ready, table_ref)
        })

      case ready {
        [] -> list.append(resolved, remaining)
        _ -> resolve_insert_order(blocked, relations_by_table, list.append(resolved, ready))
      }
    }
  }
}

fn dependencies_resolved(
  table: relation.TableRef,
  remaining: List(relation.TableRef),
  relations_by_table: List(relation.TableRelations),
) -> Bool {
  case find_relations_for_table(relations_by_table, table) {
    option.None -> True
    option.Some(table_relations) -> {
      let dependencies =
        list.filter_map(table_relations.relations, with: fn(next_relation) {
          case next_relation.kind {
            relation.BelongsTo -> Ok(next_relation.related_table)
            relation.HasMany -> Error(Nil)
          }
        })

      list.fold(over: dependencies, from: True, with: fn(acc, dependency) {
        acc && !list.contains(remaining, dependency)
      })
    }
  }
}

fn find_relations_for_table(
  relations_by_table: List(relation.TableRelations),
  table: relation.TableRef,
) -> option.Option(relation.TableRelations) {
  case relations_by_table {
    [] -> option.None
    [table_relations, ..rest] -> {
      case table_relations.table == table {
        True -> option.Some(table_relations)
        False -> find_relations_for_table(rest, table)
      }
    }
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

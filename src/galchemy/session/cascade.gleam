import galchemy/orm/entity
import galchemy/orm/metadata
import galchemy/schema/relation
import galchemy/session/runtime
import gleam/option

pub type CascadeAction {
  Persist
  Delete
}

pub type CascadeRule {
  CascadeRule(relation_name: String, actions: List(CascadeAction))
}

pub type CascadeRelation {
  CascadeRelation(name: String, children: List(CascadeNode))
}

pub type CascadeNode {
  CascadeNode(entity: entity.Entity, relations: List(CascadeRelation))
}

pub type CascadeError {
  RuntimeError(runtime.TrackError)
  UnknownRelation(table: relation.TableRef, relation_name: String)
}

pub fn rule(relation_name: String, actions: List(CascadeAction)) -> CascadeRule {
  CascadeRule(relation_name: relation_name, actions: actions)
}

pub fn persist_rule(relation_name: String) -> CascadeRule {
  rule(relation_name, [Persist])
}

pub fn delete_rule(relation_name: String) -> CascadeRule {
  rule(relation_name, [Delete])
}

pub fn related(name: String, children: List(CascadeNode)) -> CascadeRelation {
  CascadeRelation(name: name, children: children)
}

pub fn node(
  next_entity: entity.Entity,
  relations: List(CascadeRelation),
) -> CascadeNode {
  CascadeNode(entity: next_entity, relations: relations)
}

pub fn stage(
  session: runtime.Session,
  root: CascadeNode,
  rules: List(CascadeRule),
) -> Result(runtime.Session, CascadeError) {
  use next_session <- result_try(persist_entity(session, root.entity))
  cascade_relations(next_session, root.entity, root.relations, rules, Persist)
}

pub fn delete(
  session: runtime.Session,
  root: CascadeNode,
  rules: List(CascadeRule),
) -> Result(runtime.Session, CascadeError) {
  use next_session <- result_try(delete_entity(session, root.entity))
  cascade_relations(next_session, root.entity, root.relations, rules, Delete)
}

fn cascade_relations(
  session: runtime.Session,
  parent: entity.Entity,
  relations: List(CascadeRelation),
  rules: List(CascadeRule),
  action: CascadeAction,
) -> Result(runtime.Session, CascadeError) {
  case relations {
    [] -> Ok(session)
    [next_relation, ..rest] -> {
      use metadata_relation <- result_try(relation_metadata(
        parent.metadata,
        next_relation.name,
      ))

      let next_session = case
        allows_action(rules, metadata_relation.name, action)
      {
        True -> cascade_children(session, next_relation.children, rules, action)
        False -> Ok(session)
      }

      use updated_session <- result_try(next_session)
      cascade_relations(updated_session, parent, rest, rules, action)
    }
  }
}

fn cascade_children(
  session: runtime.Session,
  children: List(CascadeNode),
  rules: List(CascadeRule),
  action: CascadeAction,
) -> Result(runtime.Session, CascadeError) {
  case children {
    [] -> Ok(session)
    [child, ..rest] -> {
      use updated_session <- result_try(cascade_child(
        session,
        child,
        rules,
        action,
      ))
      cascade_children(updated_session, rest, rules, action)
    }
  }
}

fn cascade_child(
  session: runtime.Session,
  child: CascadeNode,
  rules: List(CascadeRule),
  action: CascadeAction,
) -> Result(runtime.Session, CascadeError) {
  case action {
    Persist -> stage(session, child, rules)
    Delete -> delete(session, child, rules)
  }
}

fn persist_entity(
  session: runtime.Session,
  next_entity: entity.Entity,
) -> Result(runtime.Session, CascadeError) {
  case entity.status(next_entity) {
    entity.Clean ->
      runtime.attach(session, next_entity)
      |> map_runtime_error
    _ ->
      runtime.stage(session, next_entity)
      |> map_runtime_error
  }
}

fn delete_entity(
  session: runtime.Session,
  next_entity: entity.Entity,
) -> Result(runtime.Session, CascadeError) {
  runtime.stage(session, entity.mark_deleted(next_entity))
  |> map_runtime_error
}

fn relation_metadata(
  next_metadata: metadata.ModelMetadata,
  relation_name: String,
) -> Result(relation.Relation, CascadeError) {
  case metadata.relation_named(next_metadata, relation_name) {
    option.Some(next_relation) -> Ok(next_relation)
    option.None ->
      Error(UnknownRelation(
        table: next_metadata.table,
        relation_name: relation_name,
      ))
  }
}

fn allows_action(
  rules: List(CascadeRule),
  relation_name: String,
  action: CascadeAction,
) -> Bool {
  case rules {
    [] -> False
    [next_rule, ..rest] -> {
      case next_rule.relation_name == relation_name {
        True -> contains_action(next_rule.actions, action)
        False -> allows_action(rest, relation_name, action)
      }
    }
  }
}

fn contains_action(actions: List(CascadeAction), target: CascadeAction) -> Bool {
  case actions {
    [] -> False
    [next_action, ..rest] -> {
      case next_action == target {
        True -> True
        False -> contains_action(rest, target)
      }
    }
  }
}

fn map_runtime_error(
  value: Result(runtime.Session, runtime.TrackError),
) -> Result(runtime.Session, CascadeError) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(RuntimeError(error))
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

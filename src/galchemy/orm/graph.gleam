import galchemy/ast/expression
import galchemy/orm/entity
import galchemy/orm/hook
import galchemy/orm/identity_map
import galchemy/orm/metadata
import galchemy/schema/relation
import galchemy/session/unit_of_work
import gleam/list
import gleam/option

pub type RelationValue {
  ToOne(option.Option(entity.Entity))
  ToMany(List(entity.Entity))
}

pub type HydratedRelation {
  HydratedRelation(name: String, value: RelationValue)
}

pub type HydratedEntity {
  HydratedEntity(entity: entity.Entity, relations: List(HydratedRelation))
}

pub type HydrationError {
  EntityError(entity.EntityError)
  UnknownRelation(table: relation.TableRef, relation_name: String)
}

pub type HydrationHookError(hook_error) {
  HydrationError(HydrationError)
  HookError(hook_error)
}

pub fn hydrate(
  next_entity: entity.Entity,
  identities: identity_map.IdentityMap,
) -> Result(HydratedEntity, HydrationError) {
  case hydrate_with_hooks(next_entity, identities, hook.none()) {
    Ok(value) -> Ok(value)
    Error(HydrationError(error)) -> Error(error)
    Error(HookError(_)) -> panic as "unreachable hook error for hook.none()"
  }
}

pub fn hydrate_with_hooks(
  next_entity: entity.Entity,
  identities: identity_map.IdentityMap,
  hooks: hook.EntityHooks(hook_error),
) -> Result(HydratedEntity, HydrationHookError(hook_error)) {
  let relation_names =
    list.map(next_entity.metadata.relations, fn(next_relation) {
      next_relation.name
    })

  hydrate_only_with_hooks(next_entity, relation_names, identities, hooks)
}

pub fn hydrate_only(
  next_entity: entity.Entity,
  relation_names: List(String),
  identities: identity_map.IdentityMap,
) -> Result(HydratedEntity, HydrationError) {
  case hydrate_only_with_hooks(next_entity, relation_names, identities, hook.none()) {
    Ok(value) -> Ok(value)
    Error(HydrationError(error)) -> Error(error)
    Error(HookError(_)) -> panic as "unreachable hook error for hook.none()"
  }
}

pub fn hydrate_only_with_hooks(
  next_entity: entity.Entity,
  relation_names: List(String),
  identities: identity_map.IdentityMap,
  hooks: hook.EntityHooks(hook_error),
) -> Result(HydratedEntity, HydrationHookError(hook_error)) {
  hydrate_relations(next_entity, relation_names, identities, hooks, [])
}

pub fn hydrate_many(
  entities: List(entity.Entity),
  relation_names: List(String),
  identities: identity_map.IdentityMap,
) -> Result(List(HydratedEntity), HydrationError) {
  case hydrate_many_with_hooks(entities, relation_names, identities, hook.none()) {
    Ok(value) -> Ok(value)
    Error(HydrationError(error)) -> Error(error)
    Error(HookError(_)) -> panic as "unreachable hook error for hook.none()"
  }
}

pub fn hydrate_many_with_hooks(
  entities: List(entity.Entity),
  relation_names: List(String),
  identities: identity_map.IdentityMap,
  hooks: hook.EntityHooks(hook_error),
) -> Result(List(HydratedEntity), HydrationHookError(hook_error)) {
  hydrate_many_loop(entities, relation_names, identities, hooks, [])
}

pub fn relation_named(
  hydrated: HydratedEntity,
  relation_name: String,
) -> option.Option(HydratedRelation) {
  find_hydrated_relation(hydrated.relations, relation_name)
}

pub fn hydrated_entity(hydrated: HydratedEntity) -> entity.Entity {
  hydrated.entity
}

fn hydrate_relations(
  next_entity: entity.Entity,
  relation_names: List(String),
  identities: identity_map.IdentityMap,
  hooks: hook.EntityHooks(hook_error),
  acc: List(HydratedRelation),
) -> Result(HydratedEntity, HydrationHookError(hook_error)) {
  case relation_names {
    [] -> Ok(HydratedEntity(entity: next_entity, relations: list.reverse(acc)))
    [relation_name, ..rest] -> {
      use #(updated_entity, hydrated_relation) <- result_try(hydrate_relation(
        next_entity,
        relation_name,
        identities,
        hooks,
      ))

      hydrate_relations(updated_entity, rest, identities, hooks, [
        hydrated_relation,
        ..acc
      ])
    }
  }
}

fn hydrate_many_loop(
  entities: List(entity.Entity),
  relation_names: List(String),
  identities: identity_map.IdentityMap,
  hooks: hook.EntityHooks(hook_error),
  acc: List(HydratedEntity),
) -> Result(List(HydratedEntity), HydrationHookError(hook_error)) {
  case entities {
    [] -> Ok(list.reverse(acc))
    [next_entity, ..rest] -> {
      use hydrated <- result_try(hydrate_only_with_hooks(
        next_entity,
        relation_names,
        identities,
        hooks,
      ))

      hydrate_many_loop(rest, relation_names, identities, hooks, [hydrated, ..acc])
    }
  }
}

fn hydrate_relation(
  next_entity: entity.Entity,
  relation_name: String,
  identities: identity_map.IdentityMap,
  hooks: hook.EntityHooks(hook_error),
) -> Result(#(entity.Entity, HydratedRelation), HydrationHookError(hook_error)) {
  case metadata.relation_named(next_entity.metadata, relation_name) {
    option.None ->
      Error(HydrationError(UnknownRelation(
        table: next_entity.metadata.table,
        relation_name: relation_name,
      )))
    option.Some(next_relation) -> {
      let related_entities =
        identity_map.values_for_table(identities, next_relation.related_table)
        |> list.filter(keeping: fn(candidate) {
          related_matches(next_entity, next_relation, candidate)
        })
      use loaded_entity <- result_try(mark_relation_loaded(
        next_entity,
        relation_name,
        hooks,
      ))

      Ok(#(
        loaded_entity,
        HydratedRelation(
          name: relation_name,
          value: relation_value(next_relation, related_entities),
        ),
      ))
    }
  }
}

fn relation_value(
  next_relation: relation.Relation,
  related_entities: List(entity.Entity),
) -> RelationValue {
  case next_relation.kind {
    relation.BelongsTo ->
      case related_entities {
        [first, ..] -> ToOne(option.Some(first))
        [] -> ToOne(option.None)
      }
    relation.HasMany -> ToMany(related_entities)
  }
}

fn related_matches(
  next_entity: entity.Entity,
  next_relation: relation.Relation,
  related_entity: entity.Entity,
) -> Bool {
  case next_relation.column_pairs {
    [] -> False
    [first, ..rest] -> {
      pair_matches(next_entity, related_entity, first)
      && remaining_pairs_match(next_entity, related_entity, rest)
    }
  }
}

fn remaining_pairs_match(
  next_entity: entity.Entity,
  related_entity: entity.Entity,
  pairs: List(relation.ColumnPair),
) -> Bool {
  case pairs {
    [] -> True
    [pair, ..rest] ->
      pair_matches(next_entity, related_entity, pair)
      && remaining_pairs_match(next_entity, related_entity, rest)
  }
}

fn pair_matches(
  next_entity: entity.Entity,
  related_entity: entity.Entity,
  pair: relation.ColumnPair,
) -> Bool {
  case find_field_value(next_entity.fields, pair.local_column) {
    option.None -> False
    option.Some(local_value) ->
      case find_field_value(related_entity.fields, pair.related_column) {
        option.None -> False
        option.Some(related_value) -> local_value == related_value
      }
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

fn mark_relation_loaded(
  next_entity: entity.Entity,
  relation_name: String,
  hooks: hook.EntityHooks(hook_error),
) -> Result(entity.Entity, HydrationHookError(hook_error)) {
  case entity.mark_relation_loaded(next_entity, relation_name) {
    Ok(updated_entity) ->
      hook.after_relation_loaded(hooks, updated_entity, relation_name)
      |> map_hook_error
    Error(error) -> Error(HydrationError(EntityError(error)))
  }
}

fn find_hydrated_relation(
  relations: List(HydratedRelation),
  relation_name: String,
) -> option.Option(HydratedRelation) {
  case relations {
    [] -> option.None
    [next_relation, ..rest] -> {
      case next_relation.name == relation_name {
        True -> option.Some(next_relation)
        False -> find_hydrated_relation(rest, relation_name)
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

fn map_hook_error(
  value: Result(entity.Entity, hook_error),
) -> Result(entity.Entity, HydrationHookError(hook_error)) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(HookError(error))
  }
}

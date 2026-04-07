import galchemy/orm/entity

pub type EntityHooks(error) {
  EntityHooks(
    after_load: fn(entity.Entity) -> Result(entity.Entity, error),
    before_insert: fn(entity.Entity) -> Result(entity.Entity, error),
    before_update: fn(entity.Entity) -> Result(entity.Entity, error),
    before_delete: fn(entity.Entity) -> Result(entity.Entity, error),
    after_attach: fn(entity.Entity) -> Result(entity.Entity, error),
    after_refresh: fn(entity.Entity) -> Result(entity.Entity, error),
    after_relation_loaded: fn(entity.Entity, String) ->
      Result(entity.Entity, error),
  )
}

pub fn none() -> EntityHooks(error) {
  EntityHooks(
    after_load: ok_entity,
    before_insert: ok_entity,
    before_update: ok_entity,
    before_delete: ok_entity,
    after_attach: ok_entity,
    after_refresh: ok_entity,
    after_relation_loaded: ok_relation_entity,
  )
}

pub fn after_load(
  hooks: EntityHooks(error),
  next_entity: entity.Entity,
) -> Result(entity.Entity, error) {
  hooks.after_load(next_entity)
  |> map_clean
}

pub fn before_stage(
  hooks: EntityHooks(error),
  next_entity: entity.Entity,
) -> Result(entity.Entity, error) {
  case entity.status(next_entity) {
    entity.Clean -> Ok(next_entity)
    entity.New -> hooks.before_insert(next_entity)
    entity.Dirty(_) -> hooks.before_update(next_entity)
    entity.Deleted ->
      hooks.before_delete(entity.mark_clean(next_entity))
      |> map_deleted
  }
}

pub fn after_attach(
  hooks: EntityHooks(error),
  next_entity: entity.Entity,
) -> Result(entity.Entity, error) {
  hooks.after_attach(next_entity)
  |> map_clean
}

pub fn after_refresh(
  hooks: EntityHooks(error),
  next_entity: entity.Entity,
) -> Result(entity.Entity, error) {
  hooks.after_refresh(next_entity)
  |> map_clean
}

pub fn after_relation_loaded(
  hooks: EntityHooks(error),
  next_entity: entity.Entity,
  relation_name: String,
) -> Result(entity.Entity, error) {
  hooks.after_relation_loaded(next_entity, relation_name)
  |> map_clean
}

fn ok_entity(next_entity: entity.Entity) -> Result(entity.Entity, error) {
  Ok(next_entity)
}

fn ok_relation_entity(
  next_entity: entity.Entity,
  _relation_name: String,
) -> Result(entity.Entity, error) {
  Ok(next_entity)
}

fn map_clean(
  value: Result(entity.Entity, error),
) -> Result(entity.Entity, error) {
  case value {
    Ok(next_entity) -> Ok(entity.mark_clean(next_entity))
    Error(error) -> Error(error)
  }
}

fn map_deleted(
  value: Result(entity.Entity, error),
) -> Result(entity.Entity, error) {
  case value {
    Ok(next_entity) -> Ok(entity.mark_deleted(next_entity))
    Error(error) -> Error(error)
  }
}

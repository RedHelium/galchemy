import galchemy/orm/metadata
import galchemy/schema/relation
import galchemy/session/unit_of_work
import gleam/list
import gleam/option

pub type EntityStatus {
  Clean
  New
  Dirty(changes: List(unit_of_work.FieldValue))
  Deleted
}

pub type Entity {
  Entity(
    metadata: metadata.ModelMetadata,
    fields: List(unit_of_work.FieldValue),
    loaded_relations: List(String),
    status: EntityStatus,
  )
}

pub type EntityError {
  UnknownColumn(table: relation.TableRef, column: String)
  UnknownRelation(table: relation.TableRef, relation_name: String)
  MissingIdentityField(table: relation.TableRef, column: String)
  DeletedEntity(relation.TableRef)
}

pub fn materialize(
  metadata: metadata.ModelMetadata,
  fields: List(unit_of_work.FieldValue),
) -> Result(Entity, EntityError) {
  case validate_fields(metadata, fields) {
    Error(error) -> Error(error)
    Ok(_) ->
      Ok(Entity(
        metadata: metadata,
        fields: fields,
        loaded_relations: [],
        status: Clean,
      ))
  }
}

pub fn new_(
  metadata: metadata.ModelMetadata,
  fields: List(unit_of_work.FieldValue),
) -> Result(Entity, EntityError) {
  case validate_fields(metadata, fields) {
    Error(error) -> Error(error)
    Ok(_) ->
      Ok(Entity(
        metadata: metadata,
        fields: fields,
        loaded_relations: [],
        status: New,
      ))
  }
}

pub fn change(
  entity: Entity,
  changes: List(unit_of_work.FieldValue),
) -> Result(Entity, EntityError) {
  case validate_fields(entity.metadata, changes) {
    Error(error) -> Error(error)
    Ok(_) ->
      case entity.status {
        Deleted -> Error(DeletedEntity(entity.metadata.table))
        New ->
          Ok(
            Entity(
              ..entity,
              fields: merge_fields(entity.fields, changes),
              status: New,
            ),
          )
        Clean ->
          Ok(
            Entity(
              ..entity,
              fields: merge_fields(entity.fields, changes),
              status: Dirty(changes),
            ),
          )
        Dirty(existing_changes) ->
          Ok(
            Entity(
              ..entity,
              fields: merge_fields(entity.fields, changes),
              status: Dirty(merge_fields(existing_changes, changes)),
            ),
          )
      }
  }
}

pub fn mark_deleted(entity: Entity) -> Entity {
  Entity(..entity, status: Deleted)
}

pub fn mark_clean(entity: Entity) -> Entity {
  Entity(..entity, status: Clean)
}

pub fn mark_relation_loaded(
  entity: Entity,
  relation_name: String,
) -> Result(Entity, EntityError) {
  case metadata.has_relation(entity.metadata, relation_name) {
    False ->
      Error(UnknownRelation(
        table: entity.metadata.table,
        relation_name: relation_name,
      ))
    True ->
      Ok(
        Entity(
          ..entity,
          loaded_relations: append_unique(
            entity.loaded_relations,
            relation_name,
          ),
        ),
      )
  }
}

pub fn relation_loaded(entity: Entity, relation_name: String) -> Bool {
  list.contains(entity.loaded_relations, relation_name)
}

pub fn identity(entity: Entity) -> Result(unit_of_work.Identity, EntityError) {
  identity_fields(
    entity.metadata,
    entity.fields,
    entity.metadata.identity_columns,
    [],
  )
}

pub fn stage(
  session: unit_of_work.Session,
  entity: Entity,
) -> Result(unit_of_work.Session, EntityError) {
  case entity.status {
    Clean -> Ok(session)
    New ->
      Ok(unit_of_work.register_new(
        session,
        entity.metadata.table,
        entity.fields,
      ))
    Dirty(changes) -> {
      use next_identity <- result_try(identity(entity))

      Ok(unit_of_work.register_dirty(
        session,
        entity.metadata.table,
        next_identity,
        changes,
      ))
    }
    Deleted -> {
      use next_identity <- result_try(identity(entity))

      Ok(unit_of_work.register_deleted(
        session,
        entity.metadata.table,
        next_identity,
      ))
    }
  }
}

pub fn fields(entity: Entity) -> List(unit_of_work.FieldValue) {
  entity.fields
}

pub fn status(entity: Entity) -> EntityStatus {
  entity.status
}

fn validate_fields(
  metadata: metadata.ModelMetadata,
  fields: List(unit_of_work.FieldValue),
) -> Result(Nil, EntityError) {
  case fields {
    [] -> Ok(Nil)
    [field_value, ..rest] -> {
      case metadata.has_column(metadata, field_value.column) {
        True -> validate_fields(metadata, rest)
        False ->
          Error(UnknownColumn(table: metadata.table, column: field_value.column))
      }
    }
  }
}

fn merge_fields(
  current: List(unit_of_work.FieldValue),
  changes: List(unit_of_work.FieldValue),
) -> List(unit_of_work.FieldValue) {
  case changes {
    [] -> current
    [change, ..rest] -> {
      merge_fields(upsert_field(current, change), rest)
    }
  }
}

fn upsert_field(
  fields: List(unit_of_work.FieldValue),
  next_field: unit_of_work.FieldValue,
) -> List(unit_of_work.FieldValue) {
  case fields {
    [] -> [next_field]
    [field_value, ..rest] -> {
      case field_value.column == next_field.column {
        True -> [next_field, ..rest]
        False -> [field_value, ..upsert_field(rest, next_field)]
      }
    }
  }
}

fn append_unique(items: List(String), item: String) -> List(String) {
  case list.contains(items, item) {
    True -> items
    False -> list.append(items, [item])
  }
}

fn identity_fields(
  metadata: metadata.ModelMetadata,
  fields: List(unit_of_work.FieldValue),
  identity_columns: List(String),
  acc: List(unit_of_work.FieldValue),
) -> Result(unit_of_work.Identity, EntityError) {
  case identity_columns {
    [] -> Ok(unit_of_work.identity(list.reverse(acc)))
    [column_name, ..rest] -> {
      case find_field(fields, column_name) {
        option.Some(field_value) ->
          identity_fields(metadata, fields, rest, [field_value, ..acc])
        option.None ->
          Error(MissingIdentityField(table: metadata.table, column: column_name))
      }
    }
  }
}

fn find_field(
  fields: List(unit_of_work.FieldValue),
  column_name: String,
) -> option.Option(unit_of_work.FieldValue) {
  case fields {
    [] -> option.None
    [field_value, ..rest] -> {
      case field_value.column == column_name {
        True -> option.Some(field_value)
        False -> find_field(rest, column_name)
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

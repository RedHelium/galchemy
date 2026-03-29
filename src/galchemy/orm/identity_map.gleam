import galchemy/orm/entity
import galchemy/schema/relation
import galchemy/session/unit_of_work
import gleam/list
import gleam/option

pub type IdentityEntry {
  IdentityEntry(
    table: relation.TableRef,
    identity: unit_of_work.Identity,
    entity: entity.Entity,
  )
}

pub type IdentityMap {
  IdentityMap(entries: List(IdentityEntry))
}

pub type IdentityMapError {
  EntityError(entity.EntityError)
  DuplicateIdentity(relation.TableRef, unit_of_work.Identity)
}

pub fn empty() -> IdentityMap {
  IdentityMap(entries: [])
}

pub fn insert(
  map: IdentityMap,
  next_entity: entity.Entity,
) -> Result(IdentityMap, IdentityMapError) {
  case entity.identity(next_entity) {
    Error(error) -> Error(EntityError(error))
    Ok(next_identity) -> {
      let table_ref = next_entity.metadata.table

      case get(map, table_ref, next_identity) {
        option.Some(_) -> Error(DuplicateIdentity(table_ref, next_identity))
        option.None ->
          Ok(
            IdentityMap(
              entries: list.append(map.entries, [
                IdentityEntry(
                  table: table_ref,
                  identity: next_identity,
                  entity: next_entity,
                ),
              ]),
            ),
          )
      }
    }
  }
}

pub fn upsert(
  map: IdentityMap,
  next_entity: entity.Entity,
) -> Result(IdentityMap, IdentityMapError) {
  case entity.identity(next_entity) {
    Error(error) -> Error(EntityError(error))
    Ok(next_identity) ->
      Ok(
        IdentityMap(entries: upsert_entry(
          map.entries,
          IdentityEntry(
            table: next_entity.metadata.table,
            identity: next_identity,
            entity: next_entity,
          ),
        )),
      )
  }
}

pub fn get(
  map: IdentityMap,
  table: relation.TableRef,
  identity: unit_of_work.Identity,
) -> option.Option(entity.Entity) {
  case find_entry(map.entries, table, identity) {
    option.Some(entry) -> option.Some(entry.entity)
    option.None -> option.None
  }
}

pub fn remove(
  map: IdentityMap,
  table: relation.TableRef,
  identity: unit_of_work.Identity,
) -> IdentityMap {
  IdentityMap(entries: remove_entry(map.entries, table, identity))
}

pub fn values(map: IdentityMap) -> List(entity.Entity) {
  list.map(map.entries, fn(entry) { entry.entity })
}

pub fn values_for_table(
  map: IdentityMap,
  table: relation.TableRef,
) -> List(entity.Entity) {
  map.entries
  |> list.filter(keeping: fn(entry) { entry.table == table })
  |> list.map(fn(entry) { entry.entity })
}

pub fn entries(map: IdentityMap) -> List(IdentityEntry) {
  map.entries
}

fn find_entry(
  entries: List(IdentityEntry),
  table: relation.TableRef,
  identity: unit_of_work.Identity,
) -> option.Option(IdentityEntry) {
  case entries {
    [] -> option.None
    [entry, ..rest] -> {
      case entry.table == table && entry.identity == identity {
        True -> option.Some(entry)
        False -> find_entry(rest, table, identity)
      }
    }
  }
}

fn remove_entry(
  entries: List(IdentityEntry),
  table: relation.TableRef,
  identity: unit_of_work.Identity,
) -> List(IdentityEntry) {
  case entries {
    [] -> []
    [entry, ..rest] -> {
      case entry.table == table && entry.identity == identity {
        True -> rest
        False -> [entry, ..remove_entry(rest, table, identity)]
      }
    }
  }
}

fn upsert_entry(
  entries: List(IdentityEntry),
  next_entry: IdentityEntry,
) -> List(IdentityEntry) {
  case entries {
    [] -> [next_entry]
    [entry, ..rest] -> {
      case
        entry.table == next_entry.table && entry.identity == next_entry.identity
      {
        True -> [next_entry, ..rest]
        False -> [entry, ..upsert_entry(rest, next_entry)]
      }
    }
  }
}

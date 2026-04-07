import galchemy/orm/entity
import galchemy/orm/hook
import galchemy/orm/identity_map
import galchemy/orm/mapper_registry
import galchemy/schema/relation
import galchemy/session/unit_of_work
import gleam/list
import gleam/option

pub type RowData {
  RowData(table: relation.TableRef, fields: List(unit_of_work.FieldValue))
}

pub type Materializer {
  Materializer(
    registry: mapper_registry.MapperRegistry,
    identities: identity_map.IdentityMap,
  )
}

pub type MaterializationError {
  RegistryError(mapper_registry.RegistryError)
  EntityError(entity.EntityError)
  IdentityMapError(identity_map.IdentityMapError)
}

pub type MaterializationHookError(hook_error) {
  MaterializationError(MaterializationError)
  HookError(hook_error)
}

pub fn new(registry: mapper_registry.MapperRegistry) -> Materializer {
  Materializer(registry: registry, identities: identity_map.empty())
}

pub fn with_identity_map(
  registry: mapper_registry.MapperRegistry,
  identities: identity_map.IdentityMap,
) -> Materializer {
  Materializer(registry: registry, identities: identities)
}

pub fn row(
  schema_name: String,
  table_name: String,
  fields: List(unit_of_work.FieldValue),
) -> RowData {
  RowData(table: relation.table_ref(schema_name, table_name), fields: fields)
}

pub fn materialize(
  materializer: Materializer,
  row_data: RowData,
) -> Result(#(entity.Entity, Materializer), MaterializationError) {
  case materialize_with_hooks(materializer, row_data, hook.none()) {
    Ok(value) -> Ok(value)
    Error(MaterializationError(error)) -> Error(error)
    Error(HookError(_)) -> panic as "unreachable hook error for hook.none()"
  }
}

pub fn materialize_with_hooks(
  materializer: Materializer,
  row_data: RowData,
  hooks: hook.EntityHooks(hook_error),
) -> Result(
  #(entity.Entity, Materializer),
  MaterializationHookError(hook_error),
) {
  let RowData(table: table_ref, fields: fields) = row_data

  case
    mapper_registry.get(materializer.registry, table_ref.schema, table_ref.name)
  {
    Error(error) -> Error(MaterializationError(RegistryError(error)))
    Ok(metadata) ->
      case entity.materialize(metadata, fields) {
        Error(error) -> Error(MaterializationError(EntityError(error)))
        Ok(next_entity) ->
          case entity.identity(next_entity) {
            Error(error) -> Error(MaterializationError(EntityError(error)))
            Ok(next_identity) ->
              case
                identity_map.get(
                  materializer.identities,
                  metadata.table,
                  next_identity,
                )
              {
                option.Some(existing_entity) ->
                  Ok(#(existing_entity, materializer))
                option.None -> {
                  use hooked_entity <- result_try(
                    hook.after_load(hooks, next_entity)
                    |> map_hook_error,
                  )

                  case
                    identity_map.insert(materializer.identities, hooked_entity)
                  {
                    Error(error) ->
                      Error(MaterializationError(IdentityMapError(error)))
                    Ok(next_identities) ->
                      Ok(#(
                        hooked_entity,
                        Materializer(
                          ..materializer,
                          identities: next_identities,
                        ),
                      ))
                  }
                }
              }
          }
      }
  }
}

pub fn materialize_many(
  materializer: Materializer,
  rows: List(RowData),
) -> Result(#(List(entity.Entity), Materializer), MaterializationError) {
  materialize_many_loop(materializer, rows, [])
}

pub fn materialize_many_with_hooks(
  materializer: Materializer,
  rows: List(RowData),
  hooks: hook.EntityHooks(hook_error),
) -> Result(
  #(List(entity.Entity), Materializer),
  MaterializationHookError(hook_error),
) {
  materialize_many_with_hooks_loop(materializer, rows, hooks, [])
}

pub fn identity_map(materializer: Materializer) -> identity_map.IdentityMap {
  materializer.identities
}

pub fn registry(materializer: Materializer) -> mapper_registry.MapperRegistry {
  materializer.registry
}

fn materialize_many_loop(
  materializer: Materializer,
  rows: List(RowData),
  acc: List(entity.Entity),
) -> Result(#(List(entity.Entity), Materializer), MaterializationError) {
  case rows {
    [] -> Ok(#(list.reverse(acc), materializer))
    [next_row, ..rest] -> {
      use #(next_entity, next_materializer) <- result_try(materialize(
        materializer,
        next_row,
      ))

      materialize_many_loop(next_materializer, rest, [next_entity, ..acc])
    }
  }
}

fn materialize_many_with_hooks_loop(
  materializer: Materializer,
  rows: List(RowData),
  hooks: hook.EntityHooks(hook_error),
  acc: List(entity.Entity),
) -> Result(
  #(List(entity.Entity), Materializer),
  MaterializationHookError(hook_error),
) {
  case rows {
    [] -> Ok(#(list.reverse(acc), materializer))
    [next_row, ..rest] -> {
      use #(next_entity, next_materializer) <- result_try(
        materialize_with_hooks(materializer, next_row, hooks),
      )

      materialize_many_with_hooks_loop(next_materializer, rest, hooks, [
        next_entity,
        ..acc
      ])
    }
  }
}

fn map_hook_error(
  value: Result(entity.Entity, hook_error),
) -> Result(entity.Entity, MaterializationHookError(hook_error)) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(HookError(error))
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import gleam/list
import gleam/option

pub type MapperRegistry {
  MapperRegistry(mappers: List(metadata.ModelMetadata))
}

pub type RegistryError {
  DuplicateMapper(relation.TableRef)
  UnknownMapper(relation.TableRef)
  MetadataError(metadata.MetadataError)
}

pub fn empty() -> MapperRegistry {
  MapperRegistry(mappers: [])
}

pub fn from_snapshot(
  snapshot: model.SchemaSnapshot,
) -> Result(MapperRegistry, RegistryError) {
  from_tables(snapshot, snapshot.tables, empty())
}

pub fn register(
  registry: MapperRegistry,
  mapper: metadata.ModelMetadata,
) -> Result(MapperRegistry, RegistryError) {
  case lookup(registry, mapper.table.schema, mapper.table.name) {
    option.Some(_) -> Error(DuplicateMapper(mapper.table))
    option.None ->
      Ok(MapperRegistry(mappers: list.append(registry.mappers, [mapper])))
  }
}

pub fn lookup(
  registry: MapperRegistry,
  schema_name: String,
  table_name: String,
) -> option.Option(metadata.ModelMetadata) {
  find_mapper(registry.mappers, relation.table_ref(schema_name, table_name))
}

pub fn get(
  registry: MapperRegistry,
  schema_name: String,
  table_name: String,
) -> Result(metadata.ModelMetadata, RegistryError) {
  let table_ref = relation.table_ref(schema_name, table_name)

  case find_mapper(registry.mappers, table_ref) {
    option.Some(mapper) -> Ok(mapper)
    option.None -> Error(UnknownMapper(table_ref))
  }
}

pub fn all(registry: MapperRegistry) -> List(metadata.ModelMetadata) {
  registry.mappers
}

fn from_tables(
  snapshot: model.SchemaSnapshot,
  tables: List(model.TableSchema),
  registry: MapperRegistry,
) -> Result(MapperRegistry, RegistryError) {
  case tables {
    [] -> Ok(registry)
    [table_schema, ..rest] -> {
      let next_mapper =
        metadata.from_snapshot(snapshot, table_schema.schema, table_schema.name)

      case next_mapper {
        Error(error) -> Error(MetadataError(error))
        Ok(mapper) ->
          case register(registry, mapper) {
            Error(error) -> Error(error)
            Ok(next_registry) -> from_tables(snapshot, rest, next_registry)
          }
      }
    }
  }
}

fn find_mapper(
  mappers: List(metadata.ModelMetadata),
  table_ref: relation.TableRef,
) -> option.Option(metadata.ModelMetadata) {
  case mappers {
    [] -> option.None
    [mapper, ..rest] -> {
      case mapper.table == table_ref {
        True -> option.Some(mapper)
        False -> find_mapper(rest, table_ref)
      }
    }
  }
}

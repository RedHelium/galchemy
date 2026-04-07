import galchemy/orm/declarative
import galchemy/orm/mapper_registry
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import gleam/list
import gleam/option
import gleam/result

pub type RuntimeRegistry {
  RuntimeRegistry(
    snapshot: model.SchemaSnapshot,
    models: List(RuntimeModel),
  )
}

pub type RuntimeModel {
  RuntimeModel(
    table: relation.TableRef,
    table_schema: model.TableSchema,
    metadata: metadata.ModelMetadata,
  )
}

pub type RegistryError {
  DuplicateModel(relation.TableRef)
  UnknownModel(relation.TableRef)
  MetadataError(metadata.MetadataError)
  DeclarativeError(declarative.DeclarativeError)
  MapperRegistryError(mapper_registry.RegistryError)
}

pub fn empty() -> RuntimeRegistry {
  RuntimeRegistry(
    snapshot: model.SchemaSnapshot(tables: []),
    models: [],
  )
}

pub fn from_snapshot(
  snapshot: model.SchemaSnapshot,
) -> Result(RuntimeRegistry, RegistryError) {
  use models <- result_try(models_from_snapshot(snapshot, snapshot.tables, []))
  Ok(RuntimeRegistry(snapshot: snapshot, models: list.reverse(models)))
}

pub fn from_models(
  models: List(declarative.Model),
) -> Result(RuntimeRegistry, RegistryError) {
  registry_from_models(models, empty())
}

pub fn register_model(
  registry: RuntimeRegistry,
  next_model: declarative.Model,
) -> Result(RuntimeRegistry, RegistryError) {
  use table_schema <- result_try(
    declarative.to_table_schema(next_model)
    |> result.map_error(DeclarativeError),
  )
  let table_ref =
    relation.table_ref(table_schema.schema, table_schema.name)
  use _ <- result_try(ensure_not_registered(registry.models, table_ref))
  use next_metadata <- result_try(
    declarative.to_metadata(next_model)
    |> result.map_error(DeclarativeError),
  )

  Ok(RuntimeRegistry(
    snapshot: model.SchemaSnapshot(
      tables: list.append(registry.snapshot.tables, [table_schema]),
    ),
    models: list.append(registry.models, [
      RuntimeModel(
        table: table_ref,
        table_schema: table_schema,
        metadata: next_metadata,
      ),
    ]),
  ))
}

pub fn lookup(
  registry: RuntimeRegistry,
  schema_name: String,
  table_name: String,
) -> option.Option(RuntimeModel) {
  find_model(registry.models, relation.table_ref(schema_name, table_name))
}

pub fn get(
  registry: RuntimeRegistry,
  schema_name: String,
  table_name: String,
) -> Result(RuntimeModel, RegistryError) {
  let table_ref = relation.table_ref(schema_name, table_name)

  case find_model(registry.models, table_ref) {
    option.Some(value) -> Ok(value)
    option.None -> Error(UnknownModel(table_ref))
  }
}

pub fn all(registry: RuntimeRegistry) -> List(RuntimeModel) {
  registry.models
}

pub fn snapshot(registry: RuntimeRegistry) -> model.SchemaSnapshot {
  registry.snapshot
}

pub fn model_metadata(
  registry: RuntimeRegistry,
  schema_name: String,
  table_name: String,
) -> Result(metadata.ModelMetadata, RegistryError) {
  use next_model <- result_try(get(registry, schema_name, table_name))
  Ok(next_model.metadata)
}

pub fn table_schema(
  registry: RuntimeRegistry,
  schema_name: String,
  table_name: String,
) -> Result(model.TableSchema, RegistryError) {
  use next_model <- result_try(get(registry, schema_name, table_name))
  Ok(next_model.table_schema)
}

pub fn has_column(
  registry: RuntimeRegistry,
  schema_name: String,
  table_name: String,
  column_name: String,
) -> Result(Bool, RegistryError) {
  use next_metadata <- result_try(model_metadata(registry, schema_name, table_name))
  Ok(metadata.has_column(next_metadata, column_name))
}

pub fn has_relation(
  registry: RuntimeRegistry,
  schema_name: String,
  table_name: String,
  relation_name: String,
) -> Result(Bool, RegistryError) {
  use next_metadata <- result_try(model_metadata(registry, schema_name, table_name))
  Ok(metadata.has_relation(next_metadata, relation_name))
}

pub fn relation_named(
  registry: RuntimeRegistry,
  schema_name: String,
  table_name: String,
  relation_name: String,
) -> Result(option.Option(relation.Relation), RegistryError) {
  use next_metadata <- result_try(model_metadata(registry, schema_name, table_name))
  Ok(metadata.relation_named(next_metadata, relation_name))
}

pub fn to_mapper_registry(
  registry: RuntimeRegistry,
) -> Result(mapper_registry.MapperRegistry, RegistryError) {
  registry.models
  |> list.map(fn(next_model) { next_model.metadata })
  |> mapper_registry_from_metadata(mapper_registry.empty())
}

fn models_from_snapshot(
  snapshot: model.SchemaSnapshot,
  tables: List(model.TableSchema),
  acc: List(RuntimeModel),
) -> Result(List(RuntimeModel), RegistryError) {
  case tables {
    [] -> Ok(acc)
    [table_schema, ..rest] -> {
      use next_metadata <- result_try(
        metadata.from_snapshot(snapshot, table_schema.schema, table_schema.name)
        |> result.map_error(MetadataError),
      )

      models_from_snapshot(snapshot, rest, [
        RuntimeModel(
          table: relation.table_ref(table_schema.schema, table_schema.name),
          table_schema: table_schema,
          metadata: next_metadata,
        ),
        ..acc
      ])
    }
  }
}

fn registry_from_models(
  models: List(declarative.Model),
  registry: RuntimeRegistry,
) -> Result(RuntimeRegistry, RegistryError) {
  case models {
    [] -> Ok(registry)
    [next_model, ..rest] -> {
      use next_registry <- result_try(register_model(registry, next_model))
      registry_from_models(rest, next_registry)
    }
  }
}

fn mapper_registry_from_metadata(
  metadatas: List(metadata.ModelMetadata),
  registry: mapper_registry.MapperRegistry,
) -> Result(mapper_registry.MapperRegistry, RegistryError) {
  case metadatas {
    [] -> Ok(registry)
    [next_metadata, ..rest] -> {
      use next_registry <- result_try(
        mapper_registry.register(registry, next_metadata)
        |> result.map_error(MapperRegistryError),
      )
      mapper_registry_from_metadata(rest, next_registry)
    }
  }
}

fn ensure_not_registered(
  models: List(RuntimeModel),
  table_ref: relation.TableRef,
) -> Result(Nil, RegistryError) {
  case find_model(models, table_ref) {
    option.Some(_) -> Error(DuplicateModel(table_ref))
    option.None -> Ok(Nil)
  }
}

fn find_model(
  models: List(RuntimeModel),
  table_ref: relation.TableRef,
) -> option.Option(RuntimeModel) {
  case models {
    [] -> option.None
    [next_model, ..rest] -> {
      case next_model.table == table_ref {
        True -> option.Some(next_model)
        False -> find_model(rest, table_ref)
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

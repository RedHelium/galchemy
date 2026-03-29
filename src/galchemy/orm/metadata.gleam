import galchemy/schema/model
import galchemy/schema/relation
import gleam/list
import gleam/option

pub type ModelMetadata {
  ModelMetadata(
    table: relation.TableRef,
    identity_columns: List(String),
    columns: List(String),
    relations: List(relation.Relation),
  )
}

pub type MetadataError {
  UnknownTable(relation.TableRef)
  MissingIdentity(relation.TableRef)
}

pub fn from_snapshot(
  snapshot: model.SchemaSnapshot,
  schema_name: String,
  table_name: String,
) -> Result(ModelMetadata, MetadataError) {
  let table_ref = relation.table_ref(schema_name, table_name)

  case find_table(snapshot.tables, table_ref) {
    option.None -> Error(UnknownTable(table_ref))
    option.Some(table_schema) -> {
      let identity_columns = derive_identity_columns(table_schema)
      let columns = list.map(table_schema.columns, fn(column) { column.name })
      let relations = case
        relation.for_table(snapshot, schema_name, table_name)
      {
        option.Some(table_relations) -> table_relations.relations
        option.None -> []
      }

      case identity_columns {
        [] -> Error(MissingIdentity(table_ref))
        _ ->
          Ok(ModelMetadata(
            table: table_ref,
            identity_columns: identity_columns,
            columns: columns,
            relations: relations,
          ))
      }
    }
  }
}

pub fn has_column(metadata: ModelMetadata, column_name: String) -> Bool {
  list.contains(metadata.columns, column_name)
}

pub fn has_relation(metadata: ModelMetadata, relation_name: String) -> Bool {
  case find_relation(metadata.relations, relation_name) {
    option.Some(_) -> True
    option.None -> False
  }
}

pub fn relation_named(
  metadata: ModelMetadata,
  relation_name: String,
) -> option.Option(relation.Relation) {
  find_relation(metadata.relations, relation_name)
}

fn derive_identity_columns(table_schema: model.TableSchema) -> List(String) {
  case table_schema.primary_key {
    option.Some(primary_key) -> primary_key.columns
    option.None ->
      case table_schema.unique_constraints {
        [first, ..] -> first.columns
        [] -> []
      }
  }
}

fn find_table(
  tables: List(model.TableSchema),
  table_ref: relation.TableRef,
) -> option.Option(model.TableSchema) {
  case tables {
    [] -> option.None
    [table_schema, ..rest] -> {
      case
        table_schema.schema == table_ref.schema
        && table_schema.name == table_ref.name
      {
        True -> option.Some(table_schema)
        False -> find_table(rest, table_ref)
      }
    }
  }
}

fn find_relation(
  relations: List(relation.Relation),
  relation_name: String,
) -> option.Option(relation.Relation) {
  case relations {
    [] -> option.None
    [next_relation, ..rest] -> {
      case next_relation.name == relation_name {
        True -> option.Some(next_relation)
        False -> find_relation(rest, relation_name)
      }
    }
  }
}

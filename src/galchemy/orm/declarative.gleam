import galchemy/orm/codec
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/schema/relation
import gleam/list
import gleam/option

pub type Column {
  Column(
    name: String,
    data_type: model.ColumnType,
    nullable: Bool,
    default: option.Option(String),
    primary_key: Bool,
    unique: Bool,
  )
}

pub type RelationDefinition {
  RelationDefinition(
    name: String,
    foreign_key_name: String,
    kind: RelationKind,
    related_table: relation.TableRef,
    column_pairs: List(relation.ColumnPair),
  )
}

pub type RelationKind {
  BelongsTo
  HasMany
}

pub type Model {
  Model(
    table: relation.TableRef,
    columns: List(Column),
    identity_columns: option.Option(List(String)),
    relations: List(RelationDefinition),
  )
}

pub type DeclarativeError {
  DuplicateColumn(table: relation.TableRef, column: String)
  DuplicateRelation(table: relation.TableRef, relation_name: String)
  MissingIdentity(table: relation.TableRef)
  UnknownIdentityColumn(table: relation.TableRef, column: String)
  UnknownLocalColumn(table: relation.TableRef, column: String)
}

pub fn model_(
  schema_name: String,
  table_name: String,
  columns: List(Column),
  relations: List(RelationDefinition),
) -> Model {
  Model(
    table: relation.table_ref(schema_name, table_name),
    columns: columns,
    identity_columns: option.None,
    relations: relations,
  )
}

pub fn identity(next_model: Model, columns: List(String)) -> Model {
  Model(..next_model, identity_columns: option.Some(columns))
}

pub fn small_int(name: String) -> Column {
  column(name, model.SmallIntType)
}

pub fn int(name: String) -> Column {
  column(name, model.IntegerType)
}

pub fn big_int(name: String) -> Column {
  column(name, model.BigIntType)
}

pub fn text(name: String) -> Column {
  column(name, model.TextType)
}

pub fn varchar(name: String, length: option.Option(Int)) -> Column {
  column(name, model.VarCharType(length))
}

pub fn bool(name: String) -> Column {
  column(name, model.BooleanType)
}

pub fn float(name: String) -> Column {
  column(name, model.DoublePrecisionType)
}

pub fn real(name: String) -> Column {
  column(name, model.RealType)
}

pub fn numeric(
  name: String,
  precision: option.Option(Int),
  scale: option.Option(Int),
) -> Column {
  column(name, model.NumericType(precision: precision, scale: scale))
}

pub fn timestamp(name: String) -> Column {
  column(name, model.TimestampType(with_time_zone: False))
}

pub fn timestamptz(name: String) -> Column {
  column(name, model.TimestampType(with_time_zone: True))
}

pub fn date(name: String) -> Column {
  column(name, model.DateType)
}

pub fn time_of_day(name: String) -> Column {
  column(name, model.TimeType(with_time_zone: False))
}

pub fn timetz(name: String) -> Column {
  column(name, model.TimeType(with_time_zone: True))
}

pub fn uuid(name: String) -> Column {
  column(name, model.UuidType)
}

pub fn json(name: String) -> Column {
  column(name, model.JsonType)
}

pub fn jsonb(name: String) -> Column {
  column(name, model.JsonbType)
}

pub fn bytea(name: String) -> Column {
  column(name, model.ByteaType)
}

pub fn array(name: String, item_type: model.ColumnType) -> Column {
  column(name, model.ArrayType(item_type: item_type))
}

pub fn custom(name: String, type_name: String) -> Column {
  column(name, model.CustomType(type_name))
}

pub fn custom_with(name: String, next_type: codec.CustomCodec(a)) -> Column {
  column(name, model.CustomType(codec.sql_type_name(next_type)))
}

pub fn nullable(next_column: Column) -> Column {
  Column(..next_column, nullable: True)
}

pub fn default(next_column: Column, sql: String) -> Column {
  Column(..next_column, default: option.Some(sql))
}

pub fn primary_key(next_column: Column) -> Column {
  Column(..next_column, primary_key: True)
}

pub fn unique(next_column: Column) -> Column {
  Column(..next_column, unique: True)
}

pub fn pair(local_column: String, related_column: String) -> relation.ColumnPair {
  relation.pair(local_column, related_column)
}

pub fn belongs_to(
  name: String,
  foreign_key_name: String,
  related_schema: String,
  related_table: String,
  column_pairs: List(relation.ColumnPair),
) -> RelationDefinition {
  RelationDefinition(
    name: name,
    foreign_key_name: foreign_key_name,
    kind: BelongsTo,
    related_table: relation.table_ref(related_schema, related_table),
    column_pairs: column_pairs,
  )
}

pub fn has_many(
  name: String,
  foreign_key_name: String,
  related_schema: String,
  related_table: String,
  column_pairs: List(relation.ColumnPair),
) -> RelationDefinition {
  RelationDefinition(
    name: name,
    foreign_key_name: foreign_key_name,
    kind: HasMany,
    related_table: relation.table_ref(related_schema, related_table),
    column_pairs: column_pairs,
  )
}

pub fn to_metadata(
  next_model: Model,
) -> Result(metadata.ModelMetadata, DeclarativeError) {
  use validated_model <- result_try(validate(next_model))
  use next_identity <- result_try(identity_columns(validated_model))

  Ok(metadata.ModelMetadata(
    table: validated_model.table,
    identity_columns: next_identity,
    columns: list.map(validated_model.columns, fn(next_column) {
      next_column.name
    }),
    relations: relation_metadata(validated_model.relations),
  ))
}

pub fn to_table_schema(
  next_model: Model,
) -> Result(model.TableSchema, DeclarativeError) {
  use validated_model <- result_try(validate(next_model))

  Ok(
    model.TableSchema(
      schema: validated_model.table.schema,
      name: validated_model.table.name,
      columns: column_schemas(validated_model.columns, 1, []),
      primary_key: primary_key_schema(validated_model),
      unique_constraints: unique_constraints(validated_model),
      foreign_keys: foreign_keys(validated_model.relations),
      indexes: [],
    ),
  )
}

pub fn to_snapshot(
  models: List(Model),
) -> Result(model.SchemaSnapshot, DeclarativeError) {
  use tables <- result_try(table_schemas(models, []))
  Ok(model.SchemaSnapshot(tables: list.reverse(tables)))
}

fn column(name: String, data_type: model.ColumnType) -> Column {
  Column(
    name: name,
    data_type: data_type,
    nullable: False,
    default: option.None,
    primary_key: False,
    unique: False,
  )
}

fn validate(next_model: Model) -> Result(Model, DeclarativeError) {
  use _ <- result_try(
    validate_columns(next_model.table, next_model.columns, []),
  )
  use _ <- result_try(
    validate_relations(
      next_model.table,
      next_model.columns,
      next_model.relations,
      [],
    ),
  )
  use _ <- result_try(validate_identity(next_model))

  Ok(next_model)
}

fn validate_columns(
  table: relation.TableRef,
  columns: List(Column),
  known: List(String),
) -> Result(Nil, DeclarativeError) {
  case columns {
    [] -> Ok(Nil)
    [next_column, ..rest] -> {
      case list.contains(known, next_column.name) {
        True -> Error(DuplicateColumn(table: table, column: next_column.name))
        False -> validate_columns(table, rest, [next_column.name, ..known])
      }
    }
  }
}

fn validate_relations(
  table: relation.TableRef,
  columns: List(Column),
  relations: List(RelationDefinition),
  known: List(String),
) -> Result(Nil, DeclarativeError) {
  case relations {
    [] -> Ok(Nil)
    [next_relation, ..rest] -> {
      case list.contains(known, next_relation.name) {
        True ->
          Error(DuplicateRelation(
            table: table,
            relation_name: next_relation.name,
          ))
        False -> {
          use _ <- result_try(validate_relation_columns(
            table,
            columns,
            next_relation.column_pairs,
          ))

          validate_relations(table, columns, rest, [next_relation.name, ..known])
        }
      }
    }
  }
}

fn validate_relation_columns(
  table: relation.TableRef,
  columns: List(Column),
  pairs: List(relation.ColumnPair),
) -> Result(Nil, DeclarativeError) {
  case pairs {
    [] -> Ok(Nil)
    [next_pair, ..rest] -> {
      case has_column(columns, next_pair.local_column) {
        True -> validate_relation_columns(table, columns, rest)
        False ->
          Error(UnknownLocalColumn(table: table, column: next_pair.local_column))
      }
    }
  }
}

fn validate_identity(next_model: Model) -> Result(Nil, DeclarativeError) {
  case next_model.identity_columns {
    option.None ->
      case inferred_identity(next_model.columns) {
        [] -> Error(MissingIdentity(next_model.table))
        _ -> Ok(Nil)
      }
    option.Some(columns) ->
      validate_identity_columns(next_model.table, next_model.columns, columns)
  }
}

fn validate_identity_columns(
  table: relation.TableRef,
  columns: List(Column),
  identity_columns: List(String),
) -> Result(Nil, DeclarativeError) {
  case identity_columns {
    [] -> Error(MissingIdentity(table))
    [next_column, ..rest] -> {
      case has_column(columns, next_column) {
        True -> validate_identity_columns_rest(table, columns, rest)
        False -> Error(UnknownIdentityColumn(table: table, column: next_column))
      }
    }
  }
}

fn validate_identity_columns_rest(
  table: relation.TableRef,
  columns: List(Column),
  identity_columns: List(String),
) -> Result(Nil, DeclarativeError) {
  case identity_columns {
    [] -> Ok(Nil)
    [next_column, ..rest] -> {
      case has_column(columns, next_column) {
        True -> validate_identity_columns_rest(table, columns, rest)
        False -> Error(UnknownIdentityColumn(table: table, column: next_column))
      }
    }
  }
}

fn identity_columns(next_model: Model) -> Result(List(String), DeclarativeError) {
  case next_model.identity_columns {
    option.Some(columns) -> Ok(columns)
    option.None ->
      case inferred_identity(next_model.columns) {
        [] -> Error(MissingIdentity(next_model.table))
        columns -> Ok(columns)
      }
  }
}

fn inferred_identity(columns: List(Column)) -> List(String) {
  let primary_keys =
    list.filter_map(columns, fn(next_column) {
      case next_column.primary_key {
        True -> Ok(next_column.name)
        False -> Error(Nil)
      }
    })

  case primary_keys {
    [] ->
      case unique_columns(columns) {
        [first, ..] -> [first]
        [] -> []
      }
    _ -> primary_keys
  }
}

fn unique_columns(columns: List(Column)) -> List(String) {
  list.filter_map(columns, fn(next_column) {
    case next_column.unique {
      True -> Ok(next_column.name)
      False -> Error(Nil)
    }
  })
}

fn column_schemas(
  columns: List(Column),
  ordinal_position: Int,
  acc: List(model.ColumnSchema),
) -> List(model.ColumnSchema) {
  case columns {
    [] -> list.reverse(acc)
    [next_column, ..rest] ->
      column_schemas(rest, ordinal_position + 1, [
        model.ColumnSchema(
          name: next_column.name,
          data_type: next_column.data_type,
          nullable: next_column.nullable,
          default: next_column.default,
          ordinal_position: ordinal_position,
        ),
        ..acc
      ])
  }
}

fn primary_key_schema(next_model: Model) -> option.Option(model.PrimaryKey) {
  let primary_keys =
    list.filter_map(next_model.columns, fn(next_column) {
      case next_column.primary_key {
        True -> Ok(next_column.name)
        False -> Error(Nil)
      }
    })

  case primary_keys {
    [] -> option.None
    columns ->
      option.Some(model.PrimaryKey(
        name: next_model.table.name <> "_pkey",
        columns: columns,
      ))
  }
}

fn unique_constraints(next_model: Model) -> List(model.UniqueConstraint) {
  unique_columns(next_model.columns)
  |> list.map(fn(column_name) {
    model.UniqueConstraint(
      name: next_model.table.name <> "_" <> column_name <> "_key",
      columns: [column_name],
    )
  })
}

fn relation_metadata(
  relations: List(RelationDefinition),
) -> List(relation.Relation) {
  list.map(relations, fn(next_relation) {
    case next_relation.kind {
      BelongsTo ->
        relation.belongs_to(
          next_relation.name,
          next_relation.foreign_key_name,
          next_relation.related_table,
          next_relation.column_pairs,
        )
      HasMany ->
        relation.has_many(
          next_relation.name,
          next_relation.foreign_key_name,
          next_relation.related_table,
          next_relation.column_pairs,
        )
    }
  })
}

fn foreign_keys(relations: List(RelationDefinition)) -> List(model.ForeignKey) {
  list.filter_map(relations, fn(next_relation) {
    case next_relation.kind {
      BelongsTo ->
        Ok(model.ForeignKey(
          name: next_relation.foreign_key_name,
          columns: list.map(next_relation.column_pairs, fn(next_pair) {
            next_pair.local_column
          }),
          referenced_schema: next_relation.related_table.schema,
          referenced_table: next_relation.related_table.name,
          referenced_columns: list.map(
            next_relation.column_pairs,
            fn(next_pair) { next_pair.related_column },
          ),
        ))
      HasMany -> Error(Nil)
    }
  })
}

fn table_schemas(
  models: List(Model),
  acc: List(model.TableSchema),
) -> Result(List(model.TableSchema), DeclarativeError) {
  case models {
    [] -> Ok(acc)
    [next_model, ..rest] -> {
      use next_table <- result_try(to_table_schema(next_model))
      table_schemas(rest, [next_table, ..acc])
    }
  }
}

fn has_column(columns: List(Column), column_name: String) -> Bool {
  case columns {
    [] -> False
    [next_column, ..rest] -> {
      case next_column.name == column_name {
        True -> True
        False -> has_column(rest, column_name)
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

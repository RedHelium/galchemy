import galchemy/schema/model
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub type TableRef {
  TableRef(schema: String, name: String)
}

pub type RelationKind {
  BelongsTo
  HasMany
}

pub type ColumnPair {
  ColumnPair(local_column: String, related_column: String)
}

pub type Relation {
  Relation(
    name: String,
    foreign_key_name: String,
    kind: RelationKind,
    related_table: TableRef,
    column_pairs: List(ColumnPair),
  )
}

pub type TableRelations {
  TableRelations(table: TableRef, relations: List(Relation))
}

pub fn table_ref(schema: String, name: String) -> TableRef {
  TableRef(schema: schema, name: name)
}

pub fn pair(local_column: String, related_column: String) -> ColumnPair {
  ColumnPair(local_column: local_column, related_column: related_column)
}

pub fn belongs_to(
  name: String,
  foreign_key_name: String,
  related_table: TableRef,
  column_pairs: List(ColumnPair),
) -> Relation {
  Relation(
    name: name,
    foreign_key_name: foreign_key_name,
    kind: BelongsTo,
    related_table: related_table,
    column_pairs: column_pairs,
  )
}

pub fn has_many(
  name: String,
  foreign_key_name: String,
  related_table: TableRef,
  column_pairs: List(ColumnPair),
) -> Relation {
  Relation(
    name: name,
    foreign_key_name: foreign_key_name,
    kind: HasMany,
    related_table: related_table,
    column_pairs: column_pairs,
  )
}

pub fn infer(snapshot: model.SchemaSnapshot) -> List(TableRelations) {
  let base_relations =
    list.map(snapshot.tables, fn(table_schema) {
      TableRelations(
        table: table_ref(table_schema.schema, table_schema.name),
        relations: [],
      )
    })

  list.fold(
    over: snapshot.tables,
    from: base_relations,
    with: fn(acc, table_schema) {
      infer_table_relations(table_schema, acc)
    },
  )
  |> list.map(normalize_relation_names)
}

pub fn for_table(
  snapshot: model.SchemaSnapshot,
  schema_name: String,
  table_name: String,
) -> Option(TableRelations) {
  find_table_relations(infer(snapshot), table_ref(schema_name, table_name))
}

fn infer_table_relations(
  table_schema: model.TableSchema,
  acc: List(TableRelations),
) -> List(TableRelations) {
  list.fold(
    over: table_schema.foreign_keys,
    from: acc,
    with: fn(inner_acc, foreign_key) {
      let source_table = table_ref(table_schema.schema, table_schema.name)
      let target_table =
        table_ref(foreign_key.referenced_schema, foreign_key.referenced_table)
      let outgoing_relation =
        belongs_to(
          base_belongs_to_name(foreign_key, table_schema.name),
          foreign_key.name,
          target_table,
          zip_columns(foreign_key.columns, foreign_key.referenced_columns),
        )
      let incoming_relation =
        has_many(
          sanitize_name(table_schema.name, "related"),
          foreign_key.name,
          source_table,
          zip_columns(foreign_key.referenced_columns, foreign_key.columns),
        )

      inner_acc
      |> add_relation(source_table, outgoing_relation)
      |> add_relation_if_present(target_table, incoming_relation)
    },
  )
}

fn add_relation(
  relations_by_table: List(TableRelations),
  target_table: TableRef,
  next_relation: Relation,
) -> List(TableRelations) {
  list.map(relations_by_table, fn(table_relations) {
    case table_relations.table == target_table {
      True ->
        TableRelations(
          ..table_relations,
          relations: list.append(table_relations.relations, [next_relation]),
        )

      False -> table_relations
    }
  })
}

fn add_relation_if_present(
  relations_by_table: List(TableRelations),
  target_table: TableRef,
  next_relation: Relation,
) -> List(TableRelations) {
  case find_table_relations(relations_by_table, target_table) {
    Some(_) -> add_relation(relations_by_table, target_table, next_relation)
    None -> relations_by_table
  }
}

fn find_table_relations(
  relations_by_table: List(TableRelations),
  target_table: TableRef,
) -> Option(TableRelations) {
  case relations_by_table {
    [] -> None
    [table_relations, ..rest] -> {
      case table_relations.table == target_table {
        True -> Some(table_relations)
        False -> find_table_relations(rest, target_table)
      }
    }
  }
}

fn normalize_relation_names(table_relations: TableRelations) -> TableRelations {
  let #(reversed_relations, _) =
    list.fold(
      over: table_relations.relations,
      from: #([], []),
      with: fn(acc, relation) {
        let #(normalized, used_names) = acc
        let normalized_name = next_relation_name(relation.name, used_names)

        #(
          [Relation(..relation, name: normalized_name), ..normalized],
          [normalized_name, ..used_names],
        )
      },
    )

  TableRelations(..table_relations, relations: list.reverse(reversed_relations))
}

fn next_relation_name(base_name: String, used_names: List(String)) -> String {
  case list.contains(used_names, base_name) {
    True -> dedupe_relation_name(base_name, used_names, 2)
    False -> base_name
  }
}

fn dedupe_relation_name(
  base_name: String,
  used_names: List(String),
  suffix: Int,
) -> String {
  let candidate = base_name <> "_" <> int.to_string(suffix)

  case list.contains(used_names, candidate) {
    True -> dedupe_relation_name(base_name, used_names, suffix + 1)
    False -> candidate
  }
}

fn base_belongs_to_name(
  foreign_key: model.ForeignKey,
  _source_table_name: String,
) -> String {
  case foreign_key.columns {
    [column_name] -> column_name_to_relation_name(column_name, foreign_key.referenced_table)
    _ -> singularize_table_name(foreign_key.referenced_table, foreign_key.referenced_table)
  }
}

fn column_name_to_relation_name(
  column_name: String,
  fallback_table_name: String,
) -> String {
  case string.ends_with(column_name, "_id") {
    True ->
      column_name
      |> string.drop_end(3)
      |> sanitize_name(fallback_table_name)

    False -> {
      case string.ends_with(column_name, "_uuid") {
        True ->
          column_name
          |> string.drop_end(5)
          |> sanitize_name(fallback_table_name)

        False -> singularize_table_name(fallback_table_name, fallback_table_name)
      }
    }
  }
}

fn singularize_table_name(
  table_name: String,
  fallback_table_name: String,
) -> String {
  let singular =
    case string.ends_with(table_name, "ies") {
      True -> string.drop_end(table_name, 3) <> "y"
      False -> {
        case string.ends_with(table_name, "s") {
          True -> string.drop_end(table_name, 1)
          False -> table_name
        }
      }
    }

  sanitize_name(singular, fallback_table_name)
}

fn zip_columns(
  local_columns: List(String),
  related_columns: List(String),
) -> List(ColumnPair) {
  list.map2(local_columns, related_columns, fn(local, related) {
    pair(local, related)
  })
}

fn sanitize_name(value: String, fallback: String) -> String {
  let characters = string.to_graphemes(string.lowercase(value))
  let normalized =
    list.fold(over: characters, from: "", with: fn(acc, character) {
      case is_identifier_character(character) {
        True -> acc <> character
        False -> acc <> "_"
      }
    })
    |> collapse_underscores
    |> trim_edge_underscores

  case normalized {
    "" -> sanitize_name(fallback, "relation")
    _ -> prefix_if_needed(normalized)
  }
}

fn is_identifier_character(character: String) -> Bool {
  case character {
    "a" -> True
    "b" -> True
    "c" -> True
    "d" -> True
    "e" -> True
    "f" -> True
    "g" -> True
    "h" -> True
    "i" -> True
    "j" -> True
    "k" -> True
    "l" -> True
    "m" -> True
    "n" -> True
    "o" -> True
    "p" -> True
    "q" -> True
    "r" -> True
    "s" -> True
    "t" -> True
    "u" -> True
    "v" -> True
    "w" -> True
    "x" -> True
    "y" -> True
    "z" -> True
    "0" -> True
    "1" -> True
    "2" -> True
    "3" -> True
    "4" -> True
    "5" -> True
    "6" -> True
    "7" -> True
    "8" -> True
    "9" -> True
    "_" -> True
    _ -> False
  }
}

fn collapse_underscores(value: String) -> String {
  case string.contains(value, "__") {
    True ->
      value
      |> string.replace(each: "__", with: "_")
      |> collapse_underscores

    False -> value
  }
}

fn trim_edge_underscores(value: String) -> String {
  value
  |> trim_leading_underscores
  |> trim_trailing_underscores
}

fn trim_leading_underscores(value: String) -> String {
  case string.starts_with(value, "_") {
    True ->
      value
      |> string.drop_start(1)
      |> trim_leading_underscores

    False -> value
  }
}

fn trim_trailing_underscores(value: String) -> String {
  case string.ends_with(value, "_") {
    True ->
      value
      |> string.drop_end(1)
      |> trim_trailing_underscores

    False -> value
  }
}

fn prefix_if_needed(value: String) -> String {
  case string.to_graphemes(value) {
    [first, .._] -> {
      case is_digit(first) {
        True -> "relation_" <> value
        False -> value
      }
    }

    [] -> "relation"
  }
}

fn is_digit(character: String) -> Bool {
  case character {
    "0" -> True
    "1" -> True
    "2" -> True
    "3" -> True
    "4" -> True
    "5" -> True
    "6" -> True
    "7" -> True
    "8" -> True
    "9" -> True
    _ -> False
  }
}

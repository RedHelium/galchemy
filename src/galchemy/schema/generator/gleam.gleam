import galchemy/schema/model
import gleam/int
import gleam/list
import gleam/string

pub type GeneratorOptions {
  GeneratorOptions(root_module: String, include_schema_segment: Bool)
}

pub type GeneratedModule {
  GeneratedModule(module_path: String, file_path: String, source: String)
}

pub fn default_options(root_module: String) -> GeneratorOptions {
  GeneratorOptions(root_module: root_module, include_schema_segment: True)
}

pub fn without_schema_segment(options: GeneratorOptions) -> GeneratorOptions {
  GeneratorOptions(..options, include_schema_segment: False)
}

pub fn generate(
  snapshot: model.SchemaSnapshot,
  options: GeneratorOptions,
) -> List(GeneratedModule) {
  list.map(snapshot.tables, fn(table_schema) {
    generate_table(table_schema, options)
  })
}

pub fn generate_table(
  table_schema: model.TableSchema,
  options: GeneratorOptions,
) -> GeneratedModule {
  let module_segments = module_segments(table_schema, options)
  let module_path = string.join(module_segments, with: "/")

  GeneratedModule(
    module_path: module_path,
    file_path: "src/" <> module_path <> ".gleam",
    source: render_module_source(table_schema),
  )
}

fn module_segments(
  table_schema: model.TableSchema,
  options: GeneratorOptions,
) -> List(String) {
  let root_segments = normalize_root_segments(options.root_module)
  let table_segment = sanitize_identifier(table_schema.name, "table")

  case options.include_schema_segment {
    True ->
      list.append(
        root_segments,
        [sanitize_identifier(table_schema.schema, "schema"), table_segment],
      )

    False -> list.append(root_segments, [table_segment])
  }
}

fn normalize_root_segments(root_module: String) -> List(String) {
  let normalized_root = string.replace(in: root_module, each: ".", with: "/")
  let raw_segments = string.split(normalized_root, on: "/")
  let segments =
    list.fold(over: raw_segments, from: [], with: fn(acc, segment) {
      case segment {
        "" -> acc
        _ -> list.append(acc, [sanitize_identifier(segment, "module")])
      }
    })

  case segments {
    [] -> ["generated"]
    _ -> segments
  }
}

fn render_module_source(table_schema: model.TableSchema) -> String {
  let column_blocks = render_column_blocks(table_schema.columns)
  let base_blocks = [
    "import galchemy/dsl/table",
    render_table_function(table_schema),
    render_alias_function(),
  ]

  string.join(list.append(base_blocks, column_blocks), with: "\n\n")
  <> "\n"
}

fn render_table_function(table_schema: model.TableSchema) -> String {
  string.join(
    [
      "pub fn table_() {",
      "  table.table(\"" <> escape_string(table_schema.name) <> "\")",
      "  |> table.in_schema(\"" <> escape_string(table_schema.schema) <> "\")",
      "}",
    ],
    with: "\n",
  )
}

fn render_alias_function() -> String {
  string.join(
    [
      "pub fn as_(alias: String) {",
      "  table_()",
      "  |> table.as_(alias)",
      "}",
    ],
    with: "\n",
  )
}

fn render_column_blocks(columns: List(model.ColumnSchema)) -> List(String) {
  let #(reversed_blocks, _) =
    list.fold(over: columns, from: #([], []), with: fn(acc, column) {
      let #(blocks, used_names) = acc
      let function_name = next_function_name(column.name, used_names)

      #(
        [render_column_function(function_name, helper_name(column.data_type), column.name), ..blocks],
        [function_name, ..used_names],
      )
    })

  list.reverse(reversed_blocks)
}

fn render_column_function(
  function_name: String,
  helper: String,
  column_name: String,
) -> String {
  string.join(
    [
      "pub fn " <> function_name <> "(table_ref) {",
      "  table." <> helper <> "(table_ref, \"" <> escape_string(column_name) <> "\")",
      "}",
    ],
    with: "\n",
  )
}

fn helper_name(data_type: model.ColumnType) -> String {
  case data_type {
    model.SmallIntType -> "int"
    model.IntegerType -> "int"
    model.BigIntType -> "int"
    model.BooleanType -> "bool"
    model.TextType -> "text"
    model.VarCharType(_) -> "text"
    model.UuidType -> "text"
    model.RealType -> "float"
    model.DoublePrecisionType -> "float"
    model.TimestampType(_) -> "timestamp"
    model.TimeType(_) -> "time_of_day"
    model.DateType -> "date"
    _ -> "column"
  }
}

fn next_function_name(column_name: String, used_names: List(String)) -> String {
  let base_name = sanitize_function_name(column_name)

  case list.contains(used_names, base_name) {
    True -> dedupe_function_name(base_name, used_names, 2)
    False -> base_name
  }
}

fn dedupe_function_name(
  base_name: String,
  used_names: List(String),
  suffix: Int,
) -> String {
  let candidate = base_name <> "_" <> int.to_string(suffix)

  case list.contains(used_names, candidate) {
    True -> dedupe_function_name(base_name, used_names, suffix + 1)
    False -> candidate
  }
}

fn sanitize_function_name(name: String) -> String {
  let sanitized = sanitize_identifier(name, "column")

  case list.contains(reserved_function_names(), sanitized) {
    True -> sanitized <> "_"
    False -> sanitized
  }
}

fn sanitize_identifier(name: String, fallback: String) -> String {
  let lowercased = string.lowercase(name)
  let characters = string.to_graphemes(lowercased)
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
    "" -> fallback
    _ -> prefix_if_needed(normalized, fallback)
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

fn prefix_if_needed(value: String, fallback: String) -> String {
  case starts_with_digit(value) {
    True -> fallback <> "_" <> value
    False -> value
  }
}

fn starts_with_digit(value: String) -> Bool {
  case string.to_graphemes(value) {
    [first, .._] -> is_digit(first)
    [] -> False
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

fn reserved_function_names() -> List(String) {
  [
    "as",
    "assert",
    "case",
    "const",
    "echo",
    "external",
    "fn",
    "if",
    "import",
    "let",
    "opaque",
    "panic",
    "pub",
    "table",
    "table_",
    "test",
    "todo",
    "type",
    "use",
  ]
}

fn escape_string(value: String) -> String {
  value
  |> string.replace(each: "\\", with: "\\\\")
  |> string.replace(each: "\"", with: "\\\"")
}

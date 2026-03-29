import galchemy/schema/generator/gleam as generator
import galchemy/schema/model
import gleam/option
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn generate_table_module_test() {
  let table_schema =
    model.TableSchema(
      schema: "public",
      name: "users",
      columns: [
        column("id", model.IntegerType, False, option.None, 1),
        column(
          "email",
          model.VarCharType(length: option.Some(255)),
          False,
          option.None,
          2,
        ),
        column("active", model.BooleanType, False, option.None, 3),
        column("score", model.RealType, True, option.None, 4),
        column(
          "inserted_at",
          model.TimestampType(with_time_zone: True),
          False,
          option.None,
          5,
        ),
        column("birth_date", model.DateType, True, option.None, 6),
        column(
          "login_time",
          model.TimeType(with_time_zone: False),
          True,
          option.None,
          7,
        ),
        column("metadata", model.JsonbType, True, option.None, 8),
      ],
      primary_key: option.None,
      unique_constraints: [],
      foreign_keys: [],
      indexes: [],
    )

  assert generator.generate_table(
    table_schema,
    generator.default_options("app/generated"),
  )
    == generator.GeneratedModule(
      module_path: "app/generated/public/users",
      file_path: "src/app/generated/public/users.gleam",
      source:
        "import galchemy/dsl/table\n\n"
        <> "pub fn table_() {\n"
        <> "  table.table(\"users\")\n"
        <> "  |> table.in_schema(\"public\")\n"
        <> "}\n\n"
        <> "pub fn as_(alias: String) {\n"
        <> "  table_()\n"
        <> "  |> table.as_(alias)\n"
        <> "}\n\n"
        <> "pub fn id(table_ref) {\n"
        <> "  table.int(table_ref, \"id\")\n"
        <> "}\n\n"
        <> "pub fn email(table_ref) {\n"
        <> "  table.text(table_ref, \"email\")\n"
        <> "}\n\n"
        <> "pub fn active(table_ref) {\n"
        <> "  table.bool(table_ref, \"active\")\n"
        <> "}\n\n"
        <> "pub fn score(table_ref) {\n"
        <> "  table.float(table_ref, \"score\")\n"
        <> "}\n\n"
        <> "pub fn inserted_at(table_ref) {\n"
        <> "  table.timestamp(table_ref, \"inserted_at\")\n"
        <> "}\n\n"
        <> "pub fn birth_date(table_ref) {\n"
        <> "  table.date(table_ref, \"birth_date\")\n"
        <> "}\n\n"
        <> "pub fn login_time(table_ref) {\n"
        <> "  table.time_of_day(table_ref, \"login_time\")\n"
        <> "}\n\n"
        <> "pub fn metadata(table_ref) {\n"
        <> "  table.column(table_ref, \"metadata\")\n"
        <> "}\n",
    )
}

pub fn generate_table_module_with_sanitized_names_test() {
  let table_schema =
    model.TableSchema(
      schema: "crm-core",
      name: "audit-log",
      columns: [
        column("2fa_enabled", model.BooleanType, False, option.None, 1),
        column("type", model.TextType, False, option.None, 2),
        column("display-name", model.TextType, False, option.None, 3),
        column("user-id", model.IntegerType, False, option.None, 4),
        column("user_id", model.IntegerType, False, option.None, 5),
      ],
      primary_key: option.None,
      unique_constraints: [],
      foreign_keys: [],
      indexes: [],
    )

  assert generator.generate_table(
    table_schema,
    generator.default_options("app.generated"),
  )
    == generator.GeneratedModule(
      module_path: "app/generated/crm_core/audit_log",
      file_path: "src/app/generated/crm_core/audit_log.gleam",
      source:
        "import galchemy/dsl/table\n\n"
        <> "pub fn table_() {\n"
        <> "  table.table(\"audit-log\")\n"
        <> "  |> table.in_schema(\"crm-core\")\n"
        <> "}\n\n"
        <> "pub fn as_(alias: String) {\n"
        <> "  table_()\n"
        <> "  |> table.as_(alias)\n"
        <> "}\n\n"
        <> "pub fn column_2fa_enabled(table_ref) {\n"
        <> "  table.bool(table_ref, \"2fa_enabled\")\n"
        <> "}\n\n"
        <> "pub fn type_(table_ref) {\n"
        <> "  table.text(table_ref, \"type\")\n"
        <> "}\n\n"
        <> "pub fn display_name(table_ref) {\n"
        <> "  table.text(table_ref, \"display-name\")\n"
        <> "}\n\n"
        <> "pub fn user_id(table_ref) {\n"
        <> "  table.int(table_ref, \"user-id\")\n"
        <> "}\n\n"
        <> "pub fn user_id_2(table_ref) {\n"
        <> "  table.int(table_ref, \"user_id\")\n"
        <> "}\n",
    )
}

pub fn generate_snapshot_without_schema_segment_test() {
  let snapshot =
    model.SchemaSnapshot(tables: [
      model.TableSchema(
        schema: "public",
        name: "users",
        columns: [column("id", model.IntegerType, False, option.None, 1)],
        primary_key: option.None,
        unique_constraints: [],
        foreign_keys: [],
        indexes: [],
      ),
      model.TableSchema(
        schema: "analytics",
        name: "events",
        columns: [column("id", model.IntegerType, False, option.None, 1)],
        primary_key: option.None,
        unique_constraints: [],
        foreign_keys: [],
        indexes: [],
      ),
    ])

  assert generator.generate(
    snapshot,
    generator.default_options("my_app/db")
    |> generator.without_schema_segment,
  )
    == [
      generator.GeneratedModule(
        module_path: "my_app/db/users",
        file_path: "src/my_app/db/users.gleam",
        source:
          "import galchemy/dsl/table\n\n"
          <> "pub fn table_() {\n"
          <> "  table.table(\"users\")\n"
          <> "  |> table.in_schema(\"public\")\n"
          <> "}\n\n"
          <> "pub fn as_(alias: String) {\n"
          <> "  table_()\n"
          <> "  |> table.as_(alias)\n"
          <> "}\n\n"
          <> "pub fn id(table_ref) {\n"
          <> "  table.int(table_ref, \"id\")\n"
          <> "}\n",
      ),
      generator.GeneratedModule(
        module_path: "my_app/db/events",
        file_path: "src/my_app/db/events.gleam",
        source:
          "import galchemy/dsl/table\n\n"
          <> "pub fn table_() {\n"
          <> "  table.table(\"events\")\n"
          <> "  |> table.in_schema(\"analytics\")\n"
          <> "}\n\n"
          <> "pub fn as_(alias: String) {\n"
          <> "  table_()\n"
          <> "  |> table.as_(alias)\n"
          <> "}\n\n"
          <> "pub fn id(table_ref) {\n"
          <> "  table.int(table_ref, \"id\")\n"
          <> "}\n",
      ),
    ]
}

fn column(
  name: String,
  data_type: model.ColumnType,
  nullable: Bool,
  default: option.Option(String),
  ordinal_position: Int,
) -> model.ColumnSchema {
  model.ColumnSchema(name:, data_type:, nullable:, default:, ordinal_position:)
}

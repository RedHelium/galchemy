import galchemy/schema/diff
import galchemy/schema/generator/gleam as generator
import galchemy/schema/migration/postgres as migration
import galchemy/schema/model
import gleam/io
import gleam/option
import gleam/string

pub fn current_schema() -> model.SchemaSnapshot {
  model.SchemaSnapshot(tables: [
    model.TableSchema(
      schema: "public",
      name: "users",
      columns: [
        column("id", model.IntegerType, False, option.None, 1),
        column("name", model.TextType, False, option.None, 2),
      ],
      primary_key: option.Some(
        model.PrimaryKey(name: "users_pkey", columns: ["id"]),
      ),
      unique_constraints: [],
      foreign_keys: [],
      indexes: [],
    ),
  ])
}

pub fn target_schema() -> model.SchemaSnapshot {
  model.SchemaSnapshot(tables: [
    model.TableSchema(
      schema: "public",
      name: "users",
      columns: [
        column("id", model.IntegerType, False, option.None, 1),
        column("name", model.TextType, False, option.None, 2),
        column("email", model.TextType, False, option.None, 3),
      ],
      primary_key: option.Some(
        model.PrimaryKey(name: "users_pkey", columns: ["id"]),
      ),
      unique_constraints: [
        model.UniqueConstraint(name: "users_email_key", columns: ["email"]),
      ],
      foreign_keys: [],
      indexes: [],
    ),
  ])
}

pub fn main() -> Nil {
  let operations = diff.diff(current_schema(), target_schema())
  let modules =
    generator.generate(
      target_schema(),
      generator.default_options("app/generated"),
    )

  io.println("Schema tooling example")
  io.println("Diff operations:")
  io.println(string.inspect(operations))
  io.println("Generated modules:")
  io.println(string.inspect(modules))

  case migration.plan("20260329_add_user_email", current_schema(), target_schema()) {
    Ok(plan) -> {
      io.println("Migration plan:")
      io.println(string.inspect(plan))
    }
    Error(error) -> {
      io.println("Migration plan error:")
      io.println(string.inspect(error))
    }
  }
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

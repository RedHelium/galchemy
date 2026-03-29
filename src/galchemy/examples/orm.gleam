import galchemy/ast/expression as ast_expression
import galchemy/orm/entity
import galchemy/orm/metadata
import galchemy/schema/model
import galchemy/session/unit_of_work
import gleam/io
import gleam/option
import gleam/string

pub fn main() -> Nil {
  let snapshot = blog_snapshot()
  let users_metadata =
    case metadata.from_snapshot(snapshot, "public", "users") {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let inserted_user =
    case entity.new_(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(1)),
        unit_of_work.field("name", ast_expression.Text("Ann")),
      ],
    ) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  let updated_user =
    case entity.materialize(
      users_metadata,
      [
        unit_of_work.field("id", ast_expression.Int(2)),
        unit_of_work.field("name", ast_expression.Text("Bob")),
      ],
    ) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }
    |> entity.change([unit_of_work.field("name", ast_expression.Text("Bobby"))])
    |> unwrap_entity

  let session =
    unit_of_work.new(snapshot)
    |> entity.stage(inserted_user)
    |> unwrap_session
    |> entity.stage(updated_user)
    |> unwrap_session

  let flush_plan =
    case unit_of_work.flush_plan(session) {
      Ok(value) -> value
      Error(error) -> panic as string.inspect(error)
    }

  io.println("ORM example")
  io.println(string.inspect(users_metadata))
  io.println(string.inspect(flush_plan))
}

fn unwrap_entity(
  result: Result(entity.Entity, entity.EntityError),
) -> entity.Entity {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn unwrap_session(
  result: Result(unit_of_work.Session, entity.EntityError),
) -> unit_of_work.Session {
  case result {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn blog_snapshot() -> model.SchemaSnapshot {
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

fn column(
  name: String,
  data_type: model.ColumnType,
  nullable: Bool,
  default: option.Option(String),
  ordinal_position: Int,
) -> model.ColumnSchema {
  model.ColumnSchema(name:, data_type:, nullable:, default:, ordinal_position:)
}

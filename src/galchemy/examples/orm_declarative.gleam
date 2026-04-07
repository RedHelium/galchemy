import galchemy
import galchemy/ast/expression
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/orm/codec
import galchemy/orm/declarative
import galchemy/orm/mapper_registry
import galchemy/orm/materializer
import galchemy/orm/query as orm_query
import galchemy/orm/result as orm_result
import galchemy/orm/runtime_registry
import gleam/io
import gleam/string

pub fn main() -> Nil {
  let users = users_model()
  let posts = posts_model()
  let snapshot = unwrap(declarative.to_snapshot([users, posts]))
  let registry = unwrap(runtime_registry.from_models([users, posts]))
  let users_ref = unwrap(orm_query.from_model(users)) |> orm_query.as_("u")
  let posts_ref = unwrap(orm_query.from_model(posts)) |> orm_query.as_("p")

  let base_query =
    unwrap(
      orm_query.select_fields(users_ref, [
        orm_query.field_as("id", "user_id"),
        orm_query.field_as("name", "user_name"),
        orm_query.field_as("email", "email"),
      ]),
    )
  let joined_query =
    unwrap(orm_query.left_join_relation(
      base_query,
      users_ref,
      "posts",
      posts_ref,
    ))
  let name_filter = unwrap(orm_query.ilike(users_ref, "name", expr.text("A%")))
  let order_by_id = unwrap(orm_query.asc(users_ref, "id"))

  let final_query =
    joined_query
    |> orm_query.where_(name_filter)
    |> orm_query.order_by(order_by_id)
    |> orm_query.limit(20)

  let mapper =
    orm_result.tuple2(
      orm_result.scalar_as("user_id", codec.int()),
      orm_result.scalar_as("user_name", codec.text()),
    )
  let sample_row =
    orm_result.row(
      [
        orm_result.scalar("user_id", expression.Int(1)),
        orm_result.scalar("user_name", expression.Text("Ann")),
      ],
      [],
    )
  let #(mapped_row, _) =
    unwrap(orm_result.one(
      mapper,
      sample_row,
      materializer.new(mapper_registry.empty()),
    ))

  io.println("ORM declarative example")
  io.println("Schema snapshot:")
  io.println(string.inspect(snapshot))
  io.println("Runtime registry models:")
  io.println(string.inspect(runtime_registry.all(registry)))
  io.println("Compiled model-first query:")
  io.println(string.inspect(galchemy.compile(query.Select(final_query))))
  io.println("Mapped scalar row:")
  io.println(string.inspect(mapped_row))
}

fn users_model() -> declarative.Model {
  declarative.model_(
    "public",
    "users",
    [
      declarative.int("id") |> declarative.primary_key,
      declarative.text("name"),
      declarative.text("email") |> declarative.unique,
    ],
    [
      declarative.has_many("posts", "posts_user_id_fkey", "public", "posts", [
        declarative.pair("id", "user_id"),
      ]),
    ],
  )
}

fn posts_model() -> declarative.Model {
  declarative.model_(
    "public",
    "posts",
    [
      declarative.int("id") |> declarative.primary_key,
      declarative.int("user_id"),
      declarative.text("title"),
      declarative.bool("published"),
    ],
    [
      declarative.belongs_to("user", "posts_user_id_fkey", "public", "users", [
        declarative.pair("user_id", "id"),
      ]),
    ],
  )
}

fn unwrap(value: Result(a, e)) -> a {
  case value {
    Ok(inner) -> inner
    Error(error) -> panic as string.inspect(error)
  }
}

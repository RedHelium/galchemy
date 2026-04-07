import galchemy/ast/expression
import galchemy/orm/codec
import galchemy/orm/declarative
import galchemy/orm/mapper_registry
import galchemy/orm/materializer
import galchemy/orm/result
import galchemy/schema/model
import gleam/bit_array
import gleam/option
import gleam/string
import gleam/time/calendar
import gleam/time/timestamp
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn scalar_codecs_encode_and_decode_test() {
  assert codec.encode(codec.int(), 42) == expression.Int(42)
  assert codec.decode(codec.int(), expression.Int(42)) == Ok(42)

  assert codec.encode(codec.text(), "hello") == expression.Text("hello")
  assert codec.decode(codec.text(), expression.Text("hello")) == Ok("hello")

  assert codec.encode(codec.float(), 3.5) == expression.Float(3.5)
  assert codec.decode(codec.float(), expression.Float(3.5)) == Ok(3.5)

  assert codec.encode(codec.bool(), True) == expression.Bool(True)
  assert codec.decode(codec.bool(), expression.Bool(True)) == Ok(True)
}

pub fn extended_codecs_encode_and_decode_test() {
  let payload = bit_array.from_string("{\"name\":\"Ann\"}")

  assert codec.encode(codec.bytea(), payload) == expression.Bytea(payload)
  assert codec.decode(codec.bytea(), expression.Bytea(payload)) == Ok(payload)

  assert codec.encode(codec.uuid(), "550e8400-e29b-41d4-a716-446655440000")
    == expression.Uuid("550e8400-e29b-41d4-a716-446655440000")
  assert codec.decode(
      codec.uuid(),
      expression.Uuid("550e8400-e29b-41d4-a716-446655440000"),
    )
    == Ok("550e8400-e29b-41d4-a716-446655440000")

  assert codec.encode(codec.numeric(), "123.45")
    == expression.Numeric("123.45")
  assert codec.decode(codec.numeric(), expression.Numeric("123.45"))
    == Ok("123.45")

  assert codec.encode(codec.json(), "{\"name\":\"Ann\"}")
    == expression.Json("{\"name\":\"Ann\"}")
  assert codec.decode(codec.json(), expression.Json("{\"name\":\"Ann\"}"))
    == Ok("{\"name\":\"Ann\"}")

  assert codec.encode(codec.jsonb(), "{\"name\":\"Ann\"}")
    == expression.Jsonb("{\"name\":\"Ann\"}")
  assert codec.decode(codec.jsonb(), expression.Jsonb("{\"name\":\"Ann\"}"))
    == Ok("{\"name\":\"Ann\"}")
}

pub fn temporal_and_nullable_codecs_test() {
  let ts = timestamp.from_unix_seconds_and_nanoseconds(1_744_207_230, 123)
  let date = calendar.Date(year: 2026, month: calendar.April, day: 8)
  let time =
    calendar.TimeOfDay(
      hours: 14,
      minutes: 5,
      seconds: 33,
      nanoseconds: 456,
    )

  assert codec.encode(codec.timestamp(), ts) == expression.Timestamp(ts)
  assert codec.decode(codec.timestamp(), expression.Timestamp(ts)) == Ok(ts)

  assert codec.encode(codec.date(), date) == expression.Date(date)
  assert codec.decode(codec.date(), expression.Date(date)) == Ok(date)

  assert codec.encode(codec.time_of_day(), time) == expression.TimeOfDay(time)
  assert codec.decode(codec.time_of_day(), expression.TimeOfDay(time)) == Ok(time)

  let nullable_text = codec.nullable(codec.text())

  assert codec.encode(nullable_text, option.Some("ann")) == expression.Text("ann")
  assert codec.encode(nullable_text, option.None) == expression.Null
  assert codec.decode(nullable_text, expression.Text("ann"))
    == Ok(option.Some("ann"))
  assert codec.decode(nullable_text, expression.Null) == Ok(option.None)
}

pub fn codec_value_helper_test() {
  assert codec.value(codec.int(), 7) == expression.ValueExpr(expression.Int(7))
  assert codec.value(codec.text(), "galchemy")
    == expression.ValueExpr(expression.Text("galchemy"))
}

pub fn scalar_as_maps_typed_values_test() {
  let #(count, _) =
    result.one(
      result.scalar_as("post_count", codec.int()),
      result.row([
        result.scalar("post_count", expression.Int(3)),
      ], []),
      materializer.new(empty_registry()),
    )
    |> expect_mapped

  assert count == 3
}

pub fn scalar_as_returns_codec_error_test() {
  let mapped =
    result.one(
      result.scalar_as("post_count", codec.int()),
      result.row([
        result.scalar("post_count", expression.Text("oops")),
      ], []),
      materializer.new(empty_registry()),
    )

  assert mapped
    == Error(result.CodecError(
      codec.UnexpectedType(expected: "Int", actual: "Text"),
    ))
}

pub fn custom_codecs_can_be_mapped_and_reused_in_declarative_models_test() {
  let user_id_type =
    codec.define(
      "user_id",
      codec.map(
        codec.int(),
        fn(value) { Ok(UserId(value)) },
        fn(value) {
          let UserId(inner) = value
          inner
        },
      ),
    )

  assert codec.sql_type_name(user_id_type) == "user_id"
  assert codec.encode(codec.codec(user_id_type), UserId(7)) == expression.Int(7)
  assert codec.decode(codec.codec(user_id_type), expression.Int(7))
    == Ok(UserId(7))

  let next_model =
    declarative.model_(
      "public",
      "accounts",
      [
        declarative.primary_key(declarative.custom_with("id", user_id_type)),
        declarative.text("name"),
      ],
      [],
    )
  let table_schema = case declarative.to_table_schema(next_model) {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }

  assert table_schema.columns
    == [
      model.ColumnSchema(
        name: "id",
        data_type: model.CustomType("user_id"),
        nullable: False,
        default: option.None,
        ordinal_position: 1,
      ),
      model.ColumnSchema(
        name: "name",
        data_type: model.TextType,
        nullable: False,
        default: option.None,
        ordinal_position: 2,
      ),
    ]
}

pub fn array_and_enum_codecs_test() {
  let int_array = codec.array(codec.int())
  let role_enum = codec.enum_("user_role")

  assert codec.encode(int_array, [1, 2, 3])
    == expression.Array([
      expression.Int(1),
      expression.Int(2),
      expression.Int(3),
    ])
  assert codec.decode(
      int_array,
      expression.Array([
        expression.Int(1),
        expression.Int(2),
        expression.Int(3),
      ]),
    )
    == Ok([1, 2, 3])

  assert codec.sql_type_name(role_enum) == "user_role"
  assert codec.encode(codec.codec(role_enum), "admin")
    == expression.Enum(type_name: "user_role", value: "admin")
  assert codec.decode(
      codec.codec(role_enum),
      expression.Enum(type_name: "user_role", value: "admin"),
    )
    == Ok("admin")
}

type UserId {
  UserId(Int)
}

fn empty_registry() -> mapper_registry.MapperRegistry {
  case mapper_registry.from_snapshot(model.SchemaSnapshot(tables: [])) {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

fn expect_mapped(
  result_: Result(#(a, materializer.Materializer), result.MappingError),
) -> #(a, materializer.Materializer) {
  case result_ {
    Ok(value) -> value
    Error(error) -> panic as string.inspect(error)
  }
}

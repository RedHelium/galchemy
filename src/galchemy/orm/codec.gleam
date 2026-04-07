import galchemy/ast/expression
import gleam/list
import gleam/option
import gleam/time/calendar.{type Date, type TimeOfDay}
import gleam/time/timestamp.{type Timestamp}

pub type Codec(a) {
  Codec(
    encode: fn(a) -> expression.SqlValue,
    decode: fn(expression.SqlValue) -> Result(a, CodecError),
  )
}

pub type CustomCodec(a) {
  CustomCodec(sql_type_name: String, codec: Codec(a))
}

pub type CodecError {
  UnexpectedType(expected: String, actual: String)
  Custom(message: String)
}

pub fn custom(
  encoder: fn(a) -> expression.SqlValue,
  decoder: fn(expression.SqlValue) -> Result(a, CodecError),
) -> Codec(a) {
  Codec(encode: encoder, decode: decoder)
}

pub fn map(
  base: Codec(a),
  decoder_map: fn(a) -> Result(b, CodecError),
  encoder_map: fn(b) -> a,
) -> Codec(b) {
  Codec(
    encode: fn(value) { encode(base, encoder_map(value)) },
    decode: fn(value) {
      use decoded <- result_try(decode(base, value))
      decoder_map(decoded)
    },
  )
}

pub fn define(sql_type_name: String, next_codec: Codec(a)) -> CustomCodec(a) {
  CustomCodec(sql_type_name: sql_type_name, codec: next_codec)
}

pub fn codec(next_custom_codec: CustomCodec(a)) -> Codec(a) {
  let CustomCodec(sql_type_name: _, codec: inner_codec) = next_custom_codec
  inner_codec
}

pub fn sql_type_name(next_custom_codec: CustomCodec(a)) -> String {
  let CustomCodec(sql_type_name: inner_name, codec: _) = next_custom_codec
  inner_name
}

pub fn int() -> Codec(Int) {
  Codec(encode: expression.Int, decode: fn(value) {
    case value {
      expression.Int(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Int", actual: type_name(value)))
    }
  })
}

pub fn text() -> Codec(String) {
  Codec(encode: expression.Text, decode: fn(value) {
    case value {
      expression.Text(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Text", actual: type_name(value)))
    }
  })
}

pub fn float() -> Codec(Float) {
  Codec(encode: expression.Float, decode: fn(value) {
    case value {
      expression.Float(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Float", actual: type_name(value)))
    }
  })
}

pub fn bytea() -> Codec(BitArray) {
  Codec(encode: expression.Bytea, decode: fn(value) {
    case value {
      expression.Bytea(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Bytea", actual: type_name(value)))
    }
  })
}

pub fn uuid() -> Codec(String) {
  Codec(encode: expression.Uuid, decode: fn(value) {
    case value {
      expression.Uuid(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Uuid", actual: type_name(value)))
    }
  })
}

pub fn numeric() -> Codec(String) {
  Codec(encode: expression.Numeric, decode: fn(value) {
    case value {
      expression.Numeric(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Numeric", actual: type_name(value)))
    }
  })
}

pub fn json() -> Codec(String) {
  Codec(encode: expression.Json, decode: fn(value) {
    case value {
      expression.Json(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Json", actual: type_name(value)))
    }
  })
}

pub fn jsonb() -> Codec(String) {
  Codec(encode: expression.Jsonb, decode: fn(value) {
    case value {
      expression.Jsonb(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Jsonb", actual: type_name(value)))
    }
  })
}

pub fn bool() -> Codec(Bool) {
  Codec(encode: expression.Bool, decode: fn(value) {
    case value {
      expression.Bool(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Bool", actual: type_name(value)))
    }
  })
}

pub fn timestamp() -> Codec(Timestamp) {
  Codec(encode: expression.Timestamp, decode: fn(value) {
    case value {
      expression.Timestamp(inner) -> Ok(inner)
      _ ->
        Error(UnexpectedType(expected: "Timestamp", actual: type_name(value)))
    }
  })
}

pub fn date() -> Codec(Date) {
  Codec(encode: expression.Date, decode: fn(value) {
    case value {
      expression.Date(inner) -> Ok(inner)
      _ -> Error(UnexpectedType(expected: "Date", actual: type_name(value)))
    }
  })
}

pub fn time_of_day() -> Codec(TimeOfDay) {
  Codec(encode: expression.TimeOfDay, decode: fn(value) {
    case value {
      expression.TimeOfDay(inner) -> Ok(inner)
      _ ->
        Error(UnexpectedType(expected: "TimeOfDay", actual: type_name(value)))
    }
  })
}

pub fn array(inner: Codec(a)) -> Codec(List(a)) {
  Codec(
    encode: fn(values) {
      expression.Array(list.map(values, fn(value) { encode(inner, value) }))
    },
    decode: fn(value) {
      case value {
        expression.Array(values) ->
          list.try_map(values, fn(item) { decode(inner, item) })
        _ -> Error(UnexpectedType(expected: "Array", actual: type_name(value)))
      }
    },
  )
}

pub fn enum_(enum_type_name: String) -> CustomCodec(String) {
  define(
    enum_type_name,
    Codec(
      encode: fn(value) {
        expression.Enum(type_name: enum_type_name, value: value)
      },
      decode: fn(value) {
        case value {
          expression.Enum(type_name: inner_type, value: inner_value) ->
            case inner_type == enum_type_name {
              True -> Ok(inner_value)
              False ->
                Error(Custom(
                  "Expected enum type "
                  <> enum_type_name
                  <> ", got "
                  <> inner_type,
                ))
            }
          _ -> Error(UnexpectedType(expected: "Enum", actual: type_name(value)))
        }
      },
    ),
  )
}

pub fn nullable(inner: Codec(a)) -> Codec(option.Option(a)) {
  Codec(
    encode: fn(value) {
      case value {
        option.Some(inner_value) -> encode(inner, inner_value)
        option.None -> expression.Null
      }
    },
    decode: fn(value) {
      case value {
        expression.Null -> Ok(option.None)
        _ ->
          case decode(inner, value) {
            Ok(inner_value) -> Ok(option.Some(inner_value))
            Error(error) -> Error(error)
          }
      }
    },
  )
}

pub fn encode(next_codec: Codec(a), value: a) -> expression.SqlValue {
  let Codec(encode: encoder, decode: _) = next_codec
  encoder(value)
}

pub fn decode(
  next_codec: Codec(a),
  value: expression.SqlValue,
) -> Result(a, CodecError) {
  let Codec(encode: _, decode: decoder) = next_codec
  decoder(value)
}

pub fn value(next_codec: Codec(a), inner: a) -> expression.Expression {
  expression.ValueExpr(encode(next_codec, inner))
}

fn type_name(value: expression.SqlValue) -> String {
  case value {
    expression.Text(_) -> "Text"
    expression.Int(_) -> "Int"
    expression.Float(_) -> "Float"
    expression.Bool(_) -> "Bool"
    expression.Bytea(_) -> "Bytea"
    expression.Uuid(_) -> "Uuid"
    expression.Numeric(_) -> "Numeric"
    expression.Json(_) -> "Json"
    expression.Jsonb(_) -> "Jsonb"
    expression.Enum(type_name: _, value: _) -> "Enum"
    expression.Array(_) -> "Array"
    expression.Timestamp(_) -> "Timestamp"
    expression.Date(_) -> "Date"
    expression.TimeOfDay(_) -> "TimeOfDay"
    expression.Null -> "Null"
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

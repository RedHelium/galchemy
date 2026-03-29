import galchemy/ast/expression
import galchemy/ast/query as ast_query
import galchemy/sql/compiler
import gleam/dynamic/decode
import pog

/// Error type for the PostgreSQL adapter.
pub type PostgresError {
  CompileError(compiler.CompileError)
  QueryError(pog.QueryError)
}

/// Converts an SQL AST value into a `pog` parameter value.
pub fn to_pog_value(value: expression.SqlValue) -> pog.Value {
  case value {
    expression.Text(v) -> pog.text(v)
    expression.Int(v) -> pog.int(v)
    expression.Float(v) -> pog.float(v)
    expression.Bool(v) -> pog.bool(v)
    expression.Timestamp(v) -> pog.timestamp(v)
    expression.Date(v) -> pog.calendar_date(v)
    expression.TimeOfDay(v) -> pog.calendar_time_of_day(v)
    expression.Null -> pog.null()
  }
}

/// Adds parameters to a `pog` query while preserving their original order.
pub fn with_params(
  query: pog.Query(t),
  params: List(expression.SqlValue),
) -> pog.Query(t) {
  with_params_loop(query, params)
}

/// Recursively appends parameters to `pog.Query`.
fn with_params_loop(
  query: pog.Query(t),
  params: List(expression.SqlValue),
) -> pog.Query(t) {
  case params {
    [] -> query
    [param, ..rest] -> {
      let next_query = pog.parameter(query, to_pog_value(param))
      with_params_loop(next_query, rest)
    }
  }
}

/// Builds a `pog` query from already compiled SQL.
pub fn to_query_from_compiled(
  compiled: compiler.CompiledQuery,
) -> pog.Query(Nil) {
  let compiler.CompiledQuery(sql: sql, params: params) = compiled
  pog.query(sql)
  |> with_params(params)
}

/// Compiles an AST query and converts it into a `pog` query.
pub fn to_query(
  query: ast_query.Query,
) -> Result(pog.Query(Nil), compiler.CompileError) {
  to_query_with(query, compiler.default_config())
}

pub fn to_query_with(
  query: ast_query.Query,
  config: compiler.CompilerConfig,
) -> Result(pog.Query(Nil), compiler.CompileError) {
  case compiler.compile_with(query, config) {
    Ok(compiled) -> Ok(to_query_from_compiled(compiled))
    Error(error) -> Error(error)
  }
}

/// Executes an AST query without row decoding.
pub fn execute(
  query: ast_query.Query,
  connection: pog.Connection,
) -> Result(pog.Returned(Nil), PostgresError) {
  execute_with_config(query, compiler.default_config(), connection)
}

pub fn execute_with_config(
  query: ast_query.Query,
  config: compiler.CompilerConfig,
  connection: pog.Connection,
) -> Result(pog.Returned(Nil), PostgresError) {
  case to_query_with(query, config) {
    Ok(compiled_query) -> {
      case pog.execute(compiled_query, on: connection) {
        Ok(returned) -> Ok(returned)
        Error(error) -> Error(QueryError(error))
      }
    }
    Error(error) -> Error(CompileError(error))
  }
}

/// Executes an AST query using a row decoder.
pub fn execute_with(
  query: ast_query.Query,
  decoder: decode.Decoder(row),
  connection: pog.Connection,
) -> Result(pog.Returned(row), PostgresError) {
  execute_with_decoder_config(
    query,
    decoder,
    compiler.default_config(),
    connection,
  )
}

pub fn execute_with_decoder_config(
  query: ast_query.Query,
  decoder: decode.Decoder(row),
  config: compiler.CompilerConfig,
  connection: pog.Connection,
) -> Result(pog.Returned(row), PostgresError) {
  case to_query_with(query, config) {
    Ok(compiled_query) -> {
      let query_with_decoder = pog.returning(compiled_query, decoder)

      case pog.execute(query_with_decoder, on: connection) {
        Ok(returned) -> Ok(returned)
        Error(error) -> Error(QueryError(error))
      }
    }
    Error(error) -> Error(CompileError(error))
  }
}

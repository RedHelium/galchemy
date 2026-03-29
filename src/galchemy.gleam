import galchemy/ast/query
import galchemy/sql/compiler
import galchemy/sql/postgres
import gleam/dynamic/decode
import pog

/// Public facade for compiling and executing queries.
///
/// Stable builder APIs live in the `galchemy/dsl/*` modules.
/// This facade intentionally exposes only top-level compile and execute
/// functions so that naming conflicts between builders stay out of the root
/// module.
pub fn compile(
  q: query.Query,
) -> Result(compiler.CompiledQuery, compiler.CompileError) {
  compiler.compile(q)
}

pub fn compile_to_query(
  q: query.Query,
) -> Result(pog.Query(Nil), compiler.CompileError) {
  postgres.compile_to_query(q)
}

pub fn execute(
  q: query.Query,
  connection: pog.Connection,
) -> Result(pog.Returned(Nil), postgres.PostgresError) {
  postgres.execute(q, connection)
}

pub fn execute_with_decoder(
  q: query.Query,
  decoder: decode.Decoder(row),
  connection: pog.Connection,
) -> Result(pog.Returned(row), postgres.PostgresError) {
  postgres.execute_with_decoder(q, decoder, connection)
}

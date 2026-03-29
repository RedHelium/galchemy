import galchemy/ast/query
import galchemy/sql/compiler

/// Public facade for compiling and executing queries.
///
/// Stable builder APIs live in the `galchemy/dsl/*` modules.
/// In `2.0` the root module exposes only generic compiler entry points.
/// PostgreSQL runtime integration lives in `galchemy/sql/postgres`.
pub fn default_compiler_config() -> compiler.CompilerConfig {
  compiler.default_config()
}

pub fn compile(
  q: query.Query,
) -> Result(compiler.CompiledQuery, compiler.CompileError) {
  compiler.compile(q)
}

pub fn compile_with(
  q: query.Query,
  config: compiler.CompilerConfig,
) -> Result(compiler.CompiledQuery, compiler.CompileError) {
  compiler.compile_with(q, config)
}

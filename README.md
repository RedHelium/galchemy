# galchemy

[![Package Version](https://img.shields.io/hexpm/v/galchemy)](https://hex.pm/packages/galchemy)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/galchemy/)

`galchemy` is a PostgreSQL-first SQL toolkit for Gleam:

- SQL DSL (`select`, `insert`, `update`, `delete`);
- SQL compiler (`AST -> SQL + params`);
- PostgreSQL execution helpers (`pog`);
- schema introspection, diff, DDL, and migration planning;
- explicit ORM/session building blocks.

The design goal is explicitness:

- you build immutable query values;
- you compile explicitly;
- you execute explicitly;
- no hidden query execution or Active Record-style magic.

## Installation

```sh
gleam add galchemy
```

## Quick Start

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import gleam/io
import gleam/string

pub fn main() -> Nil {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")
  let active = table.bool(users, "active")

  let q =
    select.select([
      expr.item(expr.col(id)),
      expr.item(expr.col(name)),
    ])
    |> select.from(users)
    |> select.where_(predicate.eq(expr.col(active), expr.bool(True)))
    |> query.Select

  io.println(string.inspect(galchemy.compile(q)))
}
```

## Core Workflow

1. Define table/column references with `galchemy/dsl/table`.
2. Build query AST values with `galchemy/dsl/*`.
3. Compile with `galchemy.compile` (or `compile_with`).
4. Optionally execute through `galchemy/sql/postgres`.

## Example Catalog

All runnable examples live in `src/galchemy/examples/*`.  
Root `examples/*` contains matching entry-point wrappers.

| Scenario | Module |
| --- | --- |
| Simple select + decoder | `galchemy/examples/simple_select` |
| CRUD builders | `galchemy/examples/crud` |
| Joins | `galchemy/examples/join_example` |
| `RETURNING` | `galchemy/examples/returning_example` |
| Advanced SQL (`CTE`, derived source/join, subquery, window, `UNION ALL`) | `galchemy/examples/advanced_select` |
| Schema diff + migration plan + code generation | `galchemy/examples/schema_tooling` |
| PostgreSQL schema introspection | `galchemy/examples/schema_introspection` |
| Eager/lazy loading planners | `galchemy/examples/loading` |
| Entity + unit of work + runtime session | `galchemy/examples/orm` |
| Declarative models + model-first query + result mapping | `galchemy/examples/orm_declarative` |

Run them with:

```sh
gleam run -m galchemy/examples/simple_select
gleam run -m galchemy/examples/crud
gleam run -m galchemy/examples/join_example
gleam run -m galchemy/examples/returning_example
gleam run -m galchemy/examples/advanced_select
gleam run -m galchemy/examples/schema_tooling
gleam run -m galchemy/examples/schema_introspection
gleam run -m galchemy/examples/loading
gleam run -m galchemy/examples/orm
gleam run -m galchemy/examples/orm_declarative
```

## SQL DSL Highlights

### CRUD

Use dedicated modules:

- `galchemy/dsl/select`
- `galchemy/dsl/insert`
- `galchemy/dsl/update`
- `galchemy/dsl/delete`

Each builder is immutable and returns a new query value.

### Predicates and Expressions

`galchemy/dsl/predicate` supports:

- comparisons: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`;
- boolean composition: `and`, `or`, `not`;
- `in_list`, `in_subquery`;
- `is_null`, `is_not_null`;
- `like`, `ilike`.

`galchemy/dsl/expr` supports:

- literals (`text`, `int`, `bool`, `float`, `timestamp`, `date`, `time_of_day`, `null`);
- scalar functions (`call`, `lower`, `upper`, `coalesce`);
- aggregates (`count`, `count_all`, `sum`, `avg`, `min`, `max`);
- arithmetic and concatenation;
- window expressions via `over`.

### Advanced Select Features

Supported select composition:

- `with_cte`;
- `from_derived`, `inner_join_derived`, `left_join_derived`;
- `group_by`, `having`;
- `union`, `union_all`;
- `distinct`, `order_by`, `limit`, `offset`.

See `galchemy/examples/advanced_select` for a combined real-world example.

## PostgreSQL Execution

`galchemy/sql/postgres` bridges compiled queries to `pog`.

```gleam
import galchemy/ast/query
import galchemy/sql/postgres
import gleam/dynamic/decode
import pog

pub fn run(connection: pog.Connection, q: query.Query) {
  let decoder = decode.at([0], decode.int)
  postgres.execute_with(q, decoder, connection)
}
```

## Schema Tooling

`galchemy` can help with the full schema pipeline:

1. introspect PostgreSQL to `SchemaSnapshot`;
2. infer relations;
3. diff snapshots;
4. compile DDL operations;
5. build and apply migration plans;
6. generate Gleam table modules from snapshots.

Main modules:

- `galchemy/schema/introspection/postgres`
- `galchemy/schema/relation`
- `galchemy/schema/diff`
- `galchemy/schema/ddl/postgres`
- `galchemy/schema/migration/postgres`
- `galchemy/schema/generator/gleam`

See:

- `galchemy/examples/schema_tooling`
- `galchemy/examples/schema_introspection`

## ORM and Session Building Blocks

`galchemy` ORM is explicit and composable. It provides primitives, not hidden magic:

- declarative model definition: `galchemy/orm/declarative`;
- metadata/mapper/runtime registries;
- identity map and materializer;
- explicit entity state transitions;
- unit-of-work flush planning;
- session runtime with `track`, `stage`, `flush`, `commit`, `rollback`;
- loading planners (joined/select-in style).

Recommended starting path:

1. `galchemy/examples/orm_declarative`
2. `galchemy/examples/orm`
3. `galchemy/examples/loading`

## Public API Map

- `galchemy`: top-level compile facade (`compile`, `compile_with`).
- `galchemy/dsl/*`: SQL builders.
- `galchemy/sql/compiler`: AST compiler.
- `galchemy/sql/postgres`: `pog` adapter.
- `galchemy/schema/*`: schema introspection/diff/migration/generation.
- `galchemy/orm/*`: declarative metadata, query/result mapping, materialization.
- `galchemy/session/*`: unit-of-work, runtime, transaction and loading helpers.

## Current Scope

Intentionally in scope:

- PostgreSQL-first behavior;
- explicit AST and compile boundary;
- explicit runtime/session mechanics.

Intentionally out of scope:

- multi-dialect SQL abstraction;
- implicit ORM behaviors.

## Development

```sh
gleam check
gleam test
```

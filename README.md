# galchemy

[![Package Version](https://img.shields.io/hexpm/v/galchemy)](https://hex.pm/packages/galchemy)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/galchemy/)

`galchemy` is a PostgreSQL-first SQL query builder, schema tooling, and explicit ORM foundation library for Gleam.

It is built around a small set of explicit ideas:

- immutable builders;
- an explicit SQL AST;
- predictable compilation into `SQL + params`;
- direct integration with `pog`;
- a clear boundary between query construction and query execution.

## Installation

```sh
gleam add galchemy
```

## What It Is

`galchemy` is a SQL-first query builder core with explicit ORM building blocks layered on top.

It helps you:

- describe tables and columns with typed schema references;
- build `select`, `insert`, `update`, and `delete` queries immutably;
- compile queries into SQL with positional parameters;
- execute compiled queries through `pog`.
- introspect PostgreSQL schemas into typed snapshots;
- infer relation metadata from foreign keys;
- diff schema snapshots into explicit change operations;
- compile and apply PostgreSQL migration plans.
- generate Gleam schema modules from PostgreSQL schema snapshots.
- plan session flushes into ordered `insert` / `update` / `delete` queries.
- plan eager joins and lazy follow-up queries from relation metadata.
- derive ORM metadata from schema snapshots;
- materialize entities, track entity state, and stage them into `unit_of_work`.

It does not try to manage:

- hidden SQL execution;
- implicit Active Record magic;
- dialect abstraction.

## Public API

The public API is intentionally namespaced by module:

- `galchemy/dsl/table`: tables, columns, schemas, aliases;
- `galchemy/dsl/expr`: SQL expressions and `SelectItem`;
- `galchemy/dsl/predicate`: predicates for `where` and `join on`;
- `galchemy/dsl/select`: `select` query builder;
- `galchemy/dsl/insert`: `insert` query builder;
- `galchemy/dsl/update`: `update` query builder;
- `galchemy/dsl/delete`: `delete` query builder;
- `galchemy/schema/model`: schema snapshot types for future diffing and generation tooling;
- `galchemy/schema/introspection/postgres`: PostgreSQL schema introspection into schema snapshots;
- `galchemy/schema/relation`: relation metadata and relation inference from schema snapshots;
- `galchemy/schema/diff`: schema snapshot comparison into explicit change operations;
- `galchemy/schema/ddl/postgres`: PostgreSQL DDL compilation for schema diff operations;
- `galchemy/schema/migration/postgres`: PostgreSQL migration planning, status tracking, and application helpers;
- `galchemy/schema/generator/gleam`: Gleam module generation from schema snapshots;
- `galchemy/orm/declarative`: declarative model definitions with explicit column builders and relation definitions that bridge into `SchemaSnapshot` and `ModelMetadata`;
- `galchemy/orm/metadata`: ORM model metadata derived from `SchemaSnapshot`;
- `galchemy/orm/hook`: explicit entity lifecycle hook definitions for load, relation hydration, attach, refresh, and staged persistence;
- `galchemy/orm/mapper_registry`: mapper registry for ORM model metadata;
- `galchemy/orm/identity_map`: identity map for materialized entities;
- `galchemy/orm/materializer`: row-to-entity materialization pipeline on top of mapper metadata and identity map;
- `galchemy/orm/graph`: relation graph hydration on top of entity metadata and identity map;
- `galchemy/orm/entity`: entity materialization, state tracking, and staging into `unit_of_work`;
- `galchemy/session/execution`: generic flush execution on top of `unit_of_work` planning;
- `galchemy/session/runtime`: explicit session state with `track`, `attach`, `stage`, `detach`, `refresh`, `flush`, `commit`, and `rollback`;
- `galchemy/session/cascade`: explicit cascade planning for staged relation graphs;
- `galchemy/session/transaction`: generic transaction-aware lifecycle over `runtime.Session`;
- `galchemy/session/postgres`: PostgreSQL transaction helpers on top of `pog.transaction`;
- `galchemy/session/unit_of_work`: session-style change tracking and ordered flush planning into query AST values;
- `galchemy/session/loading`: eager and lazy loading planners on top of relation metadata;
- `galchemy/sql/compiler`: AST to `SQL + params` compilation, including `compile_with` and compiler config hooks;
- `galchemy/sql/postgres`: PostgreSQL runtime adapter on top of `pog`;
- `galchemy`: top-level facade for generic compiler entry points only.

### Naming

The public naming scheme is intentionally fixed:

- SQL-like builder names stay SQL-like: `select`, `from`, `insert_into`, `update`, `delete_from`, `returning`;
- names that would conflict with Gleam or read awkwardly use a trailing underscore: `as_`, `where_`;
- join functions are explicit by join kind: `inner_join`, `left_join`;
- expression helpers stay short and direct: `col`, `item`, `text`, `int`, `float`, `bool`, `timestamp`, `date`, `time_of_day`, `null`;
- aggregate and function helpers build on top of expression composition: `count`, `count_all`, `sum`, `avg`, `min`, `max`, `lower`, `upper`, `coalesce`.

## Architecture

The runtime split is deliberate:

- `galchemy/sql/compiler` is the general compiler layer that turns AST values into `CompiledQuery`;
- `galchemy/sql/compiler.compile_with` and `CompilerConfig` are the explicit extension points for identifier rendering and function-name validation;
- `galchemy/sql/postgres` is the PostgreSQL adapter layer that turns `CompiledQuery` and `SqlValue` into `pog.Query`, `pog.Value`, and execution calls.

This keeps SQL compilation separate from PostgreSQL runtime concerns and avoids mixing PostgreSQL-specific execution helpers into the root module.

## Identifier Strategy

As of `1.5`, `galchemy` uses automatic identifier quoting:

- table names, schema names, column names, and aliases are always emitted as quoted identifiers;
- embedded double quotes are escaped by doubling them;
- SQL function names are not quoted and are validated separately from identifiers.

This gives the library a predictable PostgreSQL-safe rendering strategy for identifiers while keeping function calls readable.

## Quick Start

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table

pub fn build_query() {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")
  let active = table.bool(users, "active")

  select.select([
    expr.item(expr.col(id)),
    expr.as_(expr.col(name), "user_name"),
  ])
  |> select.from(users)
  |> select.where_(predicate.eq(expr.col(active), expr.bool(True)))
  |> query.Select
  |> galchemy.compile
}
```

## Real Scenarios

### CRUD

A full CRUD example is available in [`src/galchemy/examples/crud.gleam`](./src/galchemy/examples/crud.gleam).

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/delete
import galchemy/dsl/expr
import galchemy/dsl/insert
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import galchemy/dsl/update

pub fn build_crud_queries() {
  let users = table.table("users")
  let id = table.int(users, "id")
  let name = table.text(users, "name")
  let active = table.bool(users, "active")

  let select_query =
    select.select([expr.item(expr.col(id)), expr.item(expr.col(name))])
    |> select.from(users)
    |> select.where_(predicate.eq(expr.col(active), expr.bool(True)))

  let insert_query =
    insert.insert_into(users)
    |> insert.value(id, expr.int(1))
    |> insert.value(name, expr.text("Ann"))
    |> insert.returning([expr.item(expr.col(id))])

  let update_query =
    update.update(users)
    |> update.set(name, expr.text("Bob"))
    |> update.where_(predicate.eq(expr.col(id), expr.int(1)))

  let delete_query =
    delete.delete_from(users)
    |> delete.where_(predicate.eq(expr.col(id), expr.int(1)))

  #(
    galchemy.compile(query.Select(select_query)),
    galchemy.compile(query.Insert(insert_query)),
    galchemy.compile(query.Update(update_query)),
    galchemy.compile(query.Delete(delete_query)),
  )
}
```

### Joined Read Model

A dedicated join example is available in [`src/galchemy/examples/join_example.gleam`](./src/galchemy/examples/join_example.gleam).

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table

pub fn build_query() {
  let users = table.as_(table.table("users"), "u")
  let posts = table.as_(table.table("posts"), "p")
  let user_id = table.int(users, "id")
  let user_name = table.text(users, "name")
  let post_user_id = table.int(posts, "user_id")

  select.select([
    expr.item(expr.col(user_id)),
    expr.item(expr.col(user_name)),
    expr.as_(expr.col(post_user_id), "author_id"),
  ])
  |> select.from(users)
  |> select.inner_join(
    posts,
    predicate.eq(expr.col(user_id), expr.col(post_user_id)),
  )
  |> query.Select
  |> galchemy.compile
}
```

### `RETURNING`

A dedicated `returning` example is available in [`src/galchemy/examples/returning_example.gleam`](./src/galchemy/examples/returning_example.gleam).

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/insert
import galchemy/dsl/table

pub fn build_insert() {
  let users = table.table("users")
  let id = table.int(users, "id")
  let name = table.text(users, "name")

  insert.insert_into(users)
  |> insert.value(name, expr.text("Ann"))
  |> insert.returning([
    expr.item(expr.col(id)),
    expr.item(expr.col(name)),
  ])
  |> query.Insert
  |> galchemy.compile
}
```

### Reporting Query With `group_by` / `having`

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table

pub fn active_user_report() {
  let users = table.as_(table.table("users"), "u")
  let active = table.bool(users, "active")
  let id = table.int(users, "id")

  select.select([
    expr.item(expr.col(active)),
    expr.as_(expr.count(expr.col(id)), "user_count"),
  ])
  |> select.from(users)
  |> select.group_by(expr.col(active))
  |> select.having(predicate.gt(expr.count(expr.col(id)), expr.int(1)))
  |> query.Select
  |> galchemy.compile
}
```

### Batch Insert

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/insert
import galchemy/dsl/table

pub fn batch_insert() {
  let users = table.table("users")
  let id = table.int(users, "id")
  let name = table.text(users, "name")

  insert.insert_into(users)
  |> insert.values([
    [
      insert.field(id, expr.int(1)),
      insert.field(name, expr.text("Ann")),
    ],
    [
      insert.field(id, expr.int(2)),
      insert.field(name, expr.text("Bob")),
    ],
  ])
  |> query.Insert
  |> galchemy.compile
}
```

### Schema-Qualified Tables

```gleam
import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/select
import galchemy/dsl/table

pub fn analytics_query() {
  let users =
    table.table("users")
    |> table.in_schema("analytics")
    |> table.as_("u")
  let id = table.int(users, "id")

  select.select([expr.item(expr.col(id))])
  |> select.from(users)
  |> query.Select
  |> galchemy.compile
}
```

### Executing Through `pog`

```gleam
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import galchemy/sql/postgres
import gleam/dynamic/decode
import pog

pub fn run(connection: pog.Connection) {
  let users = table.as_(table.table("users"), "u")
  let id = table.int(users, "id")
  let name = table.text(users, "name")

  let decoder =
    decode.at([0], decode.int)
    |> decode.then(fn(id) {
      decode.at([1], decode.string)
      |> decode.map(fn(name) { #(id, name) })
    })

  select.select([
    expr.item(expr.col(id)),
    expr.item(expr.col(name)),
  ])
  |> select.from(users)
  |> select.where_(predicate.eq(expr.col(id), expr.int(1)))
  |> query.Select
  |> fn(q) { postgres.execute_with(q, decoder, connection) }
}
```

## Example Modules

The `src/galchemy/examples` directory now covers more than the core CRUD path:

- [`src/galchemy/examples/simple_select.gleam`](./src/galchemy/examples/simple_select.gleam): minimal `select` query plus `pog` decoder entry point without hardcoded connection bootstrapping.
- [`src/galchemy/examples/crud.gleam`](./src/galchemy/examples/crud.gleam): basic CRUD builder flow.
- [`src/galchemy/examples/join_example.gleam`](./src/galchemy/examples/join_example.gleam): join-oriented read model.
- [`src/galchemy/examples/returning_example.gleam`](./src/galchemy/examples/returning_example.gleam): `returning` for `insert` and `update`.
- [`src/galchemy/examples/schema_tooling.gleam`](./src/galchemy/examples/schema_tooling.gleam): schema diff, migration planning, and Gleam module generation from snapshots.
- [`src/galchemy/examples/loading.gleam`](./src/galchemy/examples/loading.gleam): eager join planning and lazy follow-up query planning.
- [`src/galchemy/examples/orm.gleam`](./src/galchemy/examples/orm.gleam): ORM metadata, entity state tracking, and staging into `unit_of_work`.

## Supported Value Literals

`SqlValue` currently supports:

- `Text(String)`
- `Int(Int)`
- `Float(Float)`
- `Bool(Bool)`
- `Timestamp(Timestamp)`
- `Date(Date)`
- `TimeOfDay(TimeOfDay)`
- `Null`

## Current Feature Set

The current stable surface includes:

- `select`, `insert`, `update`, `delete`;
- `inner_join`, `left_join`;
- `where`, `order_by`, `limit`, `offset`, `distinct`, `returning`;
- multi-row inserts;
- schema-qualified table names;
- expression helpers and aggregate helpers;
- extensible expression nodes for unary, binary, and window expressions;
- `group_by` and `having`;
- subqueries in `select`, `where`, and `in`;
- derived tables;
- CTEs;
- `union` / `union all`;
- window functions;
- PostgreSQL schema introspection into `SchemaSnapshot`;
- relation inference and relation metadata from `ForeignKey` definitions;
- schema diff into explicit operations;
- PostgreSQL DDL compilation for schema operations;
- PostgreSQL migration plans, migration history queries, and transactional apply helpers;
- Gleam schema module generation from `SchemaSnapshot`, including `relations()` helpers;
- declarative model definitions with explicit column builders and relation definitions that can be converted into `SchemaSnapshot` and ORM metadata;
- ORM model metadata derived from schema snapshots;
- mapper registry for model metadata reuse;
- identity map for materialized entities;
- row-to-entity materialization pipeline with identity-aware reuse;
- relation graph hydration for `belongs_to` and `has_many`;
- explicit entity lifecycle hooks through `materialize_with_hooks`, `hydrate_with_hooks`, `stage_with_hooks`, `attach_with_hooks`, and `refresh_with_hooks`;
- entity materialization, state tracking, and staging into `unit_of_work`;
- generic flush execution over ordered `unit_of_work` plans;
- explicit session runtime with `track`, `attach`, `stage`, `detach`, `refresh`, `flush`, `commit`, and `rollback`;
- explicit cascade rules for staged relation graphs;
- transaction-aware session lifecycle for generic executors and PostgreSQL connections;
- session-style `unit_of_work` flush planning with dependency-aware insert/delete ordering;
- eager join planning and lazy follow-up query planning from inferred relations;
- configurable compiler hooks through `CompilerConfig`;
- PostgreSQL execution through `pog`.

## Current Limits

The library still intentionally does not include:

- dialect abstraction;
- arbitrary dialect plugins.

## Development

```sh
gleam check
gleam test
gleam run -m galchemy/examples/simple_select
gleam run -m galchemy/examples/crud
gleam run -m galchemy/examples/join_example
gleam run -m galchemy/examples/returning_example
gleam run -m galchemy/examples/schema_tooling
gleam run -m galchemy/examples/loading
gleam run -m galchemy/examples/orm
```

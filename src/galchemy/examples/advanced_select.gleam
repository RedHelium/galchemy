import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import gleam/io
import gleam/string

pub fn build_activity_report() {
  let active_users = table.as_(table.table("active_users"), "au")
  let active_user_id = table.int(active_users, "id")
  let active_user_name = table.text(active_users, "name")
  let post_counts = table.as_(table.table("post_counts"), "pc")
  let post_counts_user_id = table.int(post_counts, "user_id")
  let post_counts_total = table.int(post_counts, "post_count")

  select.select([
    expr.item(expr.col(active_user_id)),
    expr.item(expr.col(active_user_name)),
    expr.as_(
      expr.coalesce([expr.col(post_counts_total), expr.int(0)]),
      "published_post_count",
    ),
    expr.as_(
      expr.over(expr.row_number(), [], [select.asc(expr.col(active_user_id))]),
      "row_number",
    ),
  ])
  |> select.with_cte("active_users", active_users_cte())
  |> select.from(active_users)
  |> select.left_join_derived(
    post_counts_query(),
    "pc",
    predicate.eq(expr.col(active_user_id), expr.col(post_counts_user_id)),
  )
  |> select.where_(predicate.in_subquery(
    expr.col(active_user_id),
    published_user_ids_subquery(),
  ))
}

pub fn build_union_feed() {
  let active_users = table.as_(table.table("users"), "u")
  let active_user_name = table.text(active_users, "name")
  let archived_users = table.as_(table.table("archived_users"), "a")
  let archived_user_name = table.text(archived_users, "name")

  let active_branch =
    select.select([
      expr.item(expr.col(active_user_name)),
      expr.item(expr.text("active")),
    ])
    |> select.from(active_users)

  let archived_branch =
    select.select([
      expr.item(expr.col(archived_user_name)),
      expr.item(expr.text("archived")),
    ])
    |> select.from(archived_users)

  select.union_all(active_branch, archived_branch)
}

pub fn build_derived_totals() {
  let totals = table.as_(table.table("totals"), "totals")
  let total_posts = table.int(totals, "post_count")

  select.select([
    expr.as_(expr.sum(expr.col(total_posts)), "total_published_posts"),
  ])
  |> select.from_derived(post_counts_query(), "totals")
}

pub fn main() -> Nil {
  io.println("Advanced select example")
  io.println("CTE + derived join + subquery + window:")
  io.println(
    string.inspect(galchemy.compile(query.Select(build_activity_report()))),
  )
  io.println("UNION ALL:")
  io.println(string.inspect(galchemy.compile(query.Select(build_union_feed()))))
  io.println("FROM derived:")
  io.println(
    string.inspect(galchemy.compile(query.Select(build_derived_totals()))),
  )
}

fn active_users_cte() {
  let users = table.as_(table.table("users"), "u")
  let user_id = table.int(users, "id")
  let user_name = table.text(users, "name")
  let active = table.bool(users, "active")

  select.select([
    expr.item(expr.col(user_id)),
    expr.item(expr.col(user_name)),
  ])
  |> select.from(users)
  |> select.where_(predicate.eq(expr.col(active), expr.bool(True)))
}

fn post_counts_query() {
  let posts = table.as_(table.table("posts"), "p")
  let user_id = table.int(posts, "user_id")
  let published = table.bool(posts, "published")

  select.select([
    expr.item(expr.col(user_id)),
    expr.as_(expr.count_all(), "post_count"),
  ])
  |> select.from(posts)
  |> select.where_(predicate.eq(expr.col(published), expr.bool(True)))
  |> select.group_by(expr.col(user_id))
}

fn published_user_ids_subquery() {
  let posts = table.as_(table.table("posts"), "p2")
  let user_id = table.int(posts, "user_id")
  let published = table.bool(posts, "published")

  select.select([expr.item(expr.col(user_id))])
  |> select.from(posts)
  |> select.where_(predicate.eq(expr.col(published), expr.bool(True)))
  |> select.distinct
}

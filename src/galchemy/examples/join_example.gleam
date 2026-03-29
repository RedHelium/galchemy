import galchemy
import galchemy/ast/query
import galchemy/dsl/expr
import galchemy/dsl/predicate
import galchemy/dsl/select
import galchemy/dsl/table
import gleam/io
import gleam/string

pub fn build_query() {
  let users = table.as_(table.table("users"), "u")
  let posts = table.as_(table.table("posts"), "p")
  let user_id = table.int(users, "id")
  let user_name = table.text(users, "name")
  let post_user_id = table.int(posts, "user_id")
  let post_published = table.bool(posts, "published")

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
  |> select.where_(predicate.eq(expr.col(post_published), expr.bool(True)))
}

pub fn main() -> Nil {
  io.println("Join example")
  io.println(string.inspect(galchemy.compile(query.Select(build_query()))))
}

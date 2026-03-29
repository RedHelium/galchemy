import galchemy/ast/query
import galchemy/session/unit_of_work
import gleam/list

pub type ExecutedQuery(result) {
  ExecutedQuery(query: query.Query, result: result)
}

pub type FlushExecution(result) {
  FlushExecution(
    inserts: List(ExecutedQuery(result)),
    updates: List(ExecutedQuery(result)),
    deletes: List(ExecutedQuery(result)),
  )
}

pub type ExecutionError(exec_error) {
  SessionError(unit_of_work.SessionError)
  QueryError(exec_error)
}

pub fn execute(
  session: unit_of_work.Session,
  executor: fn(query.Query) -> Result(result, exec_error),
) -> Result(
  #(FlushExecution(result), unit_of_work.Session),
  ExecutionError(exec_error),
) {
  use plan <- result_try(
    unit_of_work.flush_plan(session)
    |> map_error(SessionError),
  )
  use inserts <- result_try(execute_queries(plan.inserts, executor, []))
  use updates <- result_try(execute_queries(plan.updates, executor, []))
  use deletes <- result_try(execute_queries(plan.deletes, executor, []))

  Ok(#(
    FlushExecution(inserts: inserts, updates: updates, deletes: deletes),
    cleared_session(session),
  ))
}

pub fn queries(execution: FlushExecution(result)) -> List(ExecutedQuery(result)) {
  execution.inserts
  |> list.append(execution.updates)
  |> list.append(execution.deletes)
}

fn execute_queries(
  queries: List(query.Query),
  executor: fn(query.Query) -> Result(result, exec_error),
  acc: List(ExecutedQuery(result)),
) -> Result(List(ExecutedQuery(result)), ExecutionError(exec_error)) {
  case queries {
    [] -> Ok(list.reverse(acc))
    [next_query, ..rest] -> {
      use next_result <- result_try(
        executor(next_query)
        |> map_error(QueryError),
      )

      execute_queries(rest, executor, [
        ExecutedQuery(query: next_query, result: next_result),
        ..acc
      ])
    }
  }
}

fn cleared_session(session: unit_of_work.Session) -> unit_of_work.Session {
  let unit_of_work.Session(snapshot: snapshot, ..) = session
  unit_of_work.new(snapshot)
}

fn map_error(value: Result(a, e1), mapper: fn(e1) -> e2) -> Result(a, e2) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(mapper(error))
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

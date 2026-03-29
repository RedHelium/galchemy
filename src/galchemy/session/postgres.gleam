import galchemy/ast/query
import galchemy/session/execution
import galchemy/session/runtime
import galchemy/session/transaction
import galchemy/sql/postgres as sql_postgres
import gleam/list
import pog

pub type PostgresTransactionError(error) {
  TransactionError(pog.TransactionError(error))
}

pub fn with_transaction(
  connection: pog.Connection,
  session: runtime.Session,
  callback: fn(transaction.TransactionSession(pog.Connection)) -> Result(result, error),
) -> Result(result, PostgresTransactionError(error)) {
  case pog.transaction(connection, fn(tx_connection) {
    callback(transaction.begin(tx_connection, session))
  }) {
    Ok(result) -> Ok(result)
    Error(error) -> Error(TransactionError(error))
  }
}

pub fn begin(
  connection: pog.Connection,
  session: runtime.Session,
) -> transaction.TransactionSession(pog.Connection) {
  transaction.begin(connection, session)
}

pub fn flush(
  transaction_session: transaction.TransactionSession(pog.Connection),
) -> Result(
  #(pog.Returned(Nil), transaction.TransactionSession(pog.Connection)),
  transaction.TransactionError(sql_postgres.PostgresError),
) {
  case transaction.flush(transaction_session, execute_query) {
    Ok(#(flush_result, next_transaction)) ->
      Ok(#(summarize_flush(flush_result), next_transaction))
    Error(error) -> Error(error)
  }
}

pub fn commit(
  transaction_session: transaction.TransactionSession(pog.Connection),
) -> Result(
  #(pog.Returned(Nil), runtime.Session),
  transaction.TransactionError(sql_postgres.PostgresError),
) {
  case transaction.commit(transaction_session, execute_query) {
    Ok(#(flush_result, next_session)) ->
      Ok(#(summarize_flush(flush_result), next_session))
    Error(error) -> Error(error)
  }
}

fn execute_query(
  next_query: query.Query,
  connection: pog.Connection,
) -> Result(pog.Returned(Nil), sql_postgres.PostgresError) {
  sql_postgres.execute(next_query, connection)
}

fn summarize_flush(
  flush_result: execution.FlushExecution(pog.Returned(Nil)),
) -> pog.Returned(Nil) {
  let all_results =
    execution.queries(flush_result)
    |> list.fold(over: _, from: 0, with: fn(acc, executed) {
      acc + executed.result.count
    })

  pog.Returned(count: all_results, rows: [])
}

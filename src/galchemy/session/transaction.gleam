import galchemy/ast/query
import galchemy/orm/entity
import galchemy/session/execution
import galchemy/session/runtime

pub type TransactionSession(connection) {
  TransactionSession(connection: connection, session: runtime.Session)
}

pub type TransactionError(exec_error) {
  TrackError(runtime.TrackError)
  ExecutionError(runtime.SessionExecutionError(exec_error))
}

pub fn begin(
  connection: connection,
  session: runtime.Session,
) -> TransactionSession(connection) {
  TransactionSession(connection: connection, session: session)
}

pub fn connection(
  transaction: TransactionSession(connection),
) -> connection {
  transaction.connection
}

pub fn session(
  transaction: TransactionSession(connection),
) -> runtime.Session {
  transaction.session
}

pub fn track(
  transaction: TransactionSession(connection),
  next_entity: entity.Entity,
) -> Result(TransactionSession(connection), TransactionError(exec_error)) {
  case runtime.track(transaction.session, next_entity) {
    Ok(next_session) -> Ok(TransactionSession(..transaction, session: next_session))
    Error(error) -> Error(TrackError(error))
  }
}

pub fn stage(
  transaction: TransactionSession(connection),
  next_entity: entity.Entity,
) -> Result(TransactionSession(connection), TransactionError(exec_error)) {
  case runtime.stage(transaction.session, next_entity) {
    Ok(next_session) -> Ok(TransactionSession(..transaction, session: next_session))
    Error(error) -> Error(TrackError(error))
  }
}

pub fn flush(
  transaction: TransactionSession(connection),
  executor: fn(query.Query, connection) -> Result(result, exec_error),
) -> Result(
  #(execution.FlushExecution(result), TransactionSession(connection)),
  TransactionError(exec_error),
) {
  case runtime.flush(transaction.session, fn(next_query) {
    executor(next_query, transaction.connection)
  }) {
    Ok(#(flush_result, next_session)) ->
      Ok(#(
        flush_result,
        TransactionSession(..transaction, session: next_session),
      ))
    Error(error) -> Error(ExecutionError(error))
  }
}

pub fn commit(
  transaction: TransactionSession(connection),
  executor: fn(query.Query, connection) -> Result(result, exec_error),
) -> Result(
  #(execution.FlushExecution(result), runtime.Session),
  TransactionError(exec_error),
) {
  case runtime.commit(transaction.session, fn(next_query) {
    executor(next_query, transaction.connection)
  }) {
    Ok(result) -> Ok(result)
    Error(error) -> Error(ExecutionError(error))
  }
}

pub fn rollback(
  transaction: TransactionSession(connection),
) -> TransactionSession(connection) {
  TransactionSession(
    ..transaction,
    session: runtime.rollback(transaction.session),
  )
}

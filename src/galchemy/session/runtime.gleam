import galchemy/ast/query
import galchemy/orm/entity
import galchemy/orm/hook
import galchemy/orm/identity_map
import galchemy/schema/model
import galchemy/schema/relation
import galchemy/session/execution
import galchemy/session/unit_of_work
import gleam/option

pub type Session {
  Session(
    snapshot: model.SchemaSnapshot,
    pending: unit_of_work.Session,
    tracked: identity_map.IdentityMap,
    persisted: identity_map.IdentityMap,
  )
}

pub type TrackError {
  EntityError(entity.EntityError)
  IdentityMapError(identity_map.IdentityMapError)
  UnknownTrackedEntity(relation.TableRef, unit_of_work.Identity)
}

pub type HookTrackError(hook_error) {
  TrackFailure(TrackError)
  HookFailure(hook_error)
}

pub type SessionExecutionError(exec_error) {
  TrackError(TrackError)
  ExecutionError(execution.ExecutionError(exec_error))
}

pub fn new(snapshot: model.SchemaSnapshot) -> Session {
  Session(
    snapshot: snapshot,
    pending: unit_of_work.new(snapshot),
    tracked: identity_map.empty(),
    persisted: identity_map.empty(),
  )
}

pub fn track(
  session: Session,
  next_entity: entity.Entity,
) -> Result(Session, TrackError) {
  let clean_entity = entity.mark_clean(next_entity)

  use tracked <- result_try(
    identity_map.upsert(session.tracked, clean_entity)
    |> map_identity_error,
  )
  use persisted <- result_try(
    identity_map.upsert(session.persisted, clean_entity)
    |> map_identity_error,
  )

  Ok(Session(..session, tracked: tracked, persisted: persisted))
}

pub fn stage(
  session: Session,
  next_entity: entity.Entity,
) -> Result(Session, TrackError) {
  case stage_with_hooks(session, next_entity, hook.none()) {
    Ok(value) -> Ok(value)
    Error(TrackFailure(error)) -> Error(error)
    Error(HookFailure(_)) -> panic as "unreachable hook error for hook.none()"
  }
}

pub fn stage_with_hooks(
  session: Session,
  next_entity: entity.Entity,
  hooks: hook.EntityHooks(hook_error),
) -> Result(Session, HookTrackError(hook_error)) {
  use hooked_entity <- result_try(
    hook.before_stage(hooks, next_entity)
    |> map_hook_error,
  )
  use pending <- result_try(
    entity.stage(session.pending, hooked_entity)
    |> map_entity_error
    |> map_track_error,
  )
  use tracked <- result_try(
    identity_map.upsert(session.tracked, hooked_entity)
    |> map_identity_error
    |> map_track_error,
  )
  use persisted <- result_try(
    stage_persisted(session.persisted, hooked_entity)
    |> map_track_error,
  )

  Ok(
    Session(..session, pending: pending, tracked: tracked, persisted: persisted),
  )
}

pub fn attach(
  session: Session,
  next_entity: entity.Entity,
) -> Result(Session, TrackError) {
  case attach_with_hooks(session, next_entity, hook.none()) {
    Ok(value) -> Ok(value)
    Error(TrackFailure(error)) -> Error(error)
    Error(HookFailure(_)) -> panic as "unreachable hook error for hook.none()"
  }
}

pub fn attach_with_hooks(
  session: Session,
  next_entity: entity.Entity,
  hooks: hook.EntityHooks(hook_error),
) -> Result(Session, HookTrackError(hook_error)) {
  use hooked_entity <- result_try(
    hook.after_attach(hooks, next_entity)
    |> map_hook_error,
  )

  track(session, hooked_entity)
  |> map_track_error
}

pub fn detach(
  session: Session,
  next_entity: entity.Entity,
) -> Result(Session, TrackError) {
  use next_identity <- result_try(
    entity.identity(next_entity)
    |> map_entity_error,
  )
  let table = next_entity.metadata.table

  Ok(
    Session(
      ..session,
      pending: unit_of_work.discard_entity_changes(
        session.pending,
        table,
        next_identity,
        next_entity.fields,
      ),
      tracked: identity_map.remove(session.tracked, table, next_identity),
      persisted: identity_map.remove(session.persisted, table, next_identity),
    ),
  )
}

pub fn refresh(
  session: Session,
  next_entity: entity.Entity,
) -> Result(Session, TrackError) {
  case refresh_with_hooks(session, next_entity, hook.none()) {
    Ok(value) -> Ok(value)
    Error(TrackFailure(error)) -> Error(error)
    Error(HookFailure(_)) -> panic as "unreachable hook error for hook.none()"
  }
}

pub fn refresh_with_hooks(
  session: Session,
  next_entity: entity.Entity,
  hooks: hook.EntityHooks(hook_error),
) -> Result(Session, HookTrackError(hook_error)) {
  use next_identity <- result_try(
    entity.identity(next_entity)
    |> map_entity_error
    |> map_track_error,
  )
  let table = next_entity.metadata.table

  case identity_map.get(session.persisted, table, next_identity) {
    option.None ->
      Error(TrackFailure(UnknownTrackedEntity(table, next_identity)))
    option.Some(persisted_entity) -> {
      use refreshed_entity <- result_try(
        hook.after_refresh(hooks, persisted_entity)
        |> map_hook_error,
      )

      Ok(
        Session(
          ..session,
          pending: unit_of_work.discard_entity_changes(
            session.pending,
            table,
            next_identity,
            next_entity.fields,
          ),
          tracked: identity_map.upsert(
              identity_map.remove(session.tracked, table, next_identity),
              refreshed_entity,
            )
            |> unwrap_identity_map,
        ),
      )
    }
  }
}

pub fn flush(
  session: Session,
  executor: fn(query.Query) -> Result(result, exec_error),
) -> Result(
  #(execution.FlushExecution(result), Session),
  SessionExecutionError(exec_error),
) {
  case execution.execute(session.pending, executor) {
    Error(error) -> Error(ExecutionError(error))
    Ok(#(flush_result, cleared_pending)) -> {
      let normalized = normalize_tracked(session.tracked)

      Ok(#(
        flush_result,
        Session(
          ..session,
          pending: cleared_pending,
          tracked: normalized,
          persisted: normalized,
        ),
      ))
    }
  }
}

pub fn commit(
  session: Session,
  executor: fn(query.Query) -> Result(result, exec_error),
) -> Result(
  #(execution.FlushExecution(result), Session),
  SessionExecutionError(exec_error),
) {
  flush(session, executor)
}

pub fn rollback(session: Session) -> Session {
  Session(
    ..session,
    pending: unit_of_work.new(session.snapshot),
    tracked: session.persisted,
  )
}

pub fn tracked_entities(session: Session) -> List(entity.Entity) {
  identity_map.values(session.tracked)
}

pub fn get(
  session: Session,
  table: relation.TableRef,
  identity: unit_of_work.Identity,
) -> option.Option(entity.Entity) {
  identity_map.get(session.tracked, table, identity)
}

pub fn pending_changes(session: Session) -> unit_of_work.Session {
  session.pending
}

fn stage_persisted(
  persisted: identity_map.IdentityMap,
  next_entity: entity.Entity,
) -> Result(identity_map.IdentityMap, TrackError) {
  case entity.status(next_entity) {
    entity.New -> Ok(persisted)
    entity.Clean ->
      identity_map.upsert(persisted, entity.mark_clean(next_entity))
      |> map_identity_error
    entity.Dirty(_) | entity.Deleted ->
      case entity.identity(next_entity) {
        Error(error) -> Error(EntityError(error))
        Ok(next_identity) ->
          case
            identity_map.get(
              persisted,
              next_entity.metadata.table,
              next_identity,
            )
          {
            option.Some(_) -> Ok(persisted)
            option.None ->
              identity_map.upsert(persisted, entity.mark_clean(next_entity))
              |> map_identity_error
          }
      }
  }
}

fn map_track_error(
  value: Result(a, TrackError),
) -> Result(a, HookTrackError(hook_error)) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(TrackFailure(error))
  }
}

fn map_hook_error(
  value: Result(entity.Entity, hook_error),
) -> Result(entity.Entity, HookTrackError(hook_error)) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(HookFailure(error))
  }
}

fn normalize_tracked(
  tracked: identity_map.IdentityMap,
) -> identity_map.IdentityMap {
  normalize_entities(identity_map.values(tracked), identity_map.empty())
}

fn normalize_entities(
  entities: List(entity.Entity),
  acc: identity_map.IdentityMap,
) -> identity_map.IdentityMap {
  case entities {
    [] -> acc
    [next_entity, ..rest] -> {
      let next_map = case entity.status(next_entity) {
        entity.Deleted -> acc
        _ -> {
          let assert Ok(updated_map) =
            identity_map.upsert(acc, entity.mark_clean(next_entity))
          updated_map
        }
      }

      normalize_entities(rest, next_map)
    }
  }
}

fn map_entity_error(
  value: Result(a, entity.EntityError),
) -> Result(a, TrackError) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(EntityError(error))
  }
}

fn map_identity_error(
  value: Result(a, identity_map.IdentityMapError),
) -> Result(a, TrackError) {
  case value {
    Ok(inner) -> Ok(inner)
    Error(error) -> Error(IdentityMapError(error))
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

fn unwrap_identity_map(
  value: Result(identity_map.IdentityMap, identity_map.IdentityMapError),
) -> identity_map.IdentityMap {
  let assert Ok(map) = value
  map
}

import galchemy/ast/expression
import galchemy/orm/entity
import galchemy/orm/materializer
import galchemy/orm/metadata
import gleam/list
import gleam/option

pub type ScalarField {
  ScalarField(name: String, value: expression.SqlValue)
}

pub type Row {
  Row(
    scalars: List(ScalarField),
    entities: List(materializer.RowData),
  )
}

pub type MappingError {
  MissingScalar(name: String)
  MissingEntity(table: metadata.ModelMetadata)
  MaterializationError(materializer.MaterializationError)
}

pub type Mapper(a) {
  Mapper(
    run: fn(Row, materializer.Materializer) -> Result(#(a, materializer.Materializer), MappingError),
  )
}

pub fn row(
  scalars: List(ScalarField),
  entities: List(materializer.RowData),
) -> Row {
  Row(scalars: scalars, entities: entities)
}

pub fn scalar(name: String, value: expression.SqlValue) -> ScalarField {
  ScalarField(name: name, value: value)
}

pub fn scalar_value(name: String) -> Mapper(expression.SqlValue) {
  Mapper(run: fn(next_row, next_materializer) {
    case scalar_named(next_row.scalars, name) {
      option.Some(field) -> Ok(#(field.value, next_materializer))
      option.None -> Error(MissingScalar(name))
    }
  })
}

pub fn entity_value(
  next_metadata: metadata.ModelMetadata,
) -> Mapper(entity.Entity) {
  Mapper(run: fn(next_row, next_materializer) {
    case entity_row_named(next_row.entities, next_metadata) {
      option.Some(entity_row) ->
        case materializer.materialize(next_materializer, entity_row) {
          Ok(value) -> Ok(value)
          Error(error) -> Error(MaterializationError(error))
        }
      option.None -> Error(MissingEntity(next_metadata))
    }
  })
}

pub fn tuple2(first: Mapper(a), second: Mapper(b)) -> Mapper(#(a, b)) {
  Mapper(run: fn(next_row, next_materializer) {
    use #(first_value, materializer1) <- result_try(first.run(
      next_row,
      next_materializer,
    ))
    use #(second_value, materializer2) <- result_try(second.run(
      next_row,
      materializer1,
    ))

    Ok(#(#(first_value, second_value), materializer2))
  })
}

pub fn tuple3(
  first: Mapper(a),
  second: Mapper(b),
  third: Mapper(c),
) -> Mapper(#(a, b, c)) {
  Mapper(run: fn(next_row, next_materializer) {
    use #(first_value, materializer1) <- result_try(first.run(
      next_row,
      next_materializer,
    ))
    use #(second_value, materializer2) <- result_try(second.run(
      next_row,
      materializer1,
    ))
    use #(third_value, materializer3) <- result_try(third.run(
      next_row,
      materializer2,
    ))

    Ok(#(#(first_value, second_value, third_value), materializer3))
  })
}

pub fn map(mapper: Mapper(a), transform: fn(a) -> b) -> Mapper(b) {
  Mapper(run: fn(next_row, next_materializer) {
    use #(value, updated_materializer) <- result_try(mapper.run(
      next_row,
      next_materializer,
    ))
    Ok(#(transform(value), updated_materializer))
  })
}

pub fn one(
  mapper: Mapper(a),
  next_row: Row,
  next_materializer: materializer.Materializer,
) -> Result(#(a, materializer.Materializer), MappingError) {
  mapper.run(next_row, next_materializer)
}

pub fn many(
  mapper: Mapper(a),
  rows: List(Row),
  next_materializer: materializer.Materializer,
) -> Result(#(List(a), materializer.Materializer), MappingError) {
  many_loop(mapper, rows, next_materializer, [])
}

fn many_loop(
  mapper: Mapper(a),
  rows: List(Row),
  next_materializer: materializer.Materializer,
  acc: List(a),
) -> Result(#(List(a), materializer.Materializer), MappingError) {
  case rows {
    [] -> Ok(#(list.reverse(acc), next_materializer))
    [next_row, ..rest] -> {
      use #(mapped, updated_materializer) <- result_try(one(
        mapper,
        next_row,
        next_materializer,
      ))

      many_loop(mapper, rest, updated_materializer, [mapped, ..acc])
    }
  }
}

fn scalar_named(
  scalars: List(ScalarField),
  name: String,
) -> option.Option(ScalarField) {
  case scalars {
    [] -> option.None
    [next_scalar, ..rest] -> {
      case next_scalar.name == name {
        True -> option.Some(next_scalar)
        False -> scalar_named(rest, name)
      }
    }
  }
}

fn entity_row_named(
  rows: List(materializer.RowData),
  next_metadata: metadata.ModelMetadata,
) -> option.Option(materializer.RowData) {
  case rows {
    [] -> option.None
    [next_row, ..rest] -> {
      let materializer.RowData(table: table, fields: _) = next_row

      case table == next_metadata.table {
        True -> option.Some(next_row)
        False -> entity_row_named(rest, next_metadata)
      }
    }
  }
}

fn result_try(value: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case value {
    Ok(inner) -> next(inner)
    Error(error) -> Error(error)
  }
}

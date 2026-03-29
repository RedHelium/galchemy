import galchemy/schema/model
import gleam/list
import gleam/option.{type Option, None, Some}

pub type TableRef {
  TableRef(schema: String, name: String)
}

pub type SchemaOperation {
  CreateTable(model.TableSchema)
  DropTable(TableRef)
  AddColumn(table: TableRef, column: model.ColumnSchema)
  DropColumn(table: TableRef, column_name: String)
  AlterColumn(
    table: TableRef,
    column_name: String,
    current: model.ColumnSchema,
    target: model.ColumnSchema,
  )
  AddPrimaryKey(table: TableRef, primary_key: model.PrimaryKey)
  DropPrimaryKey(table: TableRef, primary_key_name: String)
  AddUniqueConstraint(table: TableRef, constraint: model.UniqueConstraint)
  DropUniqueConstraint(table: TableRef, constraint_name: String)
  AddForeignKey(table: TableRef, foreign_key: model.ForeignKey)
  DropForeignKey(table: TableRef, foreign_key_name: String)
  AddIndex(table: TableRef, index: model.IndexSchema)
  DropIndex(table: TableRef, index_name: String)
}

pub fn diff(
  current: model.SchemaSnapshot,
  target: model.SchemaSnapshot,
) -> List(SchemaOperation) {
  let model.SchemaSnapshot(tables: current_tables) = current
  let model.SchemaSnapshot(tables: target_tables) = target

  let drop_operations = diff_removed_tables(current_tables, target_tables, [])
  let change_operations =
    diff_existing_tables(current_tables, target_tables, [])
  let create_operations = diff_created_tables(current_tables, target_tables, [])

  list.append(
    list.append(drop_operations, change_operations),
    create_operations,
  )
}

fn diff_removed_tables(
  current_tables: List(model.TableSchema),
  target_tables: List(model.TableSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case current_tables {
    [] -> reverse(acc)
    [table, ..rest] -> {
      case find_table(target_tables, table.schema, table.name) {
        Some(_) -> diff_removed_tables(rest, target_tables, acc)
        None ->
          diff_removed_tables(rest, target_tables, [
            DropTable(table_ref(table.schema, table.name)),
            ..acc
          ])
      }
    }
  }
}

fn diff_created_tables(
  current_tables: List(model.TableSchema),
  target_tables: List(model.TableSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case target_tables {
    [] -> reverse(acc)
    [table, ..rest] -> {
      case find_table(current_tables, table.schema, table.name) {
        Some(_) -> diff_created_tables(current_tables, rest, acc)
        None ->
          diff_created_tables(current_tables, rest, [CreateTable(table), ..acc])
      }
    }
  }
}

fn diff_existing_tables(
  current_tables: List(model.TableSchema),
  target_tables: List(model.TableSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case current_tables {
    [] -> acc
    [current_table, ..rest] -> {
      case find_table(target_tables, current_table.schema, current_table.name) {
        None -> diff_existing_tables(rest, target_tables, acc)
        Some(target_table) -> {
          let table_operations = diff_table(current_table, target_table)
          diff_existing_tables(
            rest,
            target_tables,
            list.append(acc, table_operations),
          )
        }
      }
    }
  }
}

fn diff_table(
  current: model.TableSchema,
  target: model.TableSchema,
) -> List(SchemaOperation) {
  let ref = table_ref(current.schema, current.name)

  let drop_constraint_operations =
    list.append(
      list.append(
        diff_removed_foreign_keys(
          ref,
          current.foreign_keys,
          target.foreign_keys,
          [],
        ),
        diff_removed_unique_constraints(
          ref,
          current.unique_constraints,
          target.unique_constraints,
          [],
        ),
      ),
      list.append(
        diff_removed_indexes(ref, current.indexes, target.indexes, []),
        diff_primary_key(current.primary_key, target.primary_key, ref),
      ),
    )

  let column_operations =
    list.append(
      list.append(
        diff_removed_columns(ref, current.columns, target.columns, []),
        diff_changed_columns(ref, current.columns, target.columns, []),
      ),
      diff_added_columns(ref, current.columns, target.columns, []),
    )

  let add_constraint_operations =
    list.append(
      list.append(
        diff_primary_key_additions(current.primary_key, target.primary_key, ref),
        diff_added_unique_constraints(
          ref,
          current.unique_constraints,
          target.unique_constraints,
          [],
        ),
      ),
      list.append(
        diff_added_foreign_keys(
          ref,
          current.foreign_keys,
          target.foreign_keys,
          [],
        ),
        diff_added_indexes(ref, current.indexes, target.indexes, []),
      ),
    )

  list.append(
    list.append(drop_constraint_operations, column_operations),
    add_constraint_operations,
  )
}

fn diff_removed_columns(
  ref: TableRef,
  current_columns: List(model.ColumnSchema),
  target_columns: List(model.ColumnSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case current_columns {
    [] -> reverse(acc)
    [column, ..rest] -> {
      case find_column(target_columns, column.name) {
        Some(_) -> diff_removed_columns(ref, rest, target_columns, acc)
        None ->
          diff_removed_columns(ref, rest, target_columns, [
            DropColumn(table: ref, column_name: column.name),
            ..acc
          ])
      }
    }
  }
}

fn diff_changed_columns(
  ref: TableRef,
  current_columns: List(model.ColumnSchema),
  target_columns: List(model.ColumnSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case current_columns {
    [] -> reverse(acc)
    [column, ..rest] -> {
      case find_column(target_columns, column.name) {
        None -> diff_changed_columns(ref, rest, target_columns, acc)
        Some(target_column) -> {
          let next_acc = case column == target_column {
            True -> acc
            False -> [
              AlterColumn(
                table: ref,
                column_name: column.name,
                current: column,
                target: target_column,
              ),
              ..acc
            ]
          }

          diff_changed_columns(ref, rest, target_columns, next_acc)
        }
      }
    }
  }
}

fn diff_added_columns(
  ref: TableRef,
  current_columns: List(model.ColumnSchema),
  target_columns: List(model.ColumnSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case target_columns {
    [] -> reverse(acc)
    [column, ..rest] -> {
      case find_column(current_columns, column.name) {
        Some(_) -> diff_added_columns(ref, current_columns, rest, acc)
        None ->
          diff_added_columns(ref, current_columns, rest, [
            AddColumn(table: ref, column: column),
            ..acc
          ])
      }
    }
  }
}

fn diff_primary_key(
  current: Option(model.PrimaryKey),
  target: Option(model.PrimaryKey),
  ref: TableRef,
) -> List(SchemaOperation) {
  case current, target {
    None, None -> []
    Some(current_primary_key), None -> [
      DropPrimaryKey(table: ref, primary_key_name: current_primary_key.name),
    ]
    None, Some(_) -> []
    Some(current_primary_key), Some(target_primary_key) -> {
      case current_primary_key == target_primary_key {
        True -> []
        False -> [
          DropPrimaryKey(table: ref, primary_key_name: current_primary_key.name),
        ]
      }
    }
  }
}

fn diff_primary_key_additions(
  current: Option(model.PrimaryKey),
  target: Option(model.PrimaryKey),
  ref: TableRef,
) -> List(SchemaOperation) {
  case current, target {
    None, None -> []
    Some(_), None -> []
    None, Some(target_primary_key) -> [
      AddPrimaryKey(table: ref, primary_key: target_primary_key),
    ]
    Some(current_primary_key), Some(target_primary_key) -> {
      case current_primary_key == target_primary_key {
        True -> []
        False -> [AddPrimaryKey(table: ref, primary_key: target_primary_key)]
      }
    }
  }
}

fn diff_removed_unique_constraints(
  ref: TableRef,
  current_constraints: List(model.UniqueConstraint),
  target_constraints: List(model.UniqueConstraint),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case current_constraints {
    [] -> reverse(acc)
    [constraint, ..rest] -> {
      case find_unique_constraint(target_constraints, constraint.name) {
        Some(target_constraint) -> {
          let next_acc = case constraint == target_constraint {
            True -> acc
            False -> [
              DropUniqueConstraint(table: ref, constraint_name: constraint.name),
              ..acc
            ]
          }
          diff_removed_unique_constraints(
            ref,
            rest,
            target_constraints,
            next_acc,
          )
        }
        None ->
          diff_removed_unique_constraints(ref, rest, target_constraints, [
            DropUniqueConstraint(table: ref, constraint_name: constraint.name),
            ..acc
          ])
      }
    }
  }
}

fn diff_added_unique_constraints(
  ref: TableRef,
  current_constraints: List(model.UniqueConstraint),
  target_constraints: List(model.UniqueConstraint),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case target_constraints {
    [] -> reverse(acc)
    [constraint, ..rest] -> {
      case find_unique_constraint(current_constraints, constraint.name) {
        Some(current_constraint) -> {
          let next_acc = case current_constraint == constraint {
            True -> acc
            False -> [
              AddUniqueConstraint(table: ref, constraint: constraint),
              ..acc
            ]
          }
          diff_added_unique_constraints(
            ref,
            current_constraints,
            rest,
            next_acc,
          )
        }
        None ->
          diff_added_unique_constraints(ref, current_constraints, rest, [
            AddUniqueConstraint(table: ref, constraint: constraint),
            ..acc
          ])
      }
    }
  }
}

fn diff_removed_foreign_keys(
  ref: TableRef,
  current_foreign_keys: List(model.ForeignKey),
  target_foreign_keys: List(model.ForeignKey),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case current_foreign_keys {
    [] -> reverse(acc)
    [foreign_key, ..rest] -> {
      case find_foreign_key(target_foreign_keys, foreign_key.name) {
        Some(target_foreign_key) -> {
          let next_acc = case foreign_key == target_foreign_key {
            True -> acc
            False -> [
              DropForeignKey(table: ref, foreign_key_name: foreign_key.name),
              ..acc
            ]
          }
          diff_removed_foreign_keys(ref, rest, target_foreign_keys, next_acc)
        }
        None ->
          diff_removed_foreign_keys(ref, rest, target_foreign_keys, [
            DropForeignKey(table: ref, foreign_key_name: foreign_key.name),
            ..acc
          ])
      }
    }
  }
}

fn diff_added_foreign_keys(
  ref: TableRef,
  current_foreign_keys: List(model.ForeignKey),
  target_foreign_keys: List(model.ForeignKey),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case target_foreign_keys {
    [] -> reverse(acc)
    [foreign_key, ..rest] -> {
      case find_foreign_key(current_foreign_keys, foreign_key.name) {
        Some(current_foreign_key) -> {
          let next_acc = case current_foreign_key == foreign_key {
            True -> acc
            False -> [
              AddForeignKey(table: ref, foreign_key: foreign_key),
              ..acc
            ]
          }
          diff_added_foreign_keys(ref, current_foreign_keys, rest, next_acc)
        }
        None ->
          diff_added_foreign_keys(ref, current_foreign_keys, rest, [
            AddForeignKey(table: ref, foreign_key: foreign_key),
            ..acc
          ])
      }
    }
  }
}

fn diff_removed_indexes(
  ref: TableRef,
  current_indexes: List(model.IndexSchema),
  target_indexes: List(model.IndexSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case current_indexes {
    [] -> reverse(acc)
    [index, ..rest] -> {
      case find_index(target_indexes, index.name) {
        Some(target_index) -> {
          let next_acc = case index == target_index {
            True -> acc
            False -> [DropIndex(table: ref, index_name: index.name), ..acc]
          }
          diff_removed_indexes(ref, rest, target_indexes, next_acc)
        }
        None ->
          diff_removed_indexes(ref, rest, target_indexes, [
            DropIndex(table: ref, index_name: index.name),
            ..acc
          ])
      }
    }
  }
}

fn diff_added_indexes(
  ref: TableRef,
  current_indexes: List(model.IndexSchema),
  target_indexes: List(model.IndexSchema),
  acc: List(SchemaOperation),
) -> List(SchemaOperation) {
  case target_indexes {
    [] -> reverse(acc)
    [index, ..rest] -> {
      case find_index(current_indexes, index.name) {
        Some(current_index) -> {
          let next_acc = case current_index == index {
            True -> acc
            False -> [AddIndex(table: ref, index: index), ..acc]
          }
          diff_added_indexes(ref, current_indexes, rest, next_acc)
        }
        None ->
          diff_added_indexes(ref, current_indexes, rest, [
            AddIndex(table: ref, index: index),
            ..acc
          ])
      }
    }
  }
}

fn find_table(
  tables: List(model.TableSchema),
  schema_name: String,
  table_name: String,
) -> Option(model.TableSchema) {
  case tables {
    [] -> None
    [table, ..rest] -> {
      case table.schema == schema_name && table.name == table_name {
        True -> Some(table)
        False -> find_table(rest, schema_name, table_name)
      }
    }
  }
}

fn find_column(
  columns: List(model.ColumnSchema),
  column_name: String,
) -> Option(model.ColumnSchema) {
  case columns {
    [] -> None
    [column, ..rest] -> {
      case column.name == column_name {
        True -> Some(column)
        False -> find_column(rest, column_name)
      }
    }
  }
}

fn find_unique_constraint(
  constraints: List(model.UniqueConstraint),
  constraint_name: String,
) -> Option(model.UniqueConstraint) {
  case constraints {
    [] -> None
    [constraint, ..rest] -> {
      case constraint.name == constraint_name {
        True -> Some(constraint)
        False -> find_unique_constraint(rest, constraint_name)
      }
    }
  }
}

fn find_foreign_key(
  foreign_keys: List(model.ForeignKey),
  foreign_key_name: String,
) -> Option(model.ForeignKey) {
  case foreign_keys {
    [] -> None
    [foreign_key, ..rest] -> {
      case foreign_key.name == foreign_key_name {
        True -> Some(foreign_key)
        False -> find_foreign_key(rest, foreign_key_name)
      }
    }
  }
}

fn find_index(
  indexes: List(model.IndexSchema),
  index_name: String,
) -> Option(model.IndexSchema) {
  case indexes {
    [] -> None
    [index, ..rest] -> {
      case index.name == index_name {
        True -> Some(index)
        False -> find_index(rest, index_name)
      }
    }
  }
}

fn table_ref(schema_name: String, table_name: String) -> TableRef {
  TableRef(schema: schema_name, name: table_name)
}

fn reverse(items: List(a)) -> List(a) {
  reverse_loop(items, [])
}

fn reverse_loop(items: List(a), acc: List(a)) -> List(a) {
  case items {
    [] -> acc
    [item, ..rest] -> reverse_loop(rest, [item, ..acc])
  }
}

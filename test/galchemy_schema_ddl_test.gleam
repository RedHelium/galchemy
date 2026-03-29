import galchemy/schema/ddl/postgres as ddl
import galchemy/schema/diff
import galchemy/schema/model
import gleam/option
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn compile_create_table_test() {
  let table =
    model.TableSchema(
      schema: "public",
      name: "users",
      columns: [
        column("id", model.IntegerType, False, option.None, 1),
        column(
          "email",
          model.VarCharType(length: option.Some(255)),
          False,
          option.None,
          2,
        ),
        column("company_id", model.IntegerType, False, option.None, 3),
      ],
      primary_key: option.Some(
        model.PrimaryKey(name: "users_pkey", columns: ["id"]),
      ),
      unique_constraints: [
        model.UniqueConstraint(name: "users_email_key", columns: ["email"]),
      ],
      foreign_keys: [
        model.ForeignKey(
          name: "users_company_id_fkey",
          columns: ["company_id"],
          referenced_schema: "public",
          referenced_table: "companies",
          referenced_columns: ["id"],
        ),
      ],
      indexes: [
        model.IndexSchema(
          name: "users_email_idx",
          unique: False,
          definition: "CREATE INDEX \"users_email_idx\" ON \"public\".\"users\" USING btree (\"email\")",
        ),
      ],
    )

  assert ddl.compile_operation(diff.CreateTable(table))
    == Ok([
      "CREATE TABLE \"public\".\"users\" (\"id\" INTEGER NOT NULL, \"email\" VARCHAR(255) NOT NULL, \"company_id\" INTEGER NOT NULL, CONSTRAINT \"users_pkey\" PRIMARY KEY (\"id\"), CONSTRAINT \"users_email_key\" UNIQUE (\"email\"), CONSTRAINT \"users_company_id_fkey\" FOREIGN KEY (\"company_id\") REFERENCES \"public\".\"companies\" (\"id\"))",
      "CREATE INDEX \"users_email_idx\" ON \"public\".\"users\" USING btree (\"email\")",
    ])
}

pub fn compile_alter_column_test() {
  let operation =
    diff.AlterColumn(
      table: diff.TableRef(schema: "public", name: "users"),
      column_name: "email",
      current: column("email", model.TextType, True, option.None, 2),
      target: column(
        "email",
        model.VarCharType(length: option.Some(255)),
        False,
        option.Some("'unknown'::text"),
        2,
      ),
    )

  assert ddl.compile_operation(operation)
    == Ok([
      "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"email\" TYPE VARCHAR(255)",
      "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"email\" SET DEFAULT 'unknown'::text",
      "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"email\" SET NOT NULL",
    ])
}

pub fn compile_diff_plan_test() {
  let operations = [
    diff.DropIndex(
      table: diff.TableRef(schema: "public", name: "users"),
      index_name: "users_email_idx",
    ),
    diff.DropColumn(
      table: diff.TableRef(schema: "public", name: "users"),
      column_name: "age",
    ),
    diff.AddColumn(
      table: diff.TableRef(schema: "public", name: "users"),
      column: column("email", model.TextType, False, option.None, 3),
    ),
    diff.AddIndex(
      table: diff.TableRef(schema: "public", name: "users"),
      index: model.IndexSchema(
        name: "users_company_idx",
        unique: False,
        definition: "CREATE INDEX \"users_company_idx\" ON \"public\".\"users\" USING btree (\"company_id\")",
      ),
    ),
  ]

  assert ddl.compile(operations)
    == Ok([
      "DROP INDEX \"public\".\"users_email_idx\"",
      "ALTER TABLE \"public\".\"users\" DROP COLUMN \"age\"",
      "ALTER TABLE \"public\".\"users\" ADD COLUMN \"email\" TEXT NOT NULL",
      "CREATE INDEX \"users_company_idx\" ON \"public\".\"users\" USING btree (\"company_id\")",
    ])
}

fn column(
  name: String,
  data_type: model.ColumnType,
  nullable: Bool,
  default: option.Option(String),
  ordinal_position: Int,
) -> model.ColumnSchema {
  model.ColumnSchema(name:, data_type:, nullable:, default:, ordinal_position:)
}

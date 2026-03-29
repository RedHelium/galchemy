import galchemy/schema/diff
import galchemy/schema/model
import gleam/option
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn schema_diff_create_and_drop_table_test() {
  let current =
    model.SchemaSnapshot(tables: [
      table_schema("public", "users", [
        column("id", model.IntegerType, False, option.None, 1),
      ]),
    ])

  let target =
    model.SchemaSnapshot(tables: [
      table_schema("public", "accounts", [
        column("id", model.IntegerType, False, option.None, 1),
      ]),
    ])

  assert diff.diff(current, target)
    == [
      diff.DropTable(diff.TableRef(schema: "public", name: "users")),
      diff.CreateTable(
        table_schema("public", "accounts", [
          column("id", model.IntegerType, False, option.None, 1),
        ]),
      ),
    ]
}

pub fn schema_diff_columns_test() {
  let current =
    model.SchemaSnapshot(tables: [
      table_schema("public", "users", [
        column("id", model.IntegerType, False, option.None, 1),
        column("name", model.TextType, False, option.None, 2),
        column("age", model.IntegerType, True, option.None, 3),
      ]),
    ])

  let target =
    model.SchemaSnapshot(tables: [
      table_schema("public", "users", [
        column("id", model.IntegerType, False, option.None, 1),
        column(
          "name",
          model.VarCharType(length: option.Some(255)),
          False,
          option.None,
          2,
        ),
        column("email", model.TextType, False, option.None, 3),
      ]),
    ])

  assert diff.diff(current, target)
    == [
      diff.DropColumn(
        table: diff.TableRef(schema: "public", name: "users"),
        column_name: "age",
      ),
      diff.AlterColumn(
        table: diff.TableRef(schema: "public", name: "users"),
        column_name: "name",
        current: column("name", model.TextType, False, option.None, 2),
        target: column(
          "name",
          model.VarCharType(length: option.Some(255)),
          False,
          option.None,
          2,
        ),
      ),
      diff.AddColumn(
        table: diff.TableRef(schema: "public", name: "users"),
        column: column("email", model.TextType, False, option.None, 3),
      ),
    ]
}

pub fn schema_diff_constraints_and_indexes_test() {
  let current =
    model.SchemaSnapshot(tables: [
      model.TableSchema(
        schema: "public",
        name: "users",
        columns: [
          column("id", model.IntegerType, False, option.None, 1),
          column("email", model.TextType, False, option.None, 2),
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
            definition: "CREATE INDEX users_email_idx ON public.users USING btree (email)",
          ),
        ],
      ),
    ])

  let target =
    model.SchemaSnapshot(tables: [
      model.TableSchema(
        schema: "public",
        name: "users",
        columns: [
          column("id", model.IntegerType, False, option.None, 1),
          column("email", model.TextType, False, option.None, 2),
          column("company_id", model.IntegerType, False, option.None, 3),
        ],
        primary_key: option.Some(
          model.PrimaryKey(name: "users_pk", columns: ["id"]),
        ),
        unique_constraints: [
          model.UniqueConstraint(name: "users_email_company_key", columns: [
            "email",
            "company_id",
          ]),
        ],
        foreign_keys: [
          model.ForeignKey(
            name: "users_company_id_fkey",
            columns: ["company_id"],
            referenced_schema: "public",
            referenced_table: "organizations",
            referenced_columns: ["id"],
          ),
        ],
        indexes: [
          model.IndexSchema(
            name: "users_company_idx",
            unique: False,
            definition: "CREATE INDEX users_company_idx ON public.users USING btree (company_id)",
          ),
        ],
      ),
    ])

  assert diff.diff(current, target)
    == [
      diff.DropForeignKey(
        table: diff.TableRef(schema: "public", name: "users"),
        foreign_key_name: "users_company_id_fkey",
      ),
      diff.DropUniqueConstraint(
        table: diff.TableRef(schema: "public", name: "users"),
        constraint_name: "users_email_key",
      ),
      diff.DropIndex(
        table: diff.TableRef(schema: "public", name: "users"),
        index_name: "users_email_idx",
      ),
      diff.DropPrimaryKey(
        table: diff.TableRef(schema: "public", name: "users"),
        primary_key_name: "users_pkey",
      ),
      diff.AddPrimaryKey(
        table: diff.TableRef(schema: "public", name: "users"),
        primary_key: model.PrimaryKey(name: "users_pk", columns: ["id"]),
      ),
      diff.AddUniqueConstraint(
        table: diff.TableRef(schema: "public", name: "users"),
        constraint: model.UniqueConstraint(
          name: "users_email_company_key",
          columns: ["email", "company_id"],
        ),
      ),
      diff.AddForeignKey(
        table: diff.TableRef(schema: "public", name: "users"),
        foreign_key: model.ForeignKey(
          name: "users_company_id_fkey",
          columns: ["company_id"],
          referenced_schema: "public",
          referenced_table: "organizations",
          referenced_columns: ["id"],
        ),
      ),
      diff.AddIndex(
        table: diff.TableRef(schema: "public", name: "users"),
        index: model.IndexSchema(
          name: "users_company_idx",
          unique: False,
          definition: "CREATE INDEX users_company_idx ON public.users USING btree (company_id)",
        ),
      ),
    ]
}

fn table_schema(
  schema_name: String,
  table_name: String,
  columns: List(model.ColumnSchema),
) -> model.TableSchema {
  model.TableSchema(
    schema: schema_name,
    name: table_name,
    columns: columns,
    primary_key: option.None,
    unique_constraints: [],
    foreign_keys: [],
    indexes: [],
  )
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

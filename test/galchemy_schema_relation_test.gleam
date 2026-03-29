import galchemy/schema/model
import galchemy/schema/relation
import gleam/option
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn infer_relations_test() {
  let snapshot =
    model.SchemaSnapshot(tables: [
      model.TableSchema(
        schema: "public",
        name: "users",
        columns: [column("id", model.IntegerType, False, option.None, 1)],
        primary_key: option.Some(
          model.PrimaryKey(name: "users_pkey", columns: ["id"]),
        ),
        unique_constraints: [],
        foreign_keys: [],
        indexes: [],
      ),
      model.TableSchema(
        schema: "public",
        name: "posts",
        columns: [
          column("id", model.IntegerType, False, option.None, 1),
          column("user_id", model.IntegerType, False, option.None, 2),
        ],
        primary_key: option.Some(
          model.PrimaryKey(name: "posts_pkey", columns: ["id"]),
        ),
        unique_constraints: [],
        foreign_keys: [
          model.ForeignKey(
            name: "posts_user_id_fkey",
            columns: ["user_id"],
            referenced_schema: "public",
            referenced_table: "users",
            referenced_columns: ["id"],
          ),
        ],
        indexes: [],
      ),
      model.TableSchema(
        schema: "public",
        name: "company_identities",
        columns: [
          column("user_id", model.IntegerType, False, option.None, 1),
          column("company_id", model.IntegerType, False, option.None, 2),
        ],
        primary_key: option.None,
        unique_constraints: [],
        foreign_keys: [
          model.ForeignKey(
            name: "company_identities_user_id_fkey",
            columns: ["user_id"],
            referenced_schema: "public",
            referenced_table: "users",
            referenced_columns: ["id"],
          ),
        ],
        indexes: [],
      ),
      model.TableSchema(
        schema: "public",
        name: "memberships",
        columns: [
          column("user_id", model.IntegerType, False, option.None, 1),
          column("company_id", model.IntegerType, False, option.None, 2),
        ],
        primary_key: option.None,
        unique_constraints: [],
        foreign_keys: [
          model.ForeignKey(
            name: "memberships_user_company_fkey",
            columns: ["user_id", "company_id"],
            referenced_schema: "public",
            referenced_table: "company_identities",
            referenced_columns: ["user_id", "company_id"],
          ),
        ],
        indexes: [],
      ),
    ])

  assert relation.infer(snapshot)
    == [
      relation.TableRelations(
        table: relation.table_ref("public", "users"),
        relations: [
          relation.has_many(
            "posts",
            "posts_user_id_fkey",
            relation.table_ref("public", "posts"),
            [relation.pair("id", "user_id")],
          ),
          relation.has_many(
            "company_identities",
            "company_identities_user_id_fkey",
            relation.table_ref("public", "company_identities"),
            [relation.pair("id", "user_id")],
          ),
        ],
      ),
      relation.TableRelations(
        table: relation.table_ref("public", "posts"),
        relations: [
          relation.belongs_to(
            "user",
            "posts_user_id_fkey",
            relation.table_ref("public", "users"),
            [relation.pair("user_id", "id")],
          ),
        ],
      ),
      relation.TableRelations(
        table: relation.table_ref("public", "company_identities"),
        relations: [
          relation.belongs_to(
            "user",
            "company_identities_user_id_fkey",
            relation.table_ref("public", "users"),
            [relation.pair("user_id", "id")],
          ),
          relation.has_many(
            "memberships",
            "memberships_user_company_fkey",
            relation.table_ref("public", "memberships"),
            [
              relation.pair("user_id", "user_id"),
              relation.pair("company_id", "company_id"),
            ],
          ),
        ],
      ),
      relation.TableRelations(
        table: relation.table_ref("public", "memberships"),
        relations: [
          relation.belongs_to(
            "company_identity",
            "memberships_user_company_fkey",
            relation.table_ref("public", "company_identities"),
            [
              relation.pair("user_id", "user_id"),
              relation.pair("company_id", "company_id"),
            ],
          ),
        ],
      ),
    ]
}

pub fn relation_name_deduplication_test() {
  let snapshot =
    model.SchemaSnapshot(tables: [
      model.TableSchema(
        schema: "public",
        name: "users",
        columns: [column("id", model.IntegerType, False, option.None, 1)],
        primary_key: option.None,
        unique_constraints: [],
        foreign_keys: [],
        indexes: [],
      ),
      model.TableSchema(
        schema: "public",
        name: "audit_entries",
        columns: [
          column("id", model.IntegerType, False, option.None, 1),
          column("user_id", model.IntegerType, False, option.None, 2),
          column("user-id", model.IntegerType, False, option.None, 3),
        ],
        primary_key: option.None,
        unique_constraints: [],
        foreign_keys: [
          model.ForeignKey(
            name: "audit_entries_user_id_fkey",
            columns: ["user_id"],
            referenced_schema: "public",
            referenced_table: "users",
            referenced_columns: ["id"],
          ),
          model.ForeignKey(
            name: "audit_entries_user_dash_id_fkey",
            columns: ["user-id"],
            referenced_schema: "public",
            referenced_table: "users",
            referenced_columns: ["id"],
          ),
        ],
        indexes: [],
      ),
    ])

  assert relation.for_table(snapshot, "public", "audit_entries")
    == option.Some(
      relation.TableRelations(
        table: relation.table_ref("public", "audit_entries"),
        relations: [
          relation.belongs_to(
            "user",
            "audit_entries_user_id_fkey",
            relation.table_ref("public", "users"),
            [relation.pair("user_id", "id")],
          ),
          relation.belongs_to(
            "user_2",
            "audit_entries_user_dash_id_fkey",
            relation.table_ref("public", "users"),
            [relation.pair("user-id", "id")],
          ),
        ],
      ),
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

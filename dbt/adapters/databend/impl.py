from concurrent.futures import Future
from dataclasses import dataclass
from typing import Callable, List, Optional, Set, Union, FrozenSet, Tuple

import agate
from dbt.adapters.base import AdapterConfig, available
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.sql import SQLAdapter

from dbt_common.clients.agate_helper import table_from_rows
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.contracts.relation import RelationType
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import CompilationError, DbtDatabaseError, DbtRuntimeError, DbtInternalError
from dbt_common.utils import filter_null_values
from dbt_common.utils import executor

import csv
import io
from dbt.adapters.databend.column import DatabendColumn
from dbt.adapters.databend.connections import DatabendConnectionManager
from dbt.adapters.databend.relation import DatabendRelation

GET_CATALOG_MACRO_NAME = "get_catalog"
LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"

logger = AdapterLogger("databend")


def _expect_row_value(key: str, row: agate.Row):
    if key not in row.keys():
        raise DbtInternalError(
            f"Got a row without '{key}' column, columns: {row.keys()}"
        )

    return row[key]


@dataclass
class DatabendConfig(AdapterConfig):
    cluster_by: Optional[Union[List[str], str]] = None


class DatabendAdapter(SQLAdapter):
    Relation = DatabendRelation
    Column = DatabendColumn
    ConnectionManager = DatabendConnectionManager
    AdapterSpecificConfigs = DatabendConfig

    @classmethod
    def date_function(cls):
        return "NOW()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "int"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "bool"

    @available
    def get_csv_data(self, table):
        csv_funcs = [c.csvify for c in table._column_types]

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")

        for row in table.rows:
            writer.writerow(tuple(csv_funcs[i](d) for i, d in enumerate(row)))

        return buf.getvalue()

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise DbtRuntimeError(
            "`convert_time_type` is not implemented for this adapter!"
        )

    def quote(self, identifier):
        return "{}".format(identifier)

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def list_relations_without_caching(
            self, schema_relation: DatabendRelation
    ) -> List[DatabendRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        for row in results:
            if len(row) != 4:
                raise DbtRuntimeError(
                    f"Invalid value from 'show table extended ...', "
                    f"got {len(row)} values, expected 4"
                )
            _database, name, schema, type_info = row
            rel_type = RelationType.View if "view" in type_info else RelationType.Table
            relation = self.Relation.create(database=None, schema=schema, identifier=name, rt=rel_type)
            relations.append(relation)

        return relations

    @classmethod
    def _catalog_filter_table(
            cls, table: agate.Table, used_schemas: FrozenSet[Tuple[str, str]]
    ) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_schema", "table_name"],
        )
        return super()._catalog_filter_table(table, used_schemas)

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        # if not self.Relation.include_policy.database:
        #     database = None

        return super().get_relation(database, schema, identifier)

    def parse_show_columns(
            self, _relation: DatabendRelation, raw_rows: List[agate.Row]
    ) -> List[DatabendColumn]:
        rows = [
            dict(zip(row._keys, row._values))  # pylint: disable=protected-access
            for row in raw_rows
        ]

        return [
            DatabendColumn(
                column=column["name"],
                dtype=column["type"],
            )
            for column in rows
        ]

    def get_columns_in_relation(
            self, relation: DatabendRelation
    ) -> List[DatabendColumn]:
        rows: List[agate.Row] = super().get_columns_in_relation(relation)

        return self.parse_show_columns(relation, rows)

    def _get_one_catalog(
            self,
            information_schema: InformationSchema,
            schemas: Set[str],
            used_schemas: FrozenSet[Tuple[str, str]],
    ) -> agate.Table:
        if len(schemas) != 1:
            DbtRuntimeError(
                f"Expected only one schema in databend _get_one_catalog, found {schemas}"
            )

        return super()._get_one_catalog(information_schema, schemas, used_schemas)

    def update_column_sql(
            self,
            dst_name: str,
            dst_column: str,
            clause: str,
            where_clause: Optional[str] = None,
    ) -> str:
        raise DbtInternalError(
            "`update_column_sql` is not implemented for this adapter!"
        )

    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == "one":
                if hasattr(cursor, "fetchone"):
                    return cursor.fetchone()
                else:
                    return cursor.fetchall()[0]
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as exc:
            logger.error(sql)
            logger.error(exc)
            raise exc
        finally:
            conn.transaction_open = False

    def get_rows_different_sql(
            self,
            relation_a: DatabendRelation,
            relation_b: DatabendRelation,
            column_names: Optional[List[str]] = None,
            except_operator: str = "EXCEPT",
    ) -> str:
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        alias_a = "A"
        alias_b = "B"
        columns_csv_a = ", ".join([f"{alias_a}.{name}" for name in names])
        columns_csv_b = ", ".join([f"{alias_b}.{name}" for name in names])
        join_condition = " AND ".join(
            [f"{alias_a}.{name} = {alias_b}.{name}" for name in names]
        )
        if len(names) == 0:
            return f"select 0, 0"
        first_column = names[0]

        # MySQL doesn't have an EXCEPT or MINUS operator, so we need to simulate it
        COLUMNS_EQUAL_SQL = """
        WITH
        a_except_b as (
            SELECT
                {columns_a}
            FROM {relation_a} as {alias_a}
            LEFT OUTER JOIN {relation_b} as {alias_b}
                ON {join_condition}
            WHERE {alias_b}.{first_column} is null
        ),
        b_except_a as (
            SELECT
                {columns_b}
            FROM {relation_b} as {alias_b}
            LEFT OUTER JOIN {relation_a} as {alias_a}
                ON {join_condition}
            WHERE {alias_a}.{first_column} is null
        ),
        diff_count as (
            SELECT
                1 as id,
                COUNT(*) as num_missing FROM (
                    SELECT * FROM a_except_b
                    UNION ALL
                    SELECT * FROM b_except_a
                ) as missing
        ),
        table_a as (
            SELECT COUNT(*) as num_rows FROM {relation_a}
        ),
        table_b as (
            SELECT COUNT(*) as num_rows FROM {relation_b}
        ),
        row_count_diff as (
            SELECT
                1 as id,
                table_a.num_rows - table_b.num_rows as difference
            FROM table_a, table_b
        )
        SELECT
            row_count_diff.difference as row_count_difference,
            diff_count.num_missing as num_mismatched
        FROM row_count_diff
        INNER JOIN diff_count ON row_count_diff.id = diff_count.id
        """.strip()

        sql = COLUMNS_EQUAL_SQL.format(
            alias_a=alias_a,
            alias_b=alias_b,
            first_column=first_column,
            columns_a=columns_csv_a,
            columns_b=columns_csv_b,
            join_condition=join_condition,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    @property
    def default_python_submission_method(self):
        raise NotImplementedError("default_python_submission_method is not specified")

    @property
    def python_submission_helpers(self):
        raise NotImplementedError("python_submission_helpers is not specified")

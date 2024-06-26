import pytest

import json
from dbt.tests.util import run_dbt, run_dbt_and_capture

NUM_VIEWS = 100
NUM_EXPECTED_RELATIONS = 1 + NUM_VIEWS

TABLE_BASE_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

VIEW_X_SQL = """
select id from {{ ref('my_model_base') }}
""".lstrip()

MACROS__VALIDATE__DATABEND__LIST_RELATIONS_WITHOUT_CACHING = """
{% macro validate_list_relations_without_caching(schema_relation) %}
    {% set relation_list_result = databend__list_relations_without_caching(schema_relation, max_iter=11, max_results_per_iter=10) %}
    {% set n_relations = relation_list_result | length %}
    {{ log("n_relations: " ~ n_relations) }}
{% endmacro %}
"""

MACROS__VALIDATE__DATABEND__LIST_RELATIONS_WITHOUT_CACHING_RAISE_ERROR = """
{% macro validate_list_relations_without_caching_raise_error(schema_relation) %}
    {{ databend__list_relations_without_caching(schema_relation, max_iter=33, max_results_per_iter=3) }}
{% endmacro %}
"""


def parse_json_logs(json_log_output):
    parsed_logs = []
    for line in json_log_output.split("\n"):
        try:
            log = json.loads(line)
        except ValueError:
            continue

        parsed_logs.append(log)

    return parsed_logs


def find_result_in_parsed_logs(parsed_logs, result_name):
    return next(
        (
            item["data"]["msg"]
            for item in parsed_logs
            if result_name in item["data"].get("msg", "msg")
        ),
        False,
    )


def find_exc_info_in_parsed_logs(parsed_logs, exc_info_name):
    return next(
        (
            item["data"]["exc_info"]
            for item in parsed_logs
            if exc_info_name in item["data"].get("exc_info", "exc_info")
        ),
        False,
    )


class TestListRelationsWithoutCachingSingle:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(0, NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})

        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__DATABEND__LIST_RELATIONS_WITHOUT_CACHING,
        }

    def test__databend__list_relations_without_caching_termination(self, project):
        """
        validates that we do NOT trigger pagination logic databend__list_relations_without_caching
        macro when there are fewer than max_results_per_iter relations in the target schema
        """
        run_dbt(["run", "-s", "my_model_base"])

        database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            schema_relation = {"database": database, "schema": schema}
            kwargs = {"schema_relation": schema_relation}
            print(kwargs)
import pytest
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps


class TestCurrentTimestampDatabend(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "get_current_timestamp.sql": "select NOW()"
        }

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {"now()": "Timestamp"}

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """select NOW()"""

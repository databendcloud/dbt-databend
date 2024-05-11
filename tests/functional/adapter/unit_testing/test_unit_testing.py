import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


class TestDatabendUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["2.0", "2.0"],
            ["'12345'", "12345"],
            ["'string'", "string"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'2013-11-03 00:00:00-0'::TIMESTAMP", "2013-11-03 00:00:00-0"],
            ["3::VARIANT", "3"],
            ["TO_GEOMETRY('POINT(1820.12 890.56)')", "POINT(1820.12 890.56)"],
            [
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
            ],
            ["['a','b','c']", "['a','b','c']"],
            ["[1,2,3]", "[1, 2, 3]"],
        ]


class TestDatabendUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestDatabendUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass

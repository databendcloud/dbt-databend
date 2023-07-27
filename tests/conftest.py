import pytest

# import os
# import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "databend",
        "host": "tnf34b0rm--small-qerj.gw.aliyun-cn-beijing.default.databend.cn",
        "port": 443,
        "user": "cloudapp",
        "pass": "ckfgsdg8fxk1",
        "schema": "debezium",
        "secure": True,
    }

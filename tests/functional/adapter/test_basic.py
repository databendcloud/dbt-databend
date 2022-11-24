import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.util import check_relation_types, relation_from_name, run_dbt


class TestSimpleMaterializationsDatabend(BaseSimpleMaterializations):
    pass


# class TestSingularTestsDatabend(BaseSingularTests):
#     pass


#
#
# class TestSingularTestsEphemeralDatabend(BaseSingularTestsEphemeral):
#     pass


#
class TestEmptyDatabend(BaseEmpty):
    pass


#
#
# class TestEphemeralDatabend(BaseEphemeral):
#     pass
#
#
# #
# #
class TestIncrementalDatabend(BaseIncremental):
    pass


#
#
# class TestGenericTestsDatabend(BaseGenericTests):
#     pass


#
#
class TestSnapshotCheckColsDatabend(BaseSnapshotCheckCols):
    pass


#
#
class TestSnapshotTimestampDatabend(BaseSnapshotTimestamp):
    pass


#
#
# class TestBaseAdapterMethodDatabend(BaseAdapterMethod):
#     pass


# CSV content with boolean column type.
seeds_boolean_csv = """
key,value
abc,true
def,false
hij,true
klm,false
""".lstrip()

# CSV content with empty fields.
seeds_empty_csv = """
key,val1,val2
abc,1,1
abc,1,0
def,1,0
hij,1,1
hij,1,
klm,1,0
klm,1,
""".lstrip()

# class TestCSVSeed:
#     @pytest.fixture(scope="class")
#     def seeds(self):
#         return {"boolean.csv": seeds_boolean_csv, "empty.csv": seeds_empty_csv}
#
#     def test_seed(self, project):
#         # seed command
#         results = run_dbt(["seed"])
#         assert len(results) == 2

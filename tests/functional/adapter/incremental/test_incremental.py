from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from pytest import fixture, mark


class TestIncrementalPredicatesDeleteInsertDatabend(BaseIncrementalPredicates):
    @fixture(scope='class')
    def project_config_update(self):
        return {
            'models': {
                '+predicates': ['id != 2'],
                '+incremental_strategy': 'delete_insert',
            }
        }


@mark.skip('No support for unique keys in default incremental strategy')
class TestIncrementalUniqueKeyDatabend(BaseIncrementalUniqueKey):
    pass


class TestUniqueKeyDeleteInsertDatabend(BaseIncrementalUniqueKey):
    @fixture(scope='class')
    def project_config_update(self):
        return {'models': {'+incremental_strategy': 'delete_insert'}}

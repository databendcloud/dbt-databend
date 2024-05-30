from dbt.adapters.databend.relation import DatabendRelation
from dbt.adapters.contracts.relation import RelationType


def test_renameable_relation():
    relation = DatabendRelation.create(
        database=None,
        schema="my_schema",
        identifier="my_table",
        type=RelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
    )

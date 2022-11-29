from dbt.adapters.databend.connections import DatabendConnectionManager  # noqa
from dbt.adapters.databend.connections import DatabendCredentials
from dbt.adapters.databend.impl import DatabendAdapter
from dbt.adapters.databend.column import DatabendColumn  # noqa
from dbt.adapters.databend.relation import DatabendRelation  # noqa

from dbt.adapters.base import AdapterPlugin
from dbt.include import databend


Plugin = AdapterPlugin(
    adapter=DatabendAdapter,
    credentials=DatabendCredentials,
    include_path=databend.PACKAGE_PATH,
)

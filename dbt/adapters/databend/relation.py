from dataclasses import dataclass, field
from typing import Optional, TypeVar, Any, Type, Dict, Union, Iterator, Tuple, Set
import dbt.exceptions
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.contracts.relation import (
    Path,
    RelationType,
)


@dataclass
class DatabendQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class DatabendIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


Self = TypeVar("Self", bound="DatabendRelation")


@dataclass(frozen=True, eq=False, repr=False)
class DatabendRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: DatabendQuotePolicy())
    include_policy: DatabendIncludePolicy = field(
        default_factory=lambda: DatabendIncludePolicy()
    )
    quote_character: str = ""

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise dbt.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Databend, database must be omitted or have the same value as"
                f" schema."
            )

    @classmethod
    def create(
            cls: Type[Self],
            database: Optional[str] = None,
            schema: Optional[str] = None,
            identifier: Optional[str] = None,
            type: Optional[RelationType] = None,
            **kwargs,
    ) -> Self:
        database = None
        kwargs.update(
            {
                "path": {
                    "database": database,
                    "schema": schema,
                    "identifier": identifier,
                },
                "type": type,
            }
        )
        return cls.from_dict(kwargs)

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise dbt.exceptions.DbtRuntimeError(
                "Got a databend relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()

    @classmethod
    def get_path(
            cls, relation: BaseRelation, information_schema_view: Optional[str]
    ) -> Path:
        Path.database = None
        return Path(
            database=None,
            schema=relation.schema,
            identifier="INFORMATION_SCHEMA",
        )

    def matches(
            self,
            database: Optional[str] = None,
            schema: Optional[str] = None,
            identifier: Optional[str] = None,
    ):
        if database:
            raise dbt.exceptions.DbtRuntimeError(
                f"Passed unexpected schema value {schema} to Relation.matches"
            )
        return self.schema == schema and self.identifier == identifier

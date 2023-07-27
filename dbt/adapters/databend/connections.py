from contextlib import contextmanager
from dataclasses import dataclass

import agate
import dbt.exceptions  # noqa
from dbt.adapters.base import Credentials

from dbt.adapters.sql import SQLConnectionManager as connection_cls
from dbt.contracts.connection import AdapterResponse
from dbt.events import AdapterLogger
from typing import Optional, Tuple, List, Any
from databend_sqlalchemy import connector

from dbt.exceptions import (
    Exception,
)

logger = AdapterLogger("databend")


@dataclass
class DatabendAdapterResponse(AdapterResponse):
    pass


@dataclass
class DatabendCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    secure: Optional[bool] = None

    # Add credentials members here, like:
    # host: str
    # port: int
    # username: str
    # password: str

    _ALIASES = {"dbname": "database", "pass": "password", "user": "username"}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.database = None

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    def __post_init__(self):
        # databend classifies database and schema as the same thing
        self.database = None
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.Exception(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Databend, database must be omitted or have the same value as"
                f" schema."
            )

    @property
    def type(self):
        """Return name of adapter."""
        return "databend"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.schema

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("host", "port", "database", "schema", "user")


class DatabendConnectionManager(connection_cls):
    TYPE = "databend"

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        try:
            yield

        except Exception as e:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            raise dbt.exceptions.Exception(str(e))

    # except for DML statements where explicitly defined
    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            # handle = mysql.connector.connect(
            #     # host=credentials.host,
            #     # port=credentials.port,
            #     # user=credentials.username,
            #     # password=credentials.password,
            # )
            if credentials.secure is None:
                credentials.secure = True

            if credentials.secure:
                handle = connector.connect(
                    f"https://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.schema}?secure=true "
                )
            else:
                handle = connector.connect(
                    f"http://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.schema}?secure=false "
                )

        except Exception as e:
            logger.debug("Error opening connection: {}".format(e))
            connection.handle = None
            connection.state = "fail"
            raise dbt.exceptions.FailedToConnectException(str(e))
        connection.state = "open"
        connection.handle = handle
        return connection

    @classmethod
    def get_response(cls, _):
        return "OK"

    def execute(
            self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        # don't apply the query comment here
        # it will be applied after ';' queries are split
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        # table: rows, column_names=None, column_types=None, row_names=None
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return response, table

    def add_query(self, sql, auto_begin=False, bindings=None, abridge_sql_log=False):
        connection, cursor = super().add_query(
            sql, auto_begin, bindings=bindings, abridge_sql_log=abridge_sql_log
        )

        if cursor is None:
            conn = self.get_thread_connection()
            if conn is None or conn.name is None:
                conn_name = "<None>"
            else:
                conn_name = conn.name

            raise Exception(
                "Tried to run an empty query on model '{}'. If you are "
                "conditionally running\nsql, eg. in a model hook, make "
                "sure your `else` clause contains valid sql!\n\n"
                "Provided SQL:\n{}".format(conn_name, sql)
            )

        return connection, cursor

    @classmethod
    def get_status(cls, _):
        """
        Returns connection status
        """
        return "OK"

    @classmethod
    def get_credentials(cls, credentials):
        """
        Returns Databend credentials
        """
        return credentials

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection_name = connection.name
        logger.debug("Cancelling query '{}'", connection_name)
        connection.handle.close()
        logger.debug("Cancel query '{}'", connection_name)

    @classmethod
    def process_results(cls, column_names, rows):

        return [dict(zip(column_names, row)) for row in rows]

    @classmethod
    def get_result_from_cursor(cls, cursor: Any) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(data, column_names)

"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import logging
import functools
import inspect

import sqlalchemy

from .mappers import (
    BaseMapper,
    DbMapResult,
    RecordCombiningMapper,
)
from .query_utils import get_query_data


logger = logging.getLogger('database')

DEFAULT_DATABASE = None
DATABASES: dict = {}


def set_default_database(database_name):
    # pylint: disable=global-statement
    global DEFAULT_DATABASE
    DEFAULT_DATABASE = database_name


def set_database_parameters(
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
        pool_size: int = 10,
        pool_recycle: int = 3600,
        echo_queries: bool = False,
):  # pylint: disable=too-many-arguments
    """
    Initializes the parameters to use when connecting to the database. This is a subset of the parameters
    used by sqlalchemy.

    :param user: user to connect to the database with
    :param password: password for given user
    :param host: the db host to try to connect to
    :param database: database to connect to
    :param port: the port to connect to (default 3306)
    :param pool_size: number of connections to maintain in the connection pool (default 10)
    :param pool_recycle: amount of time to wait between resetting the connections
                         in the pool (default 3600)
    :param echo_queries: this tells sqlalchemy to print the queries when set to True (default false)
    :exception DBNotPrepareError: happens when required parameters are missing
    """
    # pylint: disable=global-statement
    global DEFAULT_DATABASE
    DEFAULT_DATABASE = database
    required_param_missing = 'Database parameter "{}" is not set or empty and is required'
    if not host:
        raise DBNotPreparedError(required_param_missing.format('host'))
    if not user:
        raise DBNotPreparedError(required_param_missing.format('user'))
    if not password:
        raise DBNotPreparedError(required_param_missing.format('password'))
    if not database:
        raise DBNotPreparedError(required_param_missing.format('database'))

    DATABASES[database] = {
        'parameters': {
            'user': user,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'pool_recycle': pool_recycle,
            'pool_size': pool_size,
            'echo_queries': echo_queries,
        },
        'engine': None
    }


class _ConnectionManager:
    _connection: sqlalchemy.engine.base.Connection

    # pylint: disable=unused-argument
    def __init__(self, func, isolation_level, transaction, *args, **kwargs):
        self._transaction = None

        try:
            self._init_database()
        except DBNotPreparedError as dbe:
            raise dbe

        engine: sqlalchemy.engine.Engine = DATABASES[DEFAULT_DATABASE]['engine']
        self._connection = engine.connect().execution_options(isolation_level=isolation_level)
        if transaction:
            self._transaction = self._connection.begin()

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Handle transaction committing/rollback before closing the connection
        if exc_type:
            logger.error(f"Encountered an error : {exc_val}")

            # Roll back transaction if there is one
            if self._transaction:
                self._transaction.rollback()
        elif self._transaction:
            self._transaction.commit()

        # Close the connection
        self._connection.__exit__(exc_type, exc_val, exc_tb)

    @staticmethod
    def _init_database() -> None:
        current_database = DATABASES.get(DEFAULT_DATABASE, {})
        database_parameters = current_database.get('parameters', {})

        if not database_parameters:
            raise DBNotPreparedError(
                'Unable to connect to a database, set_database_parameters must first be called')

        if current_database.get('engine') is None:
            user = database_parameters.get('user')
            password = database_parameters.get('password')
            host = database_parameters.get('host')
            port = database_parameters.get('port')
            database = database_parameters.get('database')
            url = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}?charset=utf8'

            current_database['engine'] = sqlalchemy.create_engine(
                url,
                pool_recycle=database_parameters.get('pool_recycle'),
                pool_size=database_parameters.get('pool_size'),
                echo=database_parameters.get('echo_queries'),
                pool_pre_ping=True,
            )

    def execute_query(self, query, params=None) -> sqlalchemy.engine.CursorResult:
        """
        Executes the query through the connection with optional parameters.
        :param query: The query to run
        :param params: The parameters to use
        :return: The result proxy object from sqlalchemy
        """
        if not params:
            params = {}

        if isinstance(params, DbMapResult):
            params = params.raw()

        # if params:
        #     logger.debug("Executing query {} with parameters {}".format(query, params))
        # else:
        #     logger.debug("Executing query {} without parameters".format(query))
        return self._connection.execute(sqlalchemy.text(query), params)


def sqlquery(mapper: BaseMapper = None, isolation_level: str = 'READ_COMMITTED'):
    """
    query allows for defining a parameterize select query that is then executed
    :param mapper: a class extending from or an instance of BaseMapper, defaults to
        RecordCombiningMapper
    :param isolation_level: the isolation level to use for the transaction, defaults to
        'READ_COMMITTED'
    :return: Depends on the mapper used, see mapper documentation for more details.

    Examples::

    # Uses the default RecordCombiningMapper, returns a list of results
    @sqlquery()
    def get_items(name, date):
        return QueryData("SELECT item1, item2, ... FROM item join item_data ...
                WHERE name=:name and date before :date"), {'name': name, 'date': date})

    # Override the mapper to return a list of scalars
    @sqlquery(mapper=SingleColumnMapper)
    def get_items():
        return QueryData("SELECT item1 FROM item join item_data ...")

    # Override the mapper to return a single (first) result
    @sqlquery(mapper=SingleRowMapper)
    def get_items():
        return QueryData("SELECT item1, item2, ... FROM item join item_data ...")

    # Override the mapper to return a single result from the the first result
    @sqlquery(mapper=CountMapper)
    def get_count():
        return QueryData("SELECT count(*) FROM item GROUP BY item1")
    """

    def decorator(func):
        def handle_query(*args, **kwargs):
            functools.wraps(func, handle_query)
            actual_mapper = mapper
            if not actual_mapper:
                # Default to record combining mapper
                actual_mapper = RecordCombiningMapper

            # Handle mapper defined as a class
            if inspect.isclass(actual_mapper):
                actual_mapper = actual_mapper()

            with _ConnectionManager(func, isolation_level, False, *args, **kwargs) as conn_manager:
                data = func(*args, **kwargs)
                query, params = get_query_data(data)
                records = conn_manager.execute_query(query, params)
                return actual_mapper.map_records(records)

        return handle_query

    return decorator


def sqlexists(isolation_level='READ_COMMITTED'):
    """
    exists query allows for defining a parameterize select query that is
    wrapped in an exists clause and then executed
    :param isolation_level: the isolation level to use for the transaction, defaults to
        'READ_COMMITTED'
    :return: returns a True or False depending on the value returned from
        the exists query. If Exists returns 1, True. otherwise, False
    Examples::

    @sqlexists
    def get_items_exists(name, date):
        return "SELECT 1 FROM item join item_data ...
                WHERE name=:name and date before :date", { name=name, date=date}

    @sqlexists
    def get_items_exists():
        return "SELECT 1 FROM item WHERE date before now()-100

    """

    def decorator(func):
        def handle_query(*args, **kwargs):
            functools.wraps(func, handle_query)
            with _ConnectionManager(func, isolation_level, False, *args, **kwargs) as conn_manager:
                data = func(*args, **kwargs)
                query, params = get_query_data(data)

                query = query.lstrip()
                if not query.startswith('SELECT EXISTS'):
                    query = 'SELECT EXISTS ( {} )'.format(query)
                result = conn_manager.execute_query(query, params).scalar()
                return result == 1

        return handle_query

    return decorator


def sqlupdate(isolation_level='READ_COMMITTED', disable_foreign_key_checks=False):
    """
    :param isolation_level should specify whether we can read data from transactions that are not
        yet committed defaults to READ_COMMITTED
    :param disable_foreign_key_checks Should be used with caution. Only use this if you have
        confidence the data you are inserting is going to have everything it needs to satisfy
        foreign_key_checks once all the data is applied. Foreign key checks are there to provide
        our database with integrety

        That being said, there are times when you are syncronizing data from another source that
        you have data that may be difficult to sort ahead of time in order to avoid failing foreign
        key constraints this flag helps to deal with cases where the logic may be unnecessarily
        complex.

        Note: this will work as expected when a normal transaction completes successfully and if a
        transaction rolls back this will be left in a clean state as expected before executing
        anything
    Examples::

    @sqlinsert
    def insert_example(key_values)
        return "INSERT INTO table(id, value) VALUES (:id, :value)", key_values

    @sqlinsert
    def delete_example(ids)
        return "DELETE FROM table", key_values

    """

    def update_wrapper(func):
        """
        :param func: should return a tuple representing the query followed by the dictionary of key
            values for query parameters
        """

        def handle_query(*args, **kwargs):
            functools.wraps(func)
            with _ConnectionManager(func, isolation_level, True, *args, **kwargs) as conn_manager:

                if disable_foreign_key_checks:
                    conn_manager.execute_query("SET FOREIGN_KEY_CHECKS=0")

                if inspect.isgeneratorfunction(func):
                    logger.debug("handling each query before committing transaction")

                    for data in func(*args, **kwargs):
                        query, params = get_query_data(data)
                        if isinstance(params, list):
                            for param in params:
                                conn_manager.execute_query(query, param)
                        else:
                            conn_manager.execute_query(query, params)
                else:
                    data = func(*args, **kwargs)
                    query, params = get_query_data(data)
                    if isinstance(params, list):
                        raise Exception('Params must not be a list')
                    conn_manager.execute_query(query, params)

                if disable_foreign_key_checks:
                    conn_manager.execute_query("SET FOREIGN_KEY_CHECKS=1")

        return handle_query

    return update_wrapper


class DBNotPreparedError(Exception):
    """
    DB Not Prepared Error
    """

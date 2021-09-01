"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import logging
import functools
import inspect
from typing import Type, Union

import sqlalchemy

from .databases import DatabaseContainerSingleton
from .mappers import (
    BaseMapper,
    DbMapResult,
    RecordCombiningMapper,
)
from .query_utils import get_query_data


logger = logging.getLogger('database')


# Always initialize a database container, it is never set again
_DATABASE_CONTAINER = DatabaseContainerSingleton()


class _ConnectionManager:
    _connection: sqlalchemy.engine.base.Connection

    # pylint: disable=unused-argument
    def __init__(self, func, isolation_level, transaction, *args, **kwargs):
        self._transaction = None

        self._connection = _DATABASE_CONTAINER.current_database.engine.connect().execution_options(
            isolation_level=isolation_level
        )
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


def sqlquery(mapper: Union[BaseMapper, Type[BaseMapper]] = None, isolation_level: str = 'READ_COMMITTED'):
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

        That being said, there are times when you are synchronizing data from another source that
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

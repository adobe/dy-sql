"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

import logging
import sys
from typing import Optional

import sqlalchemy

from .exceptions import DBNotPreparedError


logger = logging.getLogger('database')

_DEFAULT_CONNECTION_PARAMS = {}

try:
    import contextvars
    CURRENT_DATABASE_VAR = contextvars.ContextVar("dysql_current_database", default='')
except ImportError:
    CURRENT_DATABASE_VAR = None


def is_set_current_database_supported() -> bool:
    """
    Determines if the set_current_database method is available on this python runtime.
    :return: True if available, False otherwise
    """
    return bool(CURRENT_DATABASE_VAR)


def set_current_database(database: str) -> None:
    """
    Sets the current database, may be used for multitenancy. This is only supported on Python 3.7+. This uses
    contextvars internally to set the name for the current async context.
    :param database: the database name to use for this async context
    """
    if not CURRENT_DATABASE_VAR:
        raise DBNotPreparedError(
            f'Cannot set the current database on Python "{sys.version}", please upgrade your Python version'
        )
    CURRENT_DATABASE_VAR.set(database)
    logger.debug(f'Set current database to {database}')


def reset_current_database() -> None:
    """
    Helper method to reset the current database to the default. Internally, this calls `set_current_database` with
    an empty string.
    """
    set_current_database('')


def _get_current_database() -> str:
    """
    The current database name, using contextvars (if on python 3.7+) or the default database name.
    :return: The current database name
    """
    database: Optional[str] = None
    if CURRENT_DATABASE_VAR:
        database = CURRENT_DATABASE_VAR.get()
    if not database:
        database = _DEFAULT_CONNECTION_PARAMS.get('database')
    return database


def _validate_param(name: str, value: str) -> None:
    if not value:
        raise DBNotPreparedError(f'Database parameter "{name}" is not set or empty and is required')


def set_default_connection_parameters(
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
        pool_size: int = 10,
        pool_recycle: int = 3600,
        echo_queries: bool = False,
        charset: str = 'utf8'
):  # pylint: disable=too-many-arguments,unused-argument
    """
    Initializes the parameters to use when connecting to the database. This is a subset of the parameters
    used by sqlalchemy. These may be overridden by parameters provided in the QueryData, hence the "default".

    :param host: the db host to try to connect to
    :param user: user to connect to the database with
    :param password: password for given user
    :param database: database to connect to
    :param port: the port to connect to (default 3306)
    :param pool_size: number of connections to maintain in the connection pool (default 10)
    :param pool_recycle: amount of time to wait between resetting the connections
                         in the pool (default 3600)
    :param echo_queries: this tells sqlalchemy to print the queries when set to True (default false)
    :param charset: the charset for the sql engine to initialize with. (default utf8)
    :exception DBNotPrepareError: happens when required parameters are missing
    """
    _validate_param('host', host)
    _validate_param('user', user)
    _validate_param('password', password)
    _validate_param('database', database)

    _DEFAULT_CONNECTION_PARAMS.update(locals())


class Database:
    # pylint: disable=too-few-public-methods

    def __init__(self, database: Optional[str]) -> None:
        self.database = database
        # Engine is lazy-initialized
        self._engine: Optional[sqlalchemy.engine.Engine] = None

    @property
    def engine(self):
        if not self._engine:
            user = _DEFAULT_CONNECTION_PARAMS.get('user')
            password = _DEFAULT_CONNECTION_PARAMS.get('password')
            host = _DEFAULT_CONNECTION_PARAMS.get('host')
            port = _DEFAULT_CONNECTION_PARAMS.get('port')
            charset = _DEFAULT_CONNECTION_PARAMS.get('charset')

            url = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{self.database}?charset={charset}'
            self._engine = sqlalchemy.create_engine(
                url,
                pool_recycle=_DEFAULT_CONNECTION_PARAMS.get('pool_recycle'),
                pool_size=_DEFAULT_CONNECTION_PARAMS.get('pool_size'),
                echo=_DEFAULT_CONNECTION_PARAMS.get('echo_queries'),
                pool_pre_ping=True,
            )
        return self._engine


class DatabaseContainer(dict):
    """
    Implementation of a dictionary that always provides a Database class instance, even if the key is missing.
    """
    def __getitem__(self, database: Optional[str]) -> Database:
        """
        Override getitem to always return an instance of a database, which includes a lazy-initialized engine.
        This also ensures that the database parameters have been initialized before attempting to retrieve a database.
        :param database: the database name (may be null for the default database)
        :return: a database instance
        :raises DBNotPreparedError: when set_default_connection_parameters has not yet been called
        """
        if not _DEFAULT_CONNECTION_PARAMS:
            raise DBNotPreparedError(
                'Unable to connect to a database, set_default_connection_parameters must first be called'
            )

        if not super().__contains__(database):
            super().__setitem__(database, Database(database))
        return super().__getitem__(database)

    @property
    def current_database(self) -> Database:
        """
        The current database instance, retrieved using contextvars (if python 3.7+) or the default database.
        """
        return self.__getitem__(_get_current_database())


class DatabaseContainerSingleton(DatabaseContainer):
    """
    All instantiations of this class will result in the same instance every time due to the override of
    the __new__ method.
    """
    def __new__(cls, *args, **kwargs) -> 'DatabaseContainer':
        instance = cls.__dict__.get("__instance__")
        if instance is not None:
            return instance
        cls.__instance__ = instance = DatabaseContainer.__new__(cls)
        instance.__init__(*args, **kwargs)
        return instance

"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

import contextvars
import logging
from collections import defaultdict
from typing import Callable, Optional

import sqlalchemy

from .exceptions import DBNotPreparedError


logger = logging.getLogger("database")

_DEFAULT_CONNECTION_PARAMS_BY_KEY = defaultdict(dict)
CURRENT_DATABASE_VAR = contextvars.ContextVar("dysql_current_database", default="")
# Database key used when no current database is set; None means the first registered key
_DEFAULT_DATABASE_KEY: Optional[str] = None


def set_database_init_hook(
    hook_method: Callable[[Optional[str], sqlalchemy.engine.Engine], None],
) -> None:
    """
    Sets an initialization hook whenever a new database is initialized. This method will receive the database name
    (may be none) and the sqlalchemy engine as parameters.

    :param hook_method: the hook method
    """
    Database.set_init_hook(hook_method)


def is_set_current_database_supported() -> bool:
    """
    Deprecated, left in for backwards compatibility but always returns true.
    :return: True
    """
    return True


def set_current_database(database_key: str) -> None:
    """
    Sets the current database key, may be used for multitenancy. This is only supported on Python 3.7+. This uses
    contextvars internally to set the name for the current async context.
    :param database_key: the arbitrary database key to use for this async context
    """
    CURRENT_DATABASE_VAR.set(database_key)
    logger.debug(f"Set current database to {database_key}")


def reset_current_database() -> None:
    """
    Helper method to reset the current database to the default. Internally, this calls `set_current_database` with
    an empty string.
    """
    set_current_database("")


def get_default_database_key() -> Optional[str]:
    """
    The database key used when no current database is set: the key set with `set_default_database_key`,
    otherwise the first key passed to `set_default_connection_parameters`.
    :return: The default database key, or None if no databases have been set up
    """
    if _DEFAULT_DATABASE_KEY in _DEFAULT_CONNECTION_PARAMS_BY_KEY:
        return _DEFAULT_DATABASE_KEY
    if _DEFAULT_CONNECTION_PARAMS_BY_KEY:
        return next(iter(_DEFAULT_CONNECTION_PARAMS_BY_KEY))
    return None


def _get_explicit_default_database_key() -> Optional[str]:
    """
    The key set with `set_default_database_key`, without the first-key fallback. Used to restore it later.
    """
    return _DEFAULT_DATABASE_KEY


def set_default_database_key(database_key: Optional[str]) -> None:
    """
    Sets the database key used when no current database is set. Unlike `set_current_database`, this applies to
    every thread and async context, not just the current one. Pass None to go back to using the first key passed
    to `set_default_connection_parameters`.
    :param database_key: a key previously passed to `set_default_connection_parameters`, or None
    :raises DBNotPreparedError: if the key has no connection parameters
    """
    global _DEFAULT_DATABASE_KEY  # pylint: disable=global-statement
    if (
        database_key is not None
        and database_key not in _DEFAULT_CONNECTION_PARAMS_BY_KEY
    ):
        raise DBNotPreparedError(
            f"No connection parameters found for database key '{database_key}'"
        )
    _DEFAULT_DATABASE_KEY = database_key
    logger.debug(f"Set default database to {database_key}")


def _get_current_database_key() -> str:
    """
    The current database key, using contextvars (if on python 3.7+) or the default database key.
    :return: The current database key
    """
    database: Optional[str] = None
    if CURRENT_DATABASE_VAR:
        database = CURRENT_DATABASE_VAR.get()
    if not database:
        database = get_default_database_key()
    return database


def _validate_param(name: str, value: str) -> None:
    if not value:
        raise DBNotPreparedError(
            f'Database parameter "{name}" is not set or empty and is required'
        )


def set_default_connection_parameters(
    host: str,
    user: str,
    password: str,
    database: str,
    database_key: Optional[str] = None,
    port: int = 3306,
    pool_size: int = 10,
    pool_recycle: int = 3600,
    echo_queries: bool = False,
    charset: str = "utf8",
    collation: Optional[str] = None,
):
    """
    Initializes the parameters to use when connecting to the database. This is a subset of the parameters
    used by sqlalchemy. These may be overridden by parameters provided in the QueryData, hence the "default".

    :param host: the db host to try to connect to
    :param user: user to connect to the database with
    :param password: password for given user
    :param database: database to connect to
    :param database_key: optional database key that may be used for multitenant DBs, defaults to the database name
    :param port: the port to connect to (default 3306)
    :param pool_size: number of connections to maintain in the connection pool (default 10)
    :param pool_recycle: amount of time to wait between resetting the connections
                         in the pool (default 3600)
    :param echo_queries: this tells sqlalchemy to print the queries when set to True (default false)
    :param charset: the charset for the sql engine to initialize with. (default utf8)
    :param collation: the collation for the sql engine to initialize with. (default is not set)
    :exception DBNotPrepareError: happens when required parameters are missing
    """
    _validate_param("host", host)
    _validate_param("user", user)
    _validate_param("password", password)
    _validate_param("database", database)

    if not database_key:
        database_key = database
    _DEFAULT_CONNECTION_PARAMS_BY_KEY[database_key].update(locals())


class Database:
    def __init__(self, database_key: Optional[str]) -> None:
        self.database = database_key
        # Engine is lazy-initialized
        self._engine: Optional[sqlalchemy.engine.Engine] = None

    @classmethod
    def set_init_hook(
        cls,
        hook_method: Callable[[Optional[str], sqlalchemy.engine.Engine], None],
    ) -> None:
        cls.hook_method = hook_method

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        if not self._engine:
            connection_params = _DEFAULT_CONNECTION_PARAMS_BY_KEY.get(self.database, {})
            if not connection_params:
                raise DBNotPreparedError(
                    f"No connection parameters found for database key '{self.database}'"
                )
            user = connection_params.get("user")
            password = connection_params.get("password")
            database = connection_params.get("database")
            host = connection_params.get("host")
            port = connection_params.get("port")
            charset = connection_params.get("charset")
            collation = connection_params.get("collation")
            collation_str = ""
            if collation:
                collation_str = f"&collation={collation}"

            url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}?charset={charset}{collation_str}"
            self._engine = sqlalchemy.create_engine(
                url,
                pool_recycle=connection_params.get("pool_recycle"),
                pool_size=connection_params.get("pool_size"),
                echo=connection_params.get("echo_queries"),
                pool_pre_ping=True,
            )
            hook_method: Optional[
                Callable[[Optional[str], sqlalchemy.engine.Engine], None]
            ] = getattr(self.__class__, "hook_method", None)
            if hook_method:
                hook_method(database, self._engine)

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
        if not _DEFAULT_CONNECTION_PARAMS_BY_KEY:
            raise DBNotPreparedError(
                "Unable to connect to a database, set_default_connection_parameters must first be called"
            )

        if not super().__contains__(database):
            super().__setitem__(database, Database(database))
        return super().__getitem__(database)

    @property
    def current_database(self) -> Database:
        """
        The current database instance, retrieved using contextvars (if python 3.7+) or the default database.
        """
        return self.__getitem__(_get_current_database_key())


class DatabaseContainerSingleton(DatabaseContainer):
    """
    All instantiations of this class will result in the same instance every time due to the override of
    the __new__ method.
    """

    def __new__(cls, *args, **kwargs) -> "DatabaseContainer":
        instance = cls.__dict__.get("__instance__")
        if instance is not None:
            return instance
        cls.__instance__ = instance = DatabaseContainer.__new__(cls)
        instance.__init__(*args, **kwargs)
        return instance

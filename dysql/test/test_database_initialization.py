"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

import concurrent.futures
from unittest import mock

import pytest

import dysql.connections
from dysql import (
    sqlquery,
    DBNotPreparedError,
    set_default_connection_parameters,
    QueryData,
    set_database_init_hook,
)
from dysql.test import setup_mock_engine


"""
These tests reflect what can happen when dealing with a config passing in a value that may not
have been set. There are some parameters that are absolutely necessary, if they aren't set we
want to make sure we are warning the user in different ways.
"""


@sqlquery()
def query():
    """
    This is used to call the code that would initialize the database on the first time.
    If there were any failures this is where we would expect to see them.
    """
    return QueryData("SELECT * FROM table")


@pytest.fixture(autouse=True, name="mock_engine")
def fixture_mock_engine(mock_create_engine):
    dysql.databases.DatabaseContainerSingleton().clear()
    dysql.databases._DEFAULT_CONNECTION_PARAMS_BY_KEY.clear()
    dysql.databases.set_default_database_key(None)

    # Reset the database before the test
    dysql.databases.reset_current_database()

    yield setup_mock_engine(mock_create_engine)

    # Reset database after the test as well
    dysql.databases.reset_current_database()


@pytest.fixture(autouse=True)
def fixture_reset_init_hook():
    yield
    if hasattr(dysql.databases.Database, "hook_method"):
        delattr(dysql.databases.Database, "hook_method")


def test_nothing_set():
    dysql.databases._DEFAULT_CONNECTION_PARAMS_BY_KEY.clear()
    with pytest.raises(DBNotPreparedError) as error:
        query()
    assert (
        str(error.value)
        == "Unable to connect to a database, set_default_connection_parameters must first be called"
    )


@pytest.mark.parametrize(
    "host, user, password, database, failed_field",
    [
        (None, "u", "p", "d", "host"),
        ("", "u", "p", "d", "host"),
        ("h", None, "p", "d", "user"),
        ("h", "", "p", "d", "user"),
        ("h", "u", None, "d", "password"),
        ("h", "u", "", "d", "password"),
        ("h", "u", "p", None, "database"),
        ("h", "u", "p", "", "database"),
    ],
)
def test_fields_required(host, user, password, database, failed_field):
    with pytest.raises(DBNotPreparedError) as error:
        set_default_connection_parameters(host, user, password, database)
    assert (
        str(error.value)
        == f'Database parameter "{failed_field}" is not set or empty and is required'
    )


def test_minimal_credentials(mock_engine):
    set_default_connection_parameters("h", "u", "p", "d")

    mock_engine.connect().execution_options().execute.return_value = []
    query()


def test_init_hook(mock_engine):
    init_hook = mock.MagicMock()
    set_database_init_hook(init_hook)
    set_default_connection_parameters("h", "u", "p", "d")
    dysql.databases.set_current_database("d")

    mock_engine.connect().execution_options().execute.return_value = []
    query()
    init_hook.assert_called_once_with("d", mock_engine)


def test_init_hook_multiple_databases(mock_engine):
    init_hook = mock.MagicMock()
    set_database_init_hook(init_hook)
    set_default_connection_parameters("h1", "u1", "p1", "d1")
    set_default_connection_parameters("h2", "u2", "p2", "d2")

    mock_engine.connect().execution_options().execute.return_value = []
    query()
    dysql.databases.set_current_database("d1")
    query()
    assert init_hook.call_args_list == [
        mock.call("test", mock_engine),
        mock.call("d1", mock_engine),
    ]
    dysql.databases.set_current_database("d2")
    query()
    assert init_hook.call_args_list == [
        mock.call("test", mock_engine),
        mock.call("d1", mock_engine),
        mock.call("d2", mock_engine),
    ]


def test_current_database_default(mock_engine, mock_create_engine):
    db_container = dysql.databases.DatabaseContainerSingleton()
    assert len(db_container) == 0
    mock_engine.connect().execution_options().execute.return_value = []
    query()

    # Only one database is initialized
    assert len(db_container) == 1
    assert "test" in db_container
    assert db_container.current_database.database == "test"
    mock_create_engine.assert_called_once_with(
        "mysql+mysqlconnector://user:password@fake:3306/test?charset=utf8",
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
    )


def test_different_charset_collation(mock_engine, mock_create_engine):
    db_container = dysql.databases.DatabaseContainerSingleton()
    set_default_connection_parameters(
        "host",
        "user",
        "password",
        "database",
        charset="other",
        collation="other_collation",
    )
    assert len(db_container) == 0
    dysql.databases.set_current_database("database")
    mock_engine.connect().execution_options().execute.return_value = []
    query()

    # Only one database is initialized
    mock_create_engine.assert_called_once_with(
        "mysql+mysqlconnector://user:password@host:3306/database?charset=other&collation=other_collation",
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
    )


def test_is_set_current_database_supported():
    # This test only asserts that true is always returned since this method is deprecated
    assert dysql.databases.is_set_current_database_supported()


def test_current_database_set(mock_engine, mock_create_engine):
    db_container = dysql.databases.DatabaseContainerSingleton()
    set_default_connection_parameters("h1", "u1", "p1", "d1", database_key="db1")
    dysql.databases.set_current_database("db1")
    mock_engine.connect().execution_options().execute.return_value = []
    query()

    assert len(db_container) == 1
    assert "db1" in db_container
    assert db_container.current_database.database == "db1"
    mock_create_engine.assert_called_once_with(
        "mysql+mysqlconnector://u1:p1@h1:3306/d1?charset=utf8",
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
    )


def test_current_database_cached(mock_engine, mock_create_engine):
    db_container = dysql.databases.DatabaseContainerSingleton()
    mock_engine.connect().execution_options().execute.return_value = []
    query()

    assert len(db_container) == 1
    assert "test" in db_container
    assert db_container.current_database.database == "test"

    set_default_connection_parameters("h1", "u1", "p1", "db1")
    dysql.databases.set_current_database("db1")
    query()
    assert len(db_container) == 2
    assert "test" in db_container
    assert db_container.current_database.database == "db1"

    # Set back to default
    dysql.databases.reset_current_database()
    query()
    assert len(db_container) == 2
    assert db_container.current_database.database == "test"

    assert mock_create_engine.call_count == 2
    assert mock_create_engine.call_args_list == [
        mock.call(
            "mysql+mysqlconnector://user:password@fake:3306/test?charset=utf8",
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=10,
        ),
        mock.call(
            "mysql+mysqlconnector://u1:p1@h1:3306/db1?charset=utf8",
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=10,
        ),
    ]


def test_default_database_is_first_registered(mock_engine):
    set_default_connection_parameters("h1", "u1", "p1", "d1")
    set_default_connection_parameters("h2", "u2", "p2", "d2")
    assert dysql.databases.get_default_database_key() == "test"
    assert (
        dysql.databases.DatabaseContainerSingleton().current_database.database == "test"
    )


def test_set_default_database_key(mock_engine):
    set_default_connection_parameters("h1", "u1", "p1", "d1")
    set_default_connection_parameters("h2", "u2", "p2", "d2")
    db_container = dysql.databases.DatabaseContainerSingleton()

    dysql.databases.set_default_database_key("d2")
    assert dysql.databases.get_default_database_key() == "d2"
    assert db_container.current_database.database == "d2"

    # The current database still takes precedence over the default
    dysql.databases.set_current_database("d1")
    assert db_container.current_database.database == "d1"
    dysql.databases.reset_current_database()
    assert db_container.current_database.database == "d2"

    # None goes back to the first registered database
    dysql.databases.set_default_database_key(None)
    assert db_container.current_database.database == "test"


def test_set_default_database_key_applies_to_other_threads(mock_engine):
    set_default_connection_parameters("h2", "u2", "p2", "d2")
    dysql.databases.set_default_database_key("d2")
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        key = executor.submit(dysql.databases._get_current_database_key).result()
    assert key == "d2"


def test_set_default_database_key_unknown():
    with pytest.raises(DBNotPreparedError) as error:
        dysql.databases.set_default_database_key("missing")
    assert (
        str(error.value) == "No connection parameters found for database key 'missing'"
    )

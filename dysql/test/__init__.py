"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
from unittest.mock import Mock, patch
import pytest

from dysql import set_default_connection_parameters, databases


@pytest.fixture(name="mock_create_engine")
def mock_create_engine_fixture():
    create_mock = patch("dysql.databases.sqlalchemy.create_engine")
    try:
        yield create_mock.start()
    finally:
        create_mock.stop()


def setup_mock_engine(mock_create_engine):
    """
    build up the basics of a mock engine for the database
    :return: mocked engine for use and manipulation in testing
    """
    mock_engine = Mock()
    mock_engine.connect().execution_options().__enter__ = Mock()
    mock_engine.connect().execution_options().__exit__ = Mock()
    set_default_connection_parameters("fake", "user", "password", "test")

    # Clear out the databases before attempting to mock anything
    databases.DatabaseContainerSingleton().clear()
    mock_create_engine.return_value = mock_engine
    return mock_engine


def _verify_query_params(mock_engine, expected_query, expected_args):
    _verify_query(mock_engine, expected_query)
    _verify_query_args(mock_engine, expected_args)


def _verify_query(mock_engine, expected_query):
    execute_call = (
        mock_engine.connect.return_value.execution_options.return_value.execute
    )
    execute_call.assert_called()

    query = execute_call.call_args[0][0].text
    assert query == expected_query


def _verify_query_args(mock_engine, expected_args):
    execute_call = (
        mock_engine.connect.return_value.execution_options.return_value.execute
    )
    query_args = execute_call.call_args[0][1]

    assert query_args
    for expected_key in expected_args:
        expected_value = expected_args[expected_key]
        assert query_args.get(expected_key)
        assert expected_value == query_args[expected_key]

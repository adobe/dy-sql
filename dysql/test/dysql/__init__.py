"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
from unittest.mock import Mock, patch

from dysql import set_database_parameters


def setup_mock_engine():
    """
    build up the basics of a mock engin for the database
    :return: mocked engine for use and manipulation in testing
    """
    mock_engine = Mock()
    mock_engine.connect().execution_options().__enter__ = Mock()
    mock_engine.connect().execution_options().__exit__ = Mock()
    set_database_parameters('fake', 'user', 'password', 'test')

    create_mock = patch('dysql.connections.sqlalchemy.create_engine').start()
    create_mock.return_value = mock_engine
    return mock_engine


def _verify_query_params(mock_engine, expected_query, expected_args):
    _verify_query(mock_engine, expected_query)
    _verify_query_args(mock_engine, expected_args)


def _verify_query(mock_engine, expected_query):
    execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
    execute_call.assert_called()

    query = execute_call.call_args[0][0].text
    assert query == expected_query


def _verify_query_args(mock_engine, expected_args):
    execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
    query_args = execute_call.call_args[0][1]

    assert query_args
    for expected_key in expected_args:
        expected_value = expected_args[expected_key]
        assert query_args.get(expected_key)
        assert expected_value == query_args[expected_key]

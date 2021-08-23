"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
from unittest.mock import Mock

import pytest

from dysql import sqlexists, QueryData
from dysql.test.dysql import mock_create_engine, setup_mock_engine


_ = mock_create_engine

TRUE_QUERY = 'SELECT 1 from table'
TRUE_QUERY_PARAMS = 'SELECT 1 from table where key=:key'
FALSE_QUERY = 'SELECT 1 from false_table '
FALSE_QUERY_PARAMS = 'SELECT 1 from table where key=:key'
TRUE_PARAMS = {'key': 123}
FALSE_PARAMS = {'key': 456}
BASE_EXISTS_QUERY = 'SELECT EXISTS ( {} )'
EXISTS_QUERIES = {
    'true': BASE_EXISTS_QUERY.format(TRUE_QUERY),
    'false': BASE_EXISTS_QUERY.format(FALSE_QUERY),
    'true_params': BASE_EXISTS_QUERY.format(TRUE_QUERY_PARAMS),
    'false_params': BASE_EXISTS_QUERY.format(FALSE_QUERY_PARAMS),
}


@pytest.fixture(autouse=True)
def mock_engine_fixture(mock_create_engine):
    mock_engine = setup_mock_engine(mock_create_engine)
    mock_engine.connect.return_value.execution_options.return_value.execute.side_effect = \
        _check_query_and_return_result
    mock_engine.connect().execution_options().__enter__ = Mock()
    mock_engine.connect().execution_options().__exit__ = Mock()


def test_exists_true():
    assert _exists_true()


def test_exists_false():
    assert not _exists_false()


def test_exists_true_params():
    assert _exists_true_params()


def test_exists_false_params():
    assert not _exists_false_params()


def test_exists_query_contains_with_exists_true():
    """
    this helps to test that if someone adds an exists statement, we don't add the exists
    exists(exists()) will always give a 'true' result
    :return:
    """
    # should match against the same query, should still match
    assert _exists_specified('true')


def test_exists_query_contains_with_exists_false():
    """
    this helps to test that if someone adds an exists statement, we don't add the exists
    exists(exists()) will always give a 'true' result
    :return:
    """
    # should match against the same query, should still match
    assert not _exists_specified('false')


def test_exists_query_starts_with_exists_handles_whitespace():
    """
    this helps to test that if someone adds an exists statement, we don't add the exists
    exists(exists()) will always give a 'true' result
    :return:
    """
    # should trim the whitespace and match appropiately
    assert _exists_whitespace()


@sqlexists()
def _exists_specified(key):
    return QueryData(EXISTS_QUERIES[key])


@sqlexists()
def _exists_true():
    return QueryData(TRUE_QUERY)


@sqlexists()
def _exists_false():
    return QueryData(FALSE_QUERY)


@sqlexists()
def _exists_true_params():
    return QueryData(TRUE_QUERY, TRUE_PARAMS)


@sqlexists()
def _exists_false_params():
    return QueryData(FALSE_QUERY, FALSE_PARAMS)


@sqlexists()
def _exists_whitespace():
    return QueryData("""
            """ + TRUE_QUERY)


def _check_query_and_return_result(query, params):
    """
    check that the query matches our expectations
    and then set the return value for the scalar call.
    scalar() returns either 1 or 0, 1 for true, 0 for false
    """
    assert query.text in list(EXISTS_QUERIES.values())
    scalar_mock = Mock()
    # default mock responses to true, then we only handle setting false responses
    scalar_mock.scalar.return_value = 1
    if query.text == EXISTS_QUERIES['true_params']:
        assert params.get('key') == 123
    if query.text == EXISTS_QUERIES['false_params']:
        assert params.get('key') == 456
        scalar_mock.scalar.return_value = 0
    elif query.text == EXISTS_QUERIES['false']:
        scalar_mock.scalar.return_value = 0
    return scalar_mock

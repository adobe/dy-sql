"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import pytest

import dysql
from dysql import QueryData, sqlupdate, QueryDataError
from dysql.test import (
    _verify_query,
    _verify_query_args,
    mock_create_engine_fixture,
    setup_mock_engine,
)


_ = mock_create_engine_fixture


@pytest.fixture(name="mock_engine", autouse=True)
def mock_engine_fixture(mock_create_engine):
    initial_id = 0

    def handle_execute(query=None, args=None):
        nonlocal initial_id
        if "INSERT INTO get_last(name)" in query.text:
            initial_id += 1
        if "SELECT LAST_INSERT_ID()" == query.text:
            return type("Result", (), {"scalar": lambda: initial_id})
        return []

    mock_engine = setup_mock_engine(mock_create_engine)
    execute_mock = mock_engine.connect().execution_options().execute
    execute_mock.side_effect = handle_execute
    return mock_engine


def test_insert_non_query_data_fails():
    @sqlupdate()
    def select_with_string():
        return "INSERT"

    with pytest.raises(QueryDataError):
        select_with_string()


def test_insert_single_column(mock_engine):
    insert_into_single_value(["Tom", "Jerry"])
    _verify_query(
        mock_engine,
        "INSERT INTO table(name) VALUES ( :values__name_col_0 ), ( :values__name_col_1 ) ",
    )


def test_insert_single_column_single_value(mock_engine):
    insert_into_single_value("Tom")
    _verify_query(
        mock_engine, "INSERT INTO table(name) VALUES ( :values__name_col_0 ) "
    )


def test_insert_single_value_empty():
    with pytest.raises(
        dysql.query_utils.ListTemplateException, match="['values_name_col']"
    ):
        insert_into_single_value([])


def test_insert_single_value_no_key():
    with pytest.raises(
        dysql.query_utils.ListTemplateException, match="['values_name_col']"
    ):
        insert_into_single_value(None)


def test_insert_multiple_values(mock_engine):
    insert_into_multiple_values(
        [
            {"name": "Tom", "email": "tom@adobe.com"},
            {"name": "Jerry", "email": "jerry@adobe.com"},
        ]
    )
    _verify_query(
        mock_engine,
        "INSERT INTO table(name, email) VALUES ( :values__users_0_0, :values__users_0_1 ), "
        "( :values__users_1_0, :values__users_1_1 ) ",
    )
    _verify_query_args(
        mock_engine,
        {
            "values__users_0_0": "Tom",
            "values__users_0_1": "tom@adobe.com",
            "values__users_1_0": "Jerry",
            "values__users_1_1": "jerry@adobe.com",
        },
    )


@pytest.mark.parametrize(
    "args",
    [
        ([("bob", "bob@email.com")]),
        ([("bob", "bob@email.com"), ("tom", "tom@email.com")]),
        None,
        (),
    ],
)
def test_insert_with_callback(args):
    def callback(items):
        assert items == args

    @sqlupdate(on_success=callback)
    def insert(items):
        yield QueryData(f"INSERT INTO table(name, email) {items}")

    insert(args)


def test_insert_with_callack_not_called(mock_engine):
    def callback():
        assert False

    @sqlupdate(on_success=callback)
    def insert():
        yield QueryData("INSERT INTO table(name, email)")

    mock_engine.connect().execution_options.return_value.execute.side_effect = (
        Exception()
    )
    with pytest.raises(Exception):
        insert()


@sqlupdate()
def insert_into_multiple_values(users):
    yield QueryData(
        "INSERT INTO table(name, email) {values__users}",
        template_params={"values__users": [(d["name"], d["email"]) for d in users]},
    )


@sqlupdate()
def insert_into_single_value(names):
    template_params = {}
    if names is not None:
        template_params = {"values__name_col": names}

    return QueryData(
        "INSERT INTO table(name) {values__name_col}", template_params=template_params
    )


def test_last_insert_id():
    @sqlupdate(use_get_last_insert_id=True)
    def insert(get_last_insert_id=None):
        yield QueryData("INSERT INTO get_last(name) VALUES ('Tom')")
        assert get_last_insert_id
        assert get_last_insert_id() == 1
        yield QueryData("INSERT INTO get_last(name) VALUES ('Jerry')")
        assert get_last_insert_id() == 2

    insert()


def test_last_insert_id_removed_before_callback():
    def callback(**kwargs):
        assert "get_last_insert_id" not in kwargs

    @sqlupdate(
        use_get_last_insert_id=True,
    )
    def insert(get_last_insert_id=None):
        assert get_last_insert_id
        yield QueryData("INSERT INTO get_last(name) VALUES ('Tom')")
        yield QueryData("INSERT INTO get_last(name) VALUES ('Jerry')")
        assert get_last_insert_id() == 2

    insert()

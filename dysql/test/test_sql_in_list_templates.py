"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

# pylint: disable=too-many-public-methods
import pytest

import dysql
from dysql import QueryData, sqlquery
from dysql.test import (
    _verify_query,
    _verify_query_args,
    _verify_query_params,
    mock_create_engine_fixture,
    setup_mock_engine,
)


_ = mock_create_engine_fixture


@pytest.fixture(name="mock_engine", autouse=True)
def mock_engine_fixture(mock_create_engine):
    mock_engine = setup_mock_engine(mock_create_engine)
    mock_engine.connect().execution_options().execute.side_effect = lambda x, y: []
    return mock_engine


def test_list_in_numbers(mock_engine):
    _query(
        "SELECT * FROM table WHERE {in__column_a}",
        template_params={"in__column_a": [1, 2, 3, 4]},
    )
    _verify_query_params(
        mock_engine,
        "SELECT * FROM table WHERE column_a IN ( :in__column_a_0, :in__column_a_1, :in__column_a_2, :in__column_a_3 ) ",
        {
            "in__column_a_0": 1,
            "in__column_a_1": 2,
            "in__column_a_2": 3,
            "in__column_a_3": 4,
        },
    )


def test_list_in__strings(mock_engine):
    _query(
        "SELECT * FROM table WHERE {in__column_a}",
        template_params={"in__column_a": ["a", "b", "c", "d"]},
    )
    _verify_query_params(
        mock_engine,
        "SELECT * FROM table WHERE column_a IN ( :in__column_a_0, :in__column_a_1, :in__column_a_2, :in__column_a_3 ) ",
        {
            "in__column_a_0": "a",
            "in__column_a_1": "b",
            "in__column_a_2": "c",
            "in__column_a_3": "d",
        },
    )


def test_list_not_in_numbers(mock_engine):
    _query(
        "SELECT * FROM table WHERE {not_in__column_b}",
        template_params={"not_in__column_b": [1, 2, 3, 4]},
    )
    _verify_query_params(
        mock_engine,
        "SELECT * FROM table WHERE column_b NOT IN ( :not_in__column_b_0, :not_in__column_b_1, "
        ":not_in__column_b_2, :not_in__column_b_3 ) ",
        {
            "not_in__column_b_0": 1,
            "not_in__column_b_1": 2,
            "not_in__column_b_2": 3,
            "not_in__column_b_3": 4,
        },
    )


def test_list_not_in_strings(mock_engine):
    _query(
        "SELECT * FROM table WHERE {not_in__column_b}",
        template_params={"not_in__column_b": ["a", "b", "c", "d"]},
    )
    _verify_query_params(
        mock_engine,
        "SELECT * FROM table WHERE column_b NOT IN ( :not_in__column_b_0, :not_in__column_b_1, "
        ":not_in__column_b_2, :not_in__column_b_3 ) ",
        {
            "not_in__column_b_0": "a",
            "not_in__column_b_1": "b",
            "not_in__column_b_2": "c",
            "not_in__column_b_3": "d",
        },
    )


def test_list_in_handles_empty(mock_engine):
    _query(
        "SELECT * FROM table WHERE {in__column_a}", template_params={"in__column_a": []}
    )
    _verify_query(mock_engine, "SELECT * FROM table WHERE 1 <> 1 ")


def test_list_in_handles_no_param():
    with pytest.raises(
        dysql.query_utils.ListTemplateException, match="['in__column_a']"
    ):
        _query("SELECT * FROM table WHERE {in__column_a}")


def test_list_in_multiple_lists(mock_engine):
    _query(
        "SELECT * FROM table WHERE {in__column_a} OR {in__column_b}",
        template_params={"in__column_a": ["first", "second"], "in__column_b": [1, 2]},
    )
    _verify_query(
        mock_engine,
        "SELECT * FROM table WHERE column_a IN ( :in__column_a_0, :in__column_a_1 ) "
        "OR column_b IN ( :in__column_b_0, :in__column_b_1 ) ",
    )


def test_list_in_multiple_lists_one_empty(mock_engine):
    _query(
        "SELECT * FROM table WHERE {in__column_a} OR {in__column_b}",
        template_params={"in__column_a": ["first", "second"], "in__column_b": []},
    )
    _verify_query(
        mock_engine,
        "SELECT * FROM table WHERE column_a IN ( :in__column_a_0, :in__column_a_1 ) OR 1 <> 1 ",
    )


def test_list_in_multiple_lists_one_missing():
    with pytest.raises(
        dysql.query_utils.ListTemplateException, match="['in__column_a']"
    ):
        _query(
            "SELECT * FROM table WHERE {in__column_a} OR {in__column_b} ",
            template_params={"in__column_b": [1, 2]},
        )


def test_list_in_multiple_lists_all_missing():
    with pytest.raises(
        dysql.query_utils.ListTemplateException, match="['in__column_a','in__column_b']"
    ):
        _query("SELECT * FROM table WHERE {in__column_a} OR {in__column_b} ")


def test_list_not_in_handles_empty(mock_engine):
    _query(
        "SELECT * FROM table WHERE {not_in__column_b}",
        template_params={"not_in__column_b": []},
    )
    _verify_query(mock_engine, "SELECT * FROM table WHERE 1 = 1 ")


def test_list_not_in_handles_no_param():
    with pytest.raises(
        dysql.query_utils.ListTemplateException, match="['not_in__column_b']"
    ):
        _query("SELECT * FROM table WHERE {not_in__column_b} ")


def test_list_gives_template_space_before(mock_engine):
    _query(
        "SELECT * FROM table WHERE{in__space}", template_params={"in__space": [9, 8]}
    )
    _verify_query(
        mock_engine,
        "SELECT * FROM table WHERE space IN ( :in__space_0, :in__space_1 ) ",
    )


def test_list_gives_template_space_after(mock_engine):
    _query(
        "SELECT * FROM table WHERE {in__space}AND other_condition = 1",
        template_params={"in__space": [9, 8]},
    )
    _verify_query(
        mock_engine,
        "SELECT * FROM table WHERE space IN ( :in__space_0, :in__space_1 ) AND other_condition = 1",
    )


def test_list_gives_template_space_before_and_after(mock_engine):
    _query(
        "SELECT * FROM table WHERE{in__space}AND other_condition = 1",
        template_params={"in__space": [9, 8]},
    )
    _verify_query(
        mock_engine,
        "SELECT * FROM table WHERE space IN ( :in__space_0, :in__space_1 ) AND other_condition = 1",
    )


def test_in_contains_whitespace(mock_engine):
    _query("{in__column_one}", template_params={"in__column_one": [1, 2]})
    _verify_query(
        mock_engine, " column_one IN ( :in__column_one_0, :in__column_one_1 ) "
    )


def test_template_handles_table_qualifier(mock_engine):
    """
    when the table is specified with a dot separator, we need to split the table out of the
    keyword and the keyword passed in should be passed in without the keyword

    {in__table.column} -> {table}.{in__column} -> table.column IN (:in__column_0, :in__column_1)
    :return:
    """
    _query(
        "SELECT * FROM table WHERE {in__table.column}",
        template_params={"in__table.column": [1, 2]},
    )
    _verify_query(
        mock_engine,
        "SELECT * FROM table WHERE table.column IN ( :in__table_column_0, :in__table_column_1 ) ",
    )
    _verify_query_args(mock_engine, {"in__table_column_0": 1, "in__table_column_1": 2})


def test_template_handles_multiple_table_qualifier(mock_engine):
    _query(
        "SELECT * FROM table WHERE {in__table.column} AND {not_in__other_column}",
        template_params={
            "in__table.column": [1, 2],
            "not_in__other_column": ["a", "b"],
        },
    )
    _verify_query(
        mock_engine,
        "SELECT * FROM table WHERE table.column IN ( :in__table_column_0, :in__table_column_1 ) "
        "AND other_column NOT IN ( :not_in__other_column_0, :not_in__other_column_1 ) ",
    )
    _verify_query_args(
        mock_engine,
        {
            "in__table_column_0": 1,
            "in__table_column_1": 2,
            "not_in__other_column_0": "a",
            "not_in__other_column_1": "b",
        },
    )


def test_empty_in_contains_whitespace(mock_engine):
    _query("{in__column_one}", template_params={"in__column_one": []})
    _verify_query(mock_engine, " 1 <> 1 ")


def test_multiple_templates_same_column_diff_table(mock_engine):
    template_params = {
        "in__table.status": ["on", "off", "waiting"],
        "in__other_table.status": ["success", "partial_success"],
    }
    expected_params_from_template = {
        "in__table_status_0": "on",
        "in__table_status_1": "off",
        "in__table_status_2": "waiting",
        "in__other_table_status_0": "success",
        "in__other_table_status_1": "partial_success",
    }

    # writing each of these queries out to help see what we expect compared to
    # the query we actually sent
    _query(
        "SELECT * FROM table WHERE {in__table.status} AND {in__other_table.status}",
        template_params=template_params,
    )
    expected_query = (
        "SELECT * FROM table WHERE table.status IN ( :in__table_status_0, :in__table_status_1, "
        ":in__table_status_2 ) AND other_table.status IN ( :in__other_table_status_0, "
        ":in__other_table_status_1 ) "
    )

    connection = mock_engine.connect.return_value.execution_options.return_value
    execute_call = connection.execute
    execute_call.assert_called_once()
    assert execute_call.call_args[0][0].text == expected_query
    assert execute_call.call_args[0][1] == expected_params_from_template


@sqlquery()
def _query(query, query_params=None, template_params=None):
    return QueryData(query, query_params=query_params, template_params=template_params)

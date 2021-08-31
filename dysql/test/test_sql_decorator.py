"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import pytest

from dysql import sqlupdate, sqlquery, DbMapResult, CountMapper, SingleRowMapper, \
    QueryData, QueryDataError
from dysql.test import mock_create_engine_fixture, setup_mock_engine


_ = mock_create_engine_fixture


class TestSqlSelectDecorator:
    """
    Test Sql Select Decorator
    """

    @staticmethod
    @pytest.fixture
    def mock_results():
        return [{
            'id': 1,
            'name': 'jack',
            'email': 'jack@adobe.com',
            'hobbies': ['golf', 'bikes', 'coding']
        }, {
            'id': 2,
            'name': 'flora',
            'email': 'flora@adobe.com',
            'hobbies': ['golf', 'coding']
        }, {
            'id': 3,
            'name': 'Terrence',
            'email': 'terrence@adobe.com',
            'hobbies': ['coding', 'watching tv', 'hot dog eating contest']
        }]

    @staticmethod
    @pytest.fixture
    def mock_engine(mock_results, mock_create_engine):
        mock_engine = setup_mock_engine(mock_create_engine)
        mock_engine.connect.return_value.execution_options.return_value.execute.return_value = mock_results
        return mock_engine

    @staticmethod
    def test_non_query_data_fails(mock_create_engine):
        # This argument forces the fixture setup
        _ = mock_create_engine

        @sqlquery()
        def select_with_string():
            return "SELECT"

        with pytest.raises(QueryDataError):
            select_with_string()

    def test_map_not_specified(self, mock_results, mock_engine):
        mock_engine.connect.return_value.execution_options.return_value.execute.return_value = mock_results
        assert isinstance(list(self._select_all())[0], DbMapResult)

    def test_single(self, mock_results, mock_engine):
        mock_engine.connect.return_value.execution_options.return_value.execute.return_value = mock_results
        result = self._select_single()
        assert isinstance(result, DbMapResult)

    def test_count(self, mock_engine):
        mock_engine.connect.return_value.execution_options.return_value.execute.return_value = [
            {'count': 2},
            {'count': 3},
        ]
        assert self._select_count() == 2

    def test_execute_no_params(self, mock_engine):
        self._select_all()
        mock_engine.connect.return_value.execution_options.return_value.execute.assert_called()
        call_args = mock_engine.connect.return_value.execution_options.return_value.execute.call_args
        assert call_args[0][0].text == "SELECT * FROM my_table"
        assert call_args[0][1] == {}

    def test_execute_params(self, mock_engine):
        self._select_filtered(3)
        mock_engine.connect.return_value.execution_options.return_value.execute.assert_called()

        call_args = mock_engine.connect.return_value.execution_options.return_value.execute.call_args
        assert call_args[0][0].text == "SELECT * FROM my_table WHERE id=:id"
        assert call_args[0][1] == {'id': 3}

    def test_list_results_map(self, mock_results, mock_engine):
        mock_engine.connect.return_value.execution_options.return_value.execute.return_value = [mock_results[2]]
        results = self._select_filtered(3)

        assert len(results) == 1
        assert len(list(results)[0].hobbies) == 3

    def test_isolation_default(self, mock_engine):
        mock_connect = mock_engine.connect.return_value.execution_options
        self._select_all()
        mock_connect.assert_called_with(isolation_level='READ_COMMITTED')

    def test_isolation_default_read_uncommited(self, mock_engine):
        mock_connect = mock_engine.connect.return_value.execution_options
        self._select_uncommitted()
        mock_connect.assert_called_with(isolation_level='READ_UNCOMMITTED')
        mock_connect.return_value.execute.assert_called()

    @staticmethod
    @sqlquery()
    def _select_all():
        """
        method is static just to help show you can use static methods if
        you don't want to instantiate an instance of the class
        """
        return QueryData("SELECT * FROM my_table")

    @staticmethod
    @sqlquery(mapper=SingleRowMapper())
    def _select_single():
        return QueryData("SELECT * FROM my_table")

    @staticmethod
    @sqlquery(mapper=CountMapper)
    def _select_count():
        return QueryData("SELECT COUNT(*) FROM my_table")

    @staticmethod
    @sqlquery()
    def _select_filtered(_id):
        return QueryData("SELECT * FROM my_table WHERE id=:id", query_params={'id': _id})

    @staticmethod
    @sqlquery(isolation_level='READ_UNCOMMITTED')
    def _select_uncommitted():
        return QueryData("SELECT * FROM uncommitted")


class TestSqlUpdateDecorator:
    """
    Test Sql Update Decorator
    """

    @staticmethod
    @pytest.fixture
    def mock_engine(mock_create_engine):
        return setup_mock_engine(mock_create_engine)

    @staticmethod
    @pytest.fixture
    def mock_connect(mock_engine):
        return mock_engine.connect.return_value.execution_options

    def test_isolation_default(self, mock_connect):
        self._update_something({'id': 1, 'value': 'test'})
        mock_connect.assert_called_with(isolation_level='READ_COMMITTED')

    def test_isolation_default_read_uncommited(self, mock_connect):
        self._update_something_uncommited_isolation({'id': 1, 'value': 'test'})
        mock_connect.assert_called_with(isolation_level='READ_UNCOMMITTED')
        mock_connect.return_value.begin.assert_called()
        mock_connect.return_value.begin.return_value.commit.assert_called()
        mock_connect.return_value.__exit__.assert_called()

    def test_transaction(self, mock_connect):
        self._update_something({'id': 1, 'value': 'test'})
        mock_connect().begin.assert_called()
        mock_connect().begin.return_value.commit.assert_called()
        mock_connect().__exit__.assert_called()

    def test_transaction_fails(self, mock_connect):
        mock_connect().execute.side_effect = Exception("error")
        with pytest.raises(Exception):
            self._update_something({'id': 1, 'value': 'test'})
        mock_connect().begin.return_value.commit.assert_not_called()
        mock_connect().begin.return_value.rollback.assert_called()
        mock_connect().__exit__.assert_called()

    def test_execute_passes_values(self, mock_engine):
        values = {'id': 1, 'value': 'test'}
        self._update_something(values)

        execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
        execute_call.assert_called()

        execute_call_args = execute_call.call_args[0]
        assert execute_call_args[0].text == "INSERT something"
        assert values == execute_call_args[1]

    def test_execute_query_values_none(self, mock_engine):
        self._update_something(None)

        execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
        execute_call.assert_called()

        self._expect_args_list(execute_call.call_args_list[0], "INSERT something")

    def test_execute_query_values_not_given(self, mock_engine):
        # pylint: disable=unused-argument
        self._update_something_no_params()

    def test_execute_multi_yield(self, mock_connect):
        expected_values = [
            {'id': 1, 'value': 'test 1'},
            {'id': 2, 'value': 'test 2'},
            {'id': 3, 'value': 'test 3'},
        ]
        self._update_something_multi_yield(expected_values)

        execute_call = mock_connect().execute
        assert execute_call.call_count == 3

        for call_args in execute_call.call_args_list:
            self._expect_args_list(call_args, "INSERT something")

    def test_execute_fails_list_if_multi_false(self):
        expected_values = [
            {'id': 1, 'value': 'test 1'},
            {'id': 2, 'value': 'test 2'},
            {'id': 3, 'value': 'test 3'},
        ]
        with pytest.raises(Exception):
            self._update_list_when_multi_false(expected_values)

    def test_execute_multi_yield_and_lists(self, mock_engine):
        expected_values = [
            {'id': 1, 'value': 'test 1'},
            {'id': 2, 'value': 'test 2'},
            {'id': 3, 'value': 'test 3'},
        ]
        other_expected_values = [
            {'id': 5, 'value': 'test 5'},
            {'id': 6, 'value': 'test 6'},
            {'id': 7, 'value': 'test 7'},
        ]
        self._update_yield_with_lists(expected_values, other_expected_values)

        execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
        assert execute_call.call_count == 2

        self._expect_args_list(
            execute_call.call_args_list[0],
            "INSERT some VALUES ( :values__in_0_0, :values__in_0_1 ), ( :values__in_1_0, :values__in_1_1 ), "
            "( :values__in_2_0, :values__in_2_1 ) "
        )
        self._expect_args_list(
            execute_call.call_args_list[1],
            "INSERT some more VALUES ( :values__other_0_0, :values__other_0_1 ), "
            "( :values__other_1_0, :values__other_1_1 ), ( :values__other_2_0, :values__other_2_1 ) "
        )

    def test_execute_multi_yield_and_lists_some_no_params(self, mock_engine):
        expected_values = [
            {'id': 1, 'value': 'test 1'},
            {'id': 2, 'value': 'test 2'},
            {'id': 3, 'value': 'test 3'},
        ]
        self._update_yield_with_lists_some_no_params(expected_values)

        execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
        assert execute_call.call_count == 4

        self._expect_args_list(
            execute_call.call_args_list[0],
            "INSERT some VALUES ( :values__in_0_0, :values__in_0_1 ), ( :values__in_1_0, :values__in_1_1 ), "
            "( :values__in_2_0, :values__in_2_1 ) "
        )
        self._expect_args_list(execute_call.call_args_list[1], "UPDATE some more")
        self._expect_args_list(execute_call.call_args_list[2], "UPDATE some more")
        self._expect_args_list(execute_call.call_args_list[3], "DELETE some more")

    def test_set_foreign_key_checks_default(self, mock_engine):
        self._update_something_no_params()

        execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
        assert execute_call.call_count == 1

    def test_set_foreign_key_checks_true(self, mock_engine):
        self._update_without_foreign_key_checks()

        execute_call = mock_engine.connect.return_value.execution_options.return_value.execute
        assert execute_call.call_count == 4
        assert execute_call.call_args_list[0][0][0].text == "SET FOREIGN_KEY_CHECKS=0"
        assert execute_call.call_args_list[-1][0][0].text == "SET FOREIGN_KEY_CHECKS=1"

    @staticmethod
    def _expect_args_list(call_args, expected_query):
        assert expected_query == call_args[0][0].text

    @staticmethod
    @sqlupdate(disable_foreign_key_checks=True)
    def _update_without_foreign_key_checks():
        yield QueryData("INSERT B_RELIES_ON_A")
        yield QueryData("INSERT A")

    @staticmethod
    @sqlupdate()
    def _update_something(values):
        return QueryData("INSERT something", query_params=values)

    @staticmethod
    @sqlupdate(isolation_level='READ_UNCOMMITTED')
    def _update_something_uncommited_isolation(values):
        return QueryData(f"INSERT WITH uncommitted {values}")

    @staticmethod
    @sqlupdate()
    def _update_something_no_params():
        return QueryData("INSERT something")

    @staticmethod
    @sqlupdate()
    def _update_something_multi_yield(multiple_values):
        for values in multiple_values:
            yield QueryData("INSERT something", query_params=values)

    @staticmethod
    @sqlupdate()
    def _update_something_return_list(multiple_values):
        return QueryData("INSERT something", query_params=multiple_values)

    @staticmethod
    @sqlupdate()
    def _update_list_when_multi_false(multiple_values):
        return "INSERT something", multiple_values

    @staticmethod
    @sqlupdate()
    def _update_yield_with_lists(multiple_values, other_values):
        """
        method is static just to help show you can use static methods if
        you don't want to instantiate an instance of the class

        NOTE:
        """
        yield QueryData(
            "INSERT some {values__in}",
            template_params=_get_template_params('values__in',multiple_values)
        )
        yield QueryData(
            "INSERT some more {values__other}",
            template_params=_get_template_params('values__other',other_values)
        )

    @staticmethod
    @sqlupdate()
    def _update_yield_with_lists_some_no_params(multiple_values):
        """
        method is static just to help show you can use static methods if
        you don't want to instantiate an instance of the class
        """
        yield QueryData(
            "INSERT some {values__in}",
            template_params=_get_template_params('values__in', multiple_values)
        )
        yield QueryData("UPDATE some more")
        yield QueryData("UPDATE some more")
        yield QueryData("DELETE some more")


def _get_template_params(key, values):
    '''
    template parameters is handled here as an example of what you might end up
    doing. usually data coming in is going to be a list

    :param key: the template key we want to use to build our template with
    :param values: the list of objects or mariadbmaps,
    :return: a keyed list of tuples or mariadbmaps
    '''
    if isinstance(values[0], DbMapResult):
        return { key : values }
    return { key: [ (v['id'],v['value']) for v in values]}

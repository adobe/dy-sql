"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import pytest

from dysql.query_utils import Templates, ListTemplateException


class TestSqlTemplates:
    """
    Test we get templates back from Templates
    """
    number_values = [1, 2, 3, 4]
    string_values = ['1', '2', '3', '4']
    insert_values = [('ironman', 1), ('batman', 2)]
    query = 'column_a IN ( :column_a_0, :column_a_1, :column_a_2, :column_a_3 )'
    query_with_table = \
        'table.column_a IN ( :table_column_a_0, :table_column_a_1, :table_column_a_2, :table_column_a_3 )'
    not_query = 'column_a NOT IN ( :column_a_0, :column_a_1, :column_a_2, :column_a_3 )'
    not_query_with_table = \
        'table.column_a NOT IN ( :table_column_a_0, :table_column_a_1, :table_column_a_2, :table_column_a_3 )'

    test_query_data = ('template_function, column_name,column_values,expected_query', [
        (Templates.in_column, 'column_a', number_values, query),
        (Templates.in_column, 'table.column_a', number_values, query_with_table),
        (Templates.in_column, 'column_a', string_values, query),
        (Templates.in_column, 'table.column_a', string_values, query_with_table),
        (Templates.in_column, 'column_a', [], '1 <> 1'),
        (Templates.not_in_column, 'column_a', number_values, not_query),
        (Templates.not_in_column, 'table.column_a', number_values, not_query_with_table),
        (Templates.not_in_column, 'column_a', string_values, not_query),
        (Templates.not_in_column, 'table.column_a', string_values, not_query_with_table),
        (Templates.not_in_column, 'column_a', [], '1 = 1'),
        (Templates.values, 'someid', insert_values, "VALUES ( :someid_0_0, :someid_0_1 ), ( :someid_1_0, :someid_1_1 )")
    ])

    parameter_numbers = {
        'column_a_0': number_values[0],
        'column_a_1': number_values[1],
        'column_a_2': number_values[2],
        'column_a_3': number_values[3]
    }
    with_table_parameter_numbers = {
        'table_column_a_0': number_values[0],
        'table_column_a_1': number_values[1],
        'table_column_a_2': number_values[2],
        'table_column_a_3': number_values[3]
    }
    parameter_strings = {
        'column_a_0': string_values[0],
        'column_a_1': string_values[1],
        'column_a_2': string_values[2],
        'column_a_3': string_values[3]
    }
    with_table_parameter_strings = {
        'table_column_a_0': string_values[0],
        'table_column_a_1': string_values[1],
        'table_column_a_2': string_values[2],
        'table_column_a_3': string_values[3]
    }
    test_params_data = ('template_function, column_name,column_values,expected_params', [
        (Templates.in_column, 'column_a', number_values, parameter_numbers),
        (Templates.in_column, 'table.column_a', number_values, with_table_parameter_numbers),
        (Templates.in_column, 'column_a', string_values, parameter_strings),
        (Templates.in_column, 'table.column_a', string_values, with_table_parameter_strings),
        (Templates.in_column, 'column_a', [], None),
        (Templates.not_in_column, 'column_a', number_values, parameter_numbers),
        (Templates.not_in_column, 'table.column_a', number_values, with_table_parameter_numbers),
        (Templates.not_in_column, 'column_a', string_values, parameter_strings),
        (Templates.not_in_column, 'table.column_a', string_values, with_table_parameter_strings),
        (Templates.not_in_column, 'column_a', [], None),
        (Templates.values, 'someid', insert_values, {
            'someid_0_0': insert_values[0][0],
            'someid_0_1': insert_values[0][1],
            'someid_1_0': insert_values[1][0],
            'someid_1_1': insert_values[1][1],

        })
    ])

    @staticmethod
    @pytest.mark.parametrize(*test_query_data)
    def test_query(template_function, column_name, column_values, expected_query):
        query, _ = template_function(column_name, column_values)
        assert query == expected_query

    @staticmethod
    @pytest.mark.parametrize(*test_params_data)
    def test_params(template_function, column_name, column_values, expected_params):
        _, params = template_function(column_name, column_values)
        assert params == expected_params

    @staticmethod
    def test_insert_none():
        with pytest.raises(ListTemplateException):
            Templates.values('someid', None)

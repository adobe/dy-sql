"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import re

from typing import Iterable, Optional, Tuple, Union


# group 0 - entire string
# group 1 - empty space before template. this helps us ensure we add space to a template but only 1
# group 2 - template keyword
# group 3 - table_name
# group 4 - column_name
# group 5 - empty space after template. this helps us ensure we add space to a template after but
#   only 1
LIST_TEMPLATE_REGEX = re.compile(r'(( +)?{(in|not_in|values)__([A-Za-z_]+\.)?([A-Za-z_]+)}( +)?)')


class TemplateGenerators:
    """
    Functions in this class help to return a tuple which contains the query template along with
    a dictionary of parameterized values for the query.
    """

    @classmethod
    def get_template(cls, name: str):
        if name == 'in':
            return cls.in_column
        if name == 'not_in':
            return cls.not_in_column
        if name == 'values':
            return cls.values
        return None

    @staticmethod
    def in_column(
            name: str,
            values: Union[str, Iterable[str]],
            legacy_key: str = None,
            is_multi_column: bool = False
    ) -> Tuple[str, Optional[dict]]:
        """
        Returns query and params for using "IN" SQL queries.
        :param name: the field name
        :param values: the field values
        :param legacy_key:
        :param is_multi_column: needed when more than one field is being compared as part of the in statement
        :return: a tuple of the query string and the params dictionary
        """
        if not values:
            return '1 <> 1', None
        key_name = TemplateGenerators._get_key(name, legacy_key, is_multi_column)
        keys, values = TemplateGenerators._parameterize_list(key_name, values)
        if is_multi_column:
            keys = f'({keys})'
        return f'{name} IN {keys}', values

    @staticmethod
    def in_multi_column(
            name: str,
            values: Union[str, Iterable[str]],
            legacy_key: str = None
    ):
        """
        A wrapper for in_column with is_multi_column set to true
        """
        return TemplateGenerators.in_column(name, values, legacy_key, True)

    @staticmethod
    def not_in_column(
            name: str,
            values: Union[str, Iterable[str]],
            legacy_key: str = None,
            is_multi_column: bool = False
    ) -> Tuple[str, Optional[dict]]:
        """
        Returns query and params for using "NOT IN" SQL queries.
        :param name: the field name
        :param values: the field values
        :param legacy_key:
        :param is_multi_column: needed when more than one field is being compared as part of the not in statement
        :return: a tuple of the query string and the params dictionary
        """
        if not values:
            return '1 = 1', None
        key_name = TemplateGenerators._get_key(name, legacy_key, is_multi_column)
        keys, values = TemplateGenerators._parameterize_list(key_name, values)
        if is_multi_column:
            keys = f'({keys})'
        return f'{name} NOT IN {keys}', values

    @staticmethod
    def not_in_multi_column(
            name: str,
            values: Union[str, Iterable[str]],
            legacy_key: str = None
    ):

        """
        A wrapper for not_in_column with is_multi_column set to true
        """
        return TemplateGenerators.not_in_column(name, values, legacy_key, True)

    @staticmethod
    def values(
            name: str,
            values: Union[str, Iterable[str]],
            legacy_key: str = None,
    ) -> Tuple[str, Optional[dict]]:
        """
        Returns query and params for using "VALUES" SQL queries.
        :param name: the field name
        :param values: the values
        :param legacy_key:
        :return: a tuple of the query string and the params dictionary
        """
        if not values:
            raise ListTemplateException(f'Must have values for {name} template')
        key_name = TemplateGenerators._get_key(name, legacy_key, False)
        keys, values = TemplateGenerators._parameterize_list(key_name, values)
        return f'VALUES {keys}', values

    @staticmethod
    def _get_key(key: str, legacy_key: str, is_multi_column: bool) -> str:
        key_name = key
        if legacy_key:
            key_name = legacy_key
        if is_multi_column:
            key_name = re.sub('[, ()]', '', key_name)
        return key_name

    @staticmethod
    def _parameterize_inner_list(key: str, values: Union[str, Iterable[str]]) -> Tuple[str, Optional[dict]]:
        param_values = {}
        parameterized_keys = []
        if not isinstance(values, (list, tuple)):
            param_values[key] = values
            parameterized_keys.append(key)
        else:
            for index, value in enumerate(values):
                parameterized_key = f"{key.replace('.', '_')}_{str(index)}"
                param_values[parameterized_key] = value
                parameterized_keys.append(parameterized_key)

        return f"( :{', :'.join(parameterized_keys)} )", param_values

    @staticmethod
    def _parameterize_list(key: str, values: Union[str, Iterable[str]]) -> Tuple[str, Optional[dict]]:
        """
        Build a string with parameterized values and a dictionary
        with key value pairs matching the string parameters.
        :return a tuple of the parameter string and the dictionary of parameter values
        """
        param_values = {}
        param_inner_keys = []

        if isinstance(values, str):
            values = tuple((values,))

        for index, value in enumerate(values):
            if isinstance(value, tuple) or key.startswith('values'):
                param_string, inner_param_values = TemplateGenerators._parameterize_inner_list(
                    f'{key}_{str(index)}', value
                )
                param_values.update(inner_param_values)
                param_inner_keys.append(param_string)
            else:
                return TemplateGenerators._parameterize_inner_list(key, values)

        return ', '.join(param_inner_keys), param_values


class ListTemplateException(Exception):
    """
    List Template Exception
    """


class QueryDataError(Exception):
    """
    This exception is thrown when we expect a QueryData object but end up with something different
    """


class QueryData:
    # pylint: disable=too-few-public-methods
    """
    Query data is a wrapper class that allows us to pass back information for a query with more
    information and specification. this helps to have template queries with template parameters but
    to still have the ability to have query parameters that get parameterized on the query.
    """

    def __init__(
            self,
            query: str,
            query_params: dict = None,
            template_params: dict = None,
    ):
        """
        Constructor.
        :param query: the SQL query
        :param query_params: object or list of objects containing values to apply to parameters

            examples of a queryparams passed to a QueryData

                Single Object
                QueryData(
                    "SELECT * FROM table WHERE value_a=:value_a and value_b=:value_b",
                    query_params={'value_a': 1, 'value_b': 'b' }
                )

                List of Objects (Note: this will create a query for each object, it may not be possible or
                    even desirable but, using template_params will create a single query with the params)
                QueryData(
                    "SELECT * FROM table WHERE value_a=:value_a` and value_b=:value_b",
                    query_params=[{'value_a': 1, 'value_b': 'b' }, {'value_a': 1, 'value_b': 'b' }]
                )
        :param template_params: these are templates that can be added to queries to generate a single
            parameterized query.

            examples of current list templates and transformations

                "IN (... )"
                QueryData(
                    "SELECT * FROM table where name {in__name}",
                    template_params={'in__name' : ['bob','tom','chic']}
                )
                {in__actor.name} ->  actor.name IN ('bob', 'tom', ' ),

                "NOT IN (... )"
                QueryData(
                    "SELECT * FROM table wher name {in__name}",
                    template_params={'in__name' : ['bob','tom','chic']}
                )
                {not_in__actor.name} ->  actor.name NOT IN ('name1','name2',...),

                "VALUES (), (), ()..."
                {values__actors} -> VALUES (a1.v1,a1.v2,a1.v3), (a1.v1,a1.v2,a1.v3),...
        """
        self.query = query
        self.query_params = query_params
        self.template_params = template_params


def __validate_keys_clean_query(query, template_params):
    validated_keys = []
    for groups in re.findall(LIST_TEMPLATE_REGEX, query):
        # check first group for the full key
        key = f'{groups[2]}__{groups[3] if groups[3] else ""}{groups[4]}'
        validated_keys.append(key)
        missing_keys = []

        #validate
        if template_params is None or template_params.get(key) is None:
            missing_keys.append(key)
        elif key == 'values' and len(template_params.get(key)) == 0:
            missing_keys.append(key)

        if len(missing_keys) > 0:
            raise ListTemplateException(f'Missing template keys {missing_keys}')

        # Clean whitespace as templates will add their own padding later on
        query = query.replace(groups[0], groups[0].strip())
    return query, validated_keys


def __validate_query_and_params(data: QueryData) -> None:
    if not isinstance(data, QueryData):
        raise QueryDataError('SQL annotated methods must return an instance of QueryData for query information')


def get_query_data(data: QueryData) -> Tuple[str, dict]:
    """
    Retrieves the query and parameters from a QueryData object.
    :param data: the query data object
    :return: a tuple of the query string and the params
    """
    __validate_query_and_params(data)

    params = {}
    query, validated_keys = __validate_keys_clean_query(data.query, data.template_params)

    if data.query_params:
        params.update(data.query_params)

    for key in validated_keys:
        list_template_key, column_name = tuple(key.split('__'))
        template_to_use = TemplateGenerators.get_template(list_template_key)
        template_query, param_dict = template_to_use(column_name, data.template_params[key], legacy_key=key)
        if param_dict:
            params.update(param_dict)
        query_key = '{' + key + '}'
        query = query.replace(query_key, f' {template_query} ')

    return query, params

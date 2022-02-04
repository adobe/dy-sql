"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
from typing import Any, Dict, List, Set

from dysql import (
    RecordCombiningMapper,
    SingleRowMapper,
)
from dysql.pydantic_mappers import DbMapResultModel


class ConversionDbModel(DbMapResultModel):
    id: int
    field_str: str
    field_int: int
    field_bool: bool


class CombiningDbModel(DbMapResultModel):
    _list_fields: Set[str] = {'list1'}
    _set_fields: Set[str] = {'set1'}
    _dict_key_fields: Dict[str, str] = {'key1': 'dict1', 'key2': 'dict2'}
    _dict_value_mappings: Dict[str, str] = {'dict1': 'val1', 'dict2': 'val2'}

    id: int = None
    list1: List[str]
    set1: Set[str] = set()
    dict1: Dict[str, Any] = {}
    dict2: Dict[str, int] = {}


class DefaultListCombiningDbModel(CombiningDbModel):
    list1: List[str] = []


class ListWithStringsModel(DbMapResultModel):
    _list_as_string_fields: Set[str] = {'list1', 'list2'}

    id: int
    list1: List[str]
    list2: List[int]


def _unwrap_results(results):
    return [r.raw() for r in results]


def test_field_conversion():
    mapper = SingleRowMapper(record_mapper=ConversionDbModel)
    assert mapper.map_records([
        {'id': 1, 'field_str': 'str1', 'field_int': 1, 'field_bool': 1},
    ]).raw() == {'id': 1, 'field_str': 'str1', 'field_int': 1, 'field_bool': True}


def test_complex_object_record_combining():
    mapper = RecordCombiningMapper(record_mapper=CombiningDbModel)
    assert len(mapper.map_records([])) == 0
    assert _unwrap_results(mapper.map_records([
        {'id': 1, 'list1': 'val1', 'set1': 'val2', 'key1': 'k1', 'val1': 'v1', 'key2': 'k3', 'val2': 3},
        {'id': 2, 'list1': 'val1'},
        {'id': 1, 'list1': 'val3', 'set1': 'val4', 'key1': 'k2', 'val1': 'v2', 'key2': 'k4', 'val2': 4},
    ])) == [
        {
            'id': 1,
            'list1': ['val1', 'val3'],
            'set1': {'val2', 'val4'},
            'dict1': {'k1': 'v1', 'k2': 'v2'},
            'dict2': {'k3': 3, 'k4': 4},
        },
        {
            'id': 2,
            'list1': ['val1'],
            'set1': set(),
            'dict1': {},
            'dict2': {},
        },
    ]


def test_complex_object_with_null_values():
    mapper = SingleRowMapper(record_mapper=DefaultListCombiningDbModel)
    assert mapper.map_records([
        {'id': 1},
    ]).raw() == {
        'id': 1,
        'list1': [],
        'set1': set(),
        'dict1': {},
        'dict2': {},
    }


def test_lists_from_strings():
    mapper = SingleRowMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records([{
        'id': 1,
        'list1': 'a,b,c,d',
        'list2': '1,2,3,4'
    }]).raw() == {
        'id': 1,
        'list1': ['a', 'b', 'c', 'd'],
        'list2': [1, 2, 3, 4]
    }


def test_lists_from_strings_multiple_records_first_result_only():
    mapper = RecordCombiningMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records([{
        'id': 1,
        'list1': 'a,b',
        'list2': '1,2'
    }, {
        'id': 1,
        'list1': 'c,d',
        'list2': '3,4'
    }])[0].raw() == {
               'id': 1,
               'list1': ['a', 'b'],
               'list2': [1, 2]
           }

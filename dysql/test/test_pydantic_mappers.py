"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

import json
from typing import Any, Dict, List, Set, Optional
import pytest

from pydantic import ValidationError

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
    _list_fields: Set[str] = {"list1"}
    _set_fields: Set[str] = {"set1"}
    _dict_key_fields: Dict[str, str] = {"key1": "dict1", "key2": "dict2"}
    _dict_value_mappings: Dict[str, str] = {"dict1": "val1", "dict2": "val2"}

    id: int = None
    list1: List[str]
    set1: Set[str] = set()
    dict1: Dict[str, Any] = {}
    dict2: Dict[str, int] = {}


class DefaultListCombiningDbModel(CombiningDbModel):
    list1: List[str] = []


class ListWithStringsModel(DbMapResultModel):
    _csv_list_fields: Set[str] = {"list1", "list2"}

    id: int
    list1: Optional[List[str]] = None
    list2: List[int] = []  # help test empty list gets filled


class JsonModel(DbMapResultModel):
    _json_fields: Set[str] = {"json1", "json2"}

    id: int
    json1: dict
    json2: Optional[dict] = None


class MultiKeyModel(DbMapResultModel):
    @classmethod
    def get_key_columns(cls):
        return ["a", "b"]

    _list_fields = {"c"}
    a: int
    b: str
    c: List[str]


def _unwrap_results(results):
    return [r.raw() for r in results]


def test_field_conversion():
    mapper = SingleRowMapper(record_mapper=ConversionDbModel)
    assert mapper.map_records(
        [
            {"id": 1, "field_str": "str1", "field_int": 1, "field_bool": 1},
        ]
    ).raw() == {"id": 1, "field_str": "str1", "field_int": 1, "field_bool": True}


def test_complex_object_record_combining():
    mapper = RecordCombiningMapper(record_mapper=CombiningDbModel)
    assert len(mapper.map_records([])) == 0
    assert _unwrap_results(
        mapper.map_records(
            [
                {
                    "id": 1,
                    "list1": "val1",
                    "set1": "val2",
                    "key1": "k1",
                    "val1": "v1",
                    "key2": "k3",
                    "val2": 3,
                },
                {"id": 2, "list1": "val1"},
                {
                    "id": 1,
                    "list1": "val3",
                    "set1": "val4",
                    "key1": "k2",
                    "val1": "v2",
                    "key2": "k4",
                    "val2": 4,
                },
            ]
        )
    ) == [
        {
            "id": 1,
            "list1": ["val1", "val3"],
            "set1": {"val2", "val4"},
            "dict1": {"k1": "v1", "k2": "v2"},
            "dict2": {"k3": 3, "k4": 4},
        },
        {
            "id": 2,
            "list1": ["val1"],
            "set1": set(),
            "dict1": {},
            "dict2": {},
        },
    ]


def test_record_combining_multi_column_id():
    mapper = RecordCombiningMapper(MultiKeyModel)
    assert len(mapper.map_records([])) == 0
    assert _unwrap_results(
        mapper.map_records(
            [
                {"a": 1, "b": "test", "c": "one"},
                {"a": 1, "b": "test", "c": "two"},
                {"a": 1, "b": "test", "c": "three"},
                {"a": 2, "b": "test", "c": "one"},
                {"a": 2, "b": "test", "c": "two"},
                {"a": 1, "b": "othertest", "c": "one"},
            ]
        )
    ) == [
        {"a": 1, "b": "test", "c": ["one", "two", "three"]},
        {"a": 2, "b": "test", "c": ["one", "two"]},
        {"a": 1, "b": "othertest", "c": ["one"]},
    ]


def test_complex_object_with_null_values():
    mapper = SingleRowMapper(record_mapper=DefaultListCombiningDbModel)
    assert mapper.map_records(
        [
            {"id": 1},
        ]
    ).raw() == {
        "id": 1,
        "list1": [],
        "set1": set(),
        "dict1": {},
        "dict2": {},
    }


def test_csv_list_field():
    mapper = SingleRowMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records(
        [{"id": 1, "list1": "a,b,c,d", "list2": "1,2,3,4"}]
    ).raw() == {"id": 1, "list1": ["a", "b", "c", "d"], "list2": [1, 2, 3, 4]}


def test_csv_list_field_single_value():
    mapper = SingleRowMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records([{"id": 1, "list1": "a", "list2": "1"}]).raw() == {
        "id": 1,
        "list1": ["a"],
        "list2": [1],
    }


def test_csv_list_field_empty_string():
    mapper = SingleRowMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records([{"id": 1, "list1": "", "list2": ""}]).raw() == {
        "id": 1,
        "list1": None,
        "list2": [],
    }


def test_csv_list_field_extends():
    mapper = RecordCombiningMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records(
        [
            {"id": 1, "list1": "a,b", "list2": "1,2"},
            {"id": 1, "list1": "c,d", "list2": "3,4"},
        ]
    )[0].raw() == {"id": 1, "list1": ["a", "b", "c", "d"], "list2": [1, 2, 3, 4]}


def test_csv_list_field_multiple_records_duplicates():
    mapper = RecordCombiningMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records(
        [
            {"id": 1, "list1": "a,b,c,d", "list2": "1,2,3,4"},
            {"id": 1, "list1": "a,b,c,d", "list2": "1,2,3,4"},
        ]
    )[0].raw() == {
        "id": 1,
        "list1": ["a", "b", "c", "d", "a", "b", "c", "d"],
        "list2": [1, 2, 3, 4, 1, 2, 3, 4],
    }


def test_csv_list_field_none_in_first_record():
    mapper = RecordCombiningMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records(
        [
            {"id": 1, "list1": "a,b,c,d", "list2": None},
            {"id": 1, "list1": "e,f,g", "list2": "1,2,3,4"},
        ]
    )[0].raw() == {
        "id": 1,
        "list1": ["a", "b", "c", "d", "e", "f", "g"],
        "list2": [1, 2, 3, 4],
    }


def test_csv_list_field_without_mapping_ignored():
    mapper = SingleRowMapper(record_mapper=ListWithStringsModel)
    assert mapper.map_records(
        [{"id": 1, "list1": "a,b, c,d", "list2": "1,2,3,4", "list3": "x,y,z"}]
    ).raw() == {"id": 1, "list1": ["a", "b", "c", "d"], "list2": [1, 2, 3, 4]}


def test_csv_list_field_invalid_type():
    mapper = RecordCombiningMapper(record_mapper=ListWithStringsModel)
    with pytest.raises(ValidationError, match="1 validation error for list"):
        mapper.map_records(
            [
                {"id": 1, "list1": "a,b", "list2": "1,2"},
                {"id": 1, "list1": "c,d", "list2": "3,a"},
            ]
        )


def test_json_field():
    mapper = SingleRowMapper(record_mapper=JsonModel)
    assert mapper.map_records(
        [
            {
                "id": 1,
                "json1": json.dumps(
                    {"a": 1, "b": 2, "c": {"x": 10, "y": 9, "z": {"deep": "value"}}}
                ),
            }
        ]
    ).model_dump() == {
        "id": 1,
        "json1": {"a": 1, "b": 2, "c": {"x": 10, "y": 9, "z": {"deep": "value"}}},
        "json2": None,
    }


@pytest.mark.parametrize(
    "json1, json2",
    [
        ('{ "json": value', None),
        ('{ "json": value', '{ "json": value }'),
        ('{ "json": value }', '{ "json": value'),
        (None, '{ "json": value'),
    ],
)
def test_invalid_json(json1, json2):
    with pytest.raises(ValidationError, match="Invalid JSON"):
        mapper = SingleRowMapper(record_mapper=JsonModel)
        mapper.map_records([{"id": 1, "json1": json1, "json2": json2}])


def test_json_none():
    mapper = SingleRowMapper(record_mapper=JsonModel)
    assert mapper.map_records(
        [{"id": 1, "json1": '{ "first": "value" }', "json2": None}]
    ).model_dump() == {
        "id": 1,
        "json1": {
            "first": "value",
        },
        "json2": None,
    }

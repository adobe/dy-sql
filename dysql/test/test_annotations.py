"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
from typing import List, Union

import pytest
from pydantic import BaseModel, ValidationError

from dysql.annotations import FromCSVToList


class StrCSVModel(BaseModel):
    values: FromCSVToList[List[str]]


class IntCSVModel(BaseModel):
    values: FromCSVToList[List[int]]


class NullableStrCSVModel(BaseModel):
    values: FromCSVToList[Union[List[str], None]]


class NullableIntCSVModel(BaseModel):
    values: FromCSVToList[Union[List[int], None]]


@pytest.mark.parametrize('cls, values, expected', [
    (StrCSVModel, '1,2,3', ['1', '2', '3']),
    (StrCSVModel, 'a,b', ['a', 'b']),
    (StrCSVModel, 'a', ['a']),
    (NullableStrCSVModel, '', None),
    (NullableStrCSVModel, None, None),
    (StrCSVModel, ['a', 'b'], ['a', 'b']),
    (StrCSVModel, ['a', 'b', 'c'], ['a', 'b', 'c']),
    (IntCSVModel, '1,2,3', [1, 2, 3]),
    (IntCSVModel, '1', [1]),
    (NullableIntCSVModel, '', None),
    (NullableIntCSVModel, None, None),
    (IntCSVModel, ['1', '2', '3'], [1, 2, 3]),
    (IntCSVModel, ['1', '2', '3', 4, 5], [1, 2, 3, 4, 5])
])
def test_from_csv_to_list(cls, values, expected):
    assert expected == cls(values=values).values


@pytest.mark.parametrize('cls, values', [
    (StrCSVModel, ''),
    (StrCSVModel, None),
    (IntCSVModel, 'a,b,c'),
    (IntCSVModel, ''),
    (IntCSVModel, None),
    (IntCSVModel, ['a', 'b', 'c']),
])
def test_from_csv_to_list_invalid(cls, values):
    with pytest.raises(ValidationError):
        cls(values=values)

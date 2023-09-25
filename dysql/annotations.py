"""
Copyright 2023 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

from typing import TypeVar, Annotated

from pydantic import BeforeValidator

# pylint: disable=invalid-name
T = TypeVar('T')


def _transform_csv(value: str) -> T:
    if not value:
        return None

    if isinstance(value, str):
        return list(map(str.strip, value.split(',')))

    if isinstance(value, list):
        return value
    # if we don't have a string or type T we aren't going to be able to transform it
    return [value]


# Annotation that helps transform a CSV string into a list of type T
FromCSVToList = Annotated[T, BeforeValidator(_transform_csv)]

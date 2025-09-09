"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

import pytest

from dysql import (
    RecordCombiningMapper,
    SingleRowMapper,
    SingleColumnMapper,
    SingleRowAndColumnMapper,
    CountMapper,
    KeyValueMapper,
)
from dysql.mappers import MapperError


class TestMappers:
    """
    Test Mappers
    """

    @staticmethod
    def _unwrap_results(results):
        return [r.raw() for r in results]

    def test_record_combining(self):
        mapper = RecordCombiningMapper()
        assert len(mapper.map_records([])) == 0
        assert self._unwrap_results(mapper.map_records([{"a": 1, "b": 2}])) == [
            {"a": 1, "b": 2}
        ]
        assert self._unwrap_results(
            mapper.map_records(
                [
                    {"id": 1, "a": 1, "b": 2},
                    {"id": 2, "a": 1, "b": 2},
                    {"id": 1, "c": 3},
                ]
            )
        ) == [
            {"id": 1, "a": 1, "b": 2, "c": 3},
            {"id": 2, "a": 1, "b": 2},
        ]

    @staticmethod
    def test_record_combining_no_record_mapper():
        mapper = RecordCombiningMapper(record_mapper=None)
        assert len(mapper.map_records([])) == 0
        assert mapper.map_records([{"a": 1, "b": 2}]) == [{"a": 1, "b": 2}]
        assert mapper.map_records(
            [
                {"id": 1, "a": 1, "b": 2},
                {"id": 2, "a": 1, "b": 2},
                {"id": 1, "c": 3},
            ]
        ) == [
            {"id": 1, "a": 1, "b": 2},
            {"id": 2, "a": 1, "b": 2},
            {"id": 1, "c": 3},
        ]

    @staticmethod
    def test_single_row():
        mapper = SingleRowMapper()
        assert mapper.map_records([]) is None
        assert mapper.map_records([{"a": 1, "b": 2}]).raw() == {"a": 1, "b": 2}
        assert mapper.map_records(
            [
                {"id": 1, "a": 1, "b": 2},
                {"id": 2, "a": 1, "b": 2},
                {"id": 1, "c": 3},
            ]
        ).raw() == {"id": 1, "a": 1, "b": 2}

    @staticmethod
    def test_single_row_no_record_mapper():
        mapper = SingleRowMapper(record_mapper=None)
        assert mapper.map_records([]) is None
        assert mapper.map_records([{"a": 1, "b": 2}]) == {"a": 1, "b": 2}
        assert mapper.map_records(
            [
                {"id": 1, "a": 1, "b": 2},
                {"id": 2, "a": 1, "b": 2},
                {"id": 1, "c": 3},
            ]
        ) == {"id": 1, "a": 1, "b": 2}

    @staticmethod
    def test_single_column():
        mapper = SingleColumnMapper()
        assert len(mapper.map_records([])) == 0
        assert mapper.map_records([{"a": 1, "b": 2}]) == [1]
        assert mapper.map_records(
            [
                {"id": "myid1", "a": 1, "b": 2},
                {"id": "myid2", "a": 1, "b": 2},
                {"id": "myid3", "c": 3},
            ]
        ) == ["myid1", "myid2", "myid3"]

    @staticmethod
    @pytest.mark.parametrize(
        "mapper",
        [
            SingleRowAndColumnMapper(),
            # Alias for the other one
            CountMapper(),
        ],
    )
    def test_single_column_and_row(mapper):
        assert mapper.map_records([]) is None
        assert mapper.map_records([{"a": 1, "b": 2}]) == 1
        assert (
            mapper.map_records(
                [
                    {"id": "myid", "a": 1, "b": 2},
                    {"id": 2, "a": 1, "b": 2},
                    {"id": 1, "c": 3},
                ]
            )
            == "myid"
        )

    @staticmethod
    @pytest.mark.parametrize(
        "mapper, expected",
        [
            (KeyValueMapper(), {"a": 4, "b": 7}),
            (KeyValueMapper(key_column="column_named_something"), {"a": 4, "b": 7}),
            (KeyValueMapper(value_column="column_with_some_value"), {"a": 4, "b": 7}),
            (
                KeyValueMapper(has_multiple_values_per_key=True),
                {"a": [1, 2, 3, 4], "b": [3, 4, 5, 6, 7]},
            ),
            (
                KeyValueMapper(
                    key_column="column_named_something",
                    has_multiple_values_per_key=True,
                ),
                {"a": [1, 2, 3, 4], "b": [3, 4, 5, 6, 7]},
            ),
            (
                KeyValueMapper(
                    key_column="column_with_some_value",
                    value_column="column_named_something",
                    has_multiple_values_per_key=True,
                ),
                {
                    1: ["a"],
                    2: ["a"],
                    3: ["a", "b"],
                    4: ["a", "b"],
                    5: ["b"],
                    6: ["b"],
                    7: ["b"],
                },
            ),
            (
                KeyValueMapper(
                    key_column="column_with_some_value",
                    value_column="column_named_something",
                ),
                {1: "a", 2: "a", 3: "b", 4: "b", 5: "b", 6: "b", 7: "b"},
            ),
        ],
    )
    def test_key_mapper_key_has_multiple(mapper, expected):
        result = mapper.map_records(
            [
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["a", 1]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["a", 2]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["a", 3]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["a", 4]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["b", 3]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["b", 4]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["b", 5]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["b", 6]
                ),
                HelperRow(
                    ("column_named_something", "column_with_some_value"), ["b", 7]
                ),
            ]
        )
        assert len(result) == len(expected)
        assert result == expected

    @staticmethod
    def test_key_mapper_key_value_same():
        with pytest.raises(
            MapperError, match="key and value columns cannot be the same"
        ):
            KeyValueMapper(key_column="same", value_column="same")


class HelperRow:  # pylint: disable=too-few-public-methods
    """
    Helper class does the most basic functionality we see when accessing records passed in
    """

    def __init__(self, fields, values):
        self.fields = fields
        self.values = values

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.values[key]
        return self.values[self.fields.index(key)]

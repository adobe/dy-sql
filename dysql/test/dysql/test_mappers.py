
import pytest

from dysql import (
    RecordCombiningMapper,
    SingleRowMapper,
    SingleColumnMapper,
    SingleRowAndColumnMapper,
    CountMapper
)


class TestMappers:
    """
    Test Mappers
    """

    @staticmethod
    def _unwrap_results(results):
        return list([r.raw() for r in results])

    def test_record_combining(self):
        mapper = RecordCombiningMapper()
        assert mapper.map_records([]) == []
        assert self._unwrap_results(mapper.map_records([{'a': 1, 'b': 2}])) == [{'a': 1, 'b': 2}]
        assert self._unwrap_results(mapper.map_records([
            {'id': 1, 'a': 1, 'b': 2},
            {'id': 2, 'a': 1, 'b': 2},
            {'id': 1, 'c': 3},
        ])) == [
            {'id': 1, 'a': 1, 'b': 2, 'c': 3},
            {'id': 2, 'a': 1, 'b': 2},
        ]

    @staticmethod
    def test_record_combining_no_record_mapper():
        mapper = RecordCombiningMapper(record_mapper=None)
        assert mapper.map_records([]) == []
        assert mapper.map_records([{'a': 1, 'b': 2}]) == [{'a': 1, 'b': 2}]
        assert mapper.map_records([
            {'id': 1, 'a': 1, 'b': 2},
            {'id': 2, 'a': 1, 'b': 2},
            {'id': 1, 'c': 3},
        ]) == [
            {'id': 1, 'a': 1, 'b': 2},
            {'id': 2, 'a': 1, 'b': 2},
            {'id': 1, 'c': 3},
        ]

    @staticmethod
    def test_single_row():
        mapper = SingleRowMapper()
        assert mapper.map_records([]) is None
        assert mapper.map_records([{'a': 1, 'b': 2}]).raw() == {'a': 1, 'b': 2}
        assert mapper.map_records([
            {'id': 1, 'a': 1, 'b': 2},
            {'id': 2, 'a': 1, 'b': 2},
            {'id': 1, 'c': 3},
        ]).raw() == {'id': 1, 'a': 1, 'b': 2}

    @staticmethod
    def test_single_row_no_record_mapper():
        mapper = SingleRowMapper(record_mapper=None)
        assert mapper.map_records([]) is None
        assert mapper.map_records([{'a': 1, 'b': 2}]) == {'a': 1, 'b': 2}
        assert mapper.map_records([
            {'id': 1, 'a': 1, 'b': 2},
            {'id': 2, 'a': 1, 'b': 2},
            {'id': 1, 'c': 3},
        ]) == {'id': 1, 'a': 1, 'b': 2}

    @staticmethod
    def test_single_column():
        mapper = SingleColumnMapper()
        assert mapper.map_records([]) == []
        assert mapper.map_records([{'a': 1, 'b': 2}]) == [1]
        assert mapper.map_records([
            {'id': 'myid1', 'a': 1, 'b': 2},
            {'id': 'myid2', 'a': 1, 'b': 2},
            {'id': 'myid3', 'c': 3},
        ]) == ['myid1', 'myid2', 'myid3']

    @staticmethod
    @pytest.mark.parametrize('_cls', [
        SingleRowAndColumnMapper,
        # Alias for the other one
        CountMapper,
    ])
    def test_single_column_and_row(_cls):
        mapper = _cls()
        assert mapper.map_records([]) is None
        assert mapper.map_records([{'a': 1, 'b': 2}]) == 1
        assert mapper.map_records([
            {'id': 'myid', 'a': 1, 'b': 2},
            {'id': 2, 'a': 1, 'b': 2},
            {'id': 1, 'c': 3},
        ]) == 'myid'

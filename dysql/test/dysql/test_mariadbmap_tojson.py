import json
import pytest

from dysql import DbMapResult


@pytest.mark.parametrize("name, expected_json, mariadb_map", [
    (
        'basic',
        '{"id": 1, "name": "test"}',
        DbMapResult(id=1, name='test'),
    ),
    (
        'basic_with_list',
        '{"id": 1, "name": "test", "my_list": ["a", "b", "c"]}',
        DbMapResult(id=1, name='test', my_list=['a', 'b', 'c']),
    ),
    (
        'inner_map',
        '{"id": 1, "inner_map": {"id": 2, "name": "inner_test"}}',
        DbMapResult(id=1, inner_map=DbMapResult(id=2, name='inner_test')),
    ),
    (
        'inner_list_of_maps',
        '{"id": 1, "inner_map_list": [{"id": 2, "name": "inner_test_2"}, {"id": 3, "name": "inner_test_3"}]}',
        DbMapResult(id=1, inner_map_list=[
            DbMapResult(id=2, name='inner_test_2'), DbMapResult(id=3, name='inner_test_3')
        ])
    ),
])
def test_raw_json_format(name, expected_json, mariadb_map):
    assert json.dumps(mariadb_map.raw()) == expected_json, 'error with ' + name

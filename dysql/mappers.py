"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
# pylint: disable=too-few-public-methods

import abc
import logging
from collections import OrderedDict, defaultdict
from typing import Any, Optional, Type

import sqlalchemy


LOGGER = logging.getLogger(__name__)

class MapperError(Exception):
    pass


class DbMapResultBase(abc.ABC):
    @classmethod
    def create_instance(cls, *args, **kwargs) -> 'DbMapResultBase':
        """
        Called instead of constructor, used to support different ways of
        creating objects instead of constructors (if desired).
        :return: an instance of the class
        """
        return cls(*args, **kwargs)

    @abc.abstractmethod
    def map_record(self, record: sqlalchemy.engine.Row) -> None:
        """
        Implements logic to handle the mapping of individual record objects from DB records.
        :param record: a record row from the database
        """

    @abc.abstractmethod
    def raw(self) -> dict:
        """
        Retrieves the raw data stored in this object as a dictionary.
        :return: the dict representing the object data
        """

    @abc.abstractmethod
    def has(self, field: str) -> bool:
        """
        Returns true if the field exists in the object.
        :param field: the field to check
        :return: True if the field exists, false otherwise
        """

    @abc.abstractmethod
    def get(self, field: str, default: Any = None) -> Any:
        """
        Retrieves the value for the field, returning a default value if the field is not available.
        :param field: the field to retrieve
        :param default: the default value to return if the field is not available (defaults to None)
        :return: the value of the field or the default value
        """


class DbMapResult(DbMapResultBase):
    def __init__(self, **kwargs):
        self.__dict__ = kwargs
        # pylint: disable=invalid-name
        if not self.__dict__.get('id'):
            self.id = None

    def __getitem__(self, field: str) -> Any:
        return self.__dict__.get(field)

    def __setitem__(self, field: str, value: Any) -> None:
        self.__dict__[field] = value

    def __str__(self) -> str:
        return str(self.__dict__)

    def map_record(self, record: sqlalchemy.engine.Row) -> None:
        for column, value in record.items():
            self[column] = value

    def raw(self) -> dict:
        def get_raw(value: Any) -> dict:
            if isinstance(value, DbMapResultBase):
                return value.raw()
            return value

        raw = {}
        for key, value in self.__dict__.items():
            if isinstance(value, list):
                raw[key] = list(map(get_raw, value))
            else:
                raw[key] = get_raw(value)
        # Remove the id field if it was never set
        if raw.get('id') is None:
            del raw['id']
        return raw

    def has(self, field: str) -> bool:
        return self.__dict__.get(field) is not None

    def get(self, field: str, default: Any = None) -> Any:
        return self.__dict__.get(field, default)


class BaseMapper(metaclass=abc.ABCMeta):
    """
    Extend this class and implement the map_records method to map the results from a database query.
    """
    @abc.abstractmethod
    def map_records(self, records: sqlalchemy.engine.CursorResult) -> Any:
        pass


class RecordCombiningMapper(BaseMapper):
    """
    Creates one or more mapped results from multiple records. This returns the same or less number
    of results as there are records since it potentially combines multiple records with the same
    identifier into a single result.

    This depends on ``id`` being a column present in the records. If not, it will return a mapped
    result for every row without combining records. Otherwise, a single result is returned for each
    unique ``id`` value.
    """

    def __init__(self, record_mapper: Optional[Type[DbMapResultBase]] = DbMapResult):
        self.record_mapper = record_mapper

    def map_records(self, records: sqlalchemy.engine.CursorResult) -> Any:
        if not self.record_mapper:
            return records

        current_results = OrderedDict()
        current_num = 0
        for record in records:
            if 'id' in record:
                record_id = record['id']
                if current_results.get(record_id) is None:
                    current_results[record_id] = self.record_mapper.create_instance()
                current_results[record_id].map_record(record)
            else:
                current_results[current_num] = self.record_mapper.create_instance()
                current_results[current_num].map_record(record)
                current_num += 1

        return list(v for k, v in current_results.items())


class SingleRowMapper(BaseMapper):
    """
    Returns a single mapped result from one or more records. The first record is returned even if
    there are multiple records from the database.
    """
    def __init__(self, record_mapper: Optional[Type[DbMapResultBase]] = DbMapResult):
        self.record_mapper = record_mapper

    def map_records(self, records: sqlalchemy.engine.CursorResult) -> Any:
        if not self.record_mapper:
            for record in records:
                return record
            return None

        for record in records:
            mapper = self.record_mapper.create_instance()
            mapper.map_record(record)
            return mapper
        return None


class SingleColumnMapper(BaseMapper):
    """
    Returns the first column value for each record from the database, even if multiple columns are
    defined. This will return a list of scalar values.
    """
    def map_records(self, records: sqlalchemy.engine.CursorResult) -> Any:
        results = []
        for record in records:
            for value in record.values():
                results.append(value)
                break
        return results


class SingleRowAndColumnMapper(BaseMapper):
    """
    Returns the first column in the first record from the database, even if multiple records or
    columns exist. This will return a single scalar or None if there are no records.
    """
    def map_records(self, records: sqlalchemy.engine.CursorResult) -> Any:
        for record in records:
            for value in record.values():
                return value
        return None


class CountMapper(SingleRowAndColumnMapper):
    """
    Alias for SingleRowAndColumnMapper, may be used to easily return the result of a count query.
    """


class KeyValueMapper(BaseMapper):
    """
    :param key_column can be specified to determine what field to map to as the key for are keyed values
    :param value_column can be specified to help decide what column to add as a value
    :param has_multiple_values_per_key is False by default but when True it tells the keyvalue mapper
        the each key may have more than 1 result. this will returns a dictionary of lists when set

    """
    def __init__(self, key_column=0, value_column=1, has_multiple_values_per_key=False):
        if key_column == value_column:
            raise MapperError('key and value columns cannot be the same')

        self.key_column = key_column
        self.value_column = value_column
        self.has_multiple_values_per_key = has_multiple_values_per_key

    def map_records(self, records: sqlalchemy.engine.CursorResult) -> Any:
        results = {}
        if self.has_multiple_values_per_key:
            results = defaultdict(list)
        for record in records:
            key = record[self.key_column]
            value = record[self.value_column]
            if self.has_multiple_values_per_key:
                results[key].append(value)
            else:
                results[key] = value
        return results

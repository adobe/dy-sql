"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

from typing import Any, Dict, Set

import sqlalchemy
from pydantic import BaseModel  # pylint: disable=no-name-in-module

from .mappers import DbMapResultBase


class DbMapResultModel(BaseModel, DbMapResultBase):
    """
    Pydantic model that allows for results to be mapped into annotated fields without explicitly
    mapping each field. The metadata fields starting with "_" are special fields that allow lists, sets,
    and dictionaries to be mapped correctly if more than one record is mapped to an instance.

    Note that validation *does* occur the very first time map_record is called, but not on subsequent runs. Therefore
    if you desire better validation for list, set, or dict fields, that must most likely be done outside of pydantic.

    Additionally, lists, sets, and dicts will ignore null values from the database. Therefore you must provide default
    values for these fields when used or else validation will fail.
    """

    # List fields (type does not matter)
    _list_fields: Set[str] = set()
    # Set fields (type does not matter)
    _set_fields: Set[str] = set()
    # Dictionary key fields as DB field name => model field name
    _dict_key_fields: Dict[str, str] = {}
    # Dictionary value fields as model field name => DB field name (this is reversed from _dict_key_fields!)
    _dict_value_mappings: Dict[str, str] = {}

    @classmethod
    def create_instance(cls, *args, **kwargs) -> 'DbMapResultModel':
        # Uses the construct method to prevent validation when mapping results
        return cls.construct(*args, **kwargs)

    def _map_list(self, current_dict: dict, record: sqlalchemy.engine.Row, field: str):
        if record[field] is None:
            # Do not map null entries into lists, this may cause problems in the future but it works
            # around some other issues when fields are nullable
            return
        if self._has_been_mapped():
            current_dict[field].append(record[field])
        else:
            current_dict[field] = [record[field]]

    def _map_set(self, current_dict: dict, record: sqlalchemy.engine.Row, field: str):
        if record[field] is None:
            # See note above for lists
            return
        if self._has_been_mapped():
            current_dict[field].add(record[field])
        else:
            current_dict[field] = {record[field]}

    def _map_dict(self, current_dict: dict, record: sqlalchemy.engine.Row, field: str):
        model_field_name = self._dict_key_fields[field]
        value_field = self._dict_value_mappings[model_field_name]
        if record[value_field] is None:
            # See note above for lists
            return
        if self._has_been_mapped():
            current_dict[model_field_name][record[field]] = record[value_field]
        else:
            current_dict[model_field_name] = {record[field]: record[value_field]}

    def map_record(self, record: sqlalchemy.engine.Row) -> None:
        """
        Implementation of map_record that handles the special "_" prefixed fields listed at the top of this class.
        The following rules are used:
          - If it is in the _list_fields and is not none, append it to the field specified
          - If it is in the _set_fields and is not none, add it to the field specified
          - If it is in the _dict_key_fields and is not none, add it to the field specified along with the value
            from the _dict_value_mappings
          - Else add it to the fields to bind to this model
          - Remove all DB fields that are present in _dict_value_mappings since they were likely added above
        :param record: the DB record
        """
        current_dict: dict = self.__dict__
        for field in record.keys():
            if field in self._list_fields:
                self._map_list(current_dict, record, field)
            elif field in self._set_fields:
                self._map_set(current_dict, record, field)
            elif field in self._dict_key_fields:
                self._map_dict(current_dict, record, field)
            else:
                # Ignore fields that should have already been set
                if not self._has_been_mapped():
                    current_dict[field] = record[field]

        # Remove all dict value fields (if present)
        for db_field in self._dict_value_mappings.values():
            current_dict.pop(db_field, None)

        if self._has_been_mapped():
            # At this point, just update the previous record
            self.__dict__.update(current_dict)
        else:
            # Init takes care of validation and assigning values to each field with conversions in place, etc
            self.__init__(**current_dict)

    def raw(self) -> dict:
        return self.dict()

    def has(self, field: str) -> bool:
        return field in self.__dict__

    def _has_been_mapped(self):
        """
        Tells if a record has already been mapped onto this class or not.
        :return: True if map_record has already been called, False otherwise
        """
        return bool(getattr(self, '__fields_set__', False))

    def get(self, field: str, default: Any = None) -> Any:
        return self.__dict__.get(field, default)

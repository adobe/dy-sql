######################
 Dynamic SQL (dy-sql)
######################

This project consists of a set of python decorators that eases integration with SQL databases.
These decorators may trigger queries, inserts, updates, and deletes.

The decorators are a way to help us map our data in python to SQL queries and vice versa.
When we select, insert, update, or delete the queries, we pass the data we want
to insert along with a well defined query.

This is designed to be done with minimal setup and coding. You need to specify
the database connection parameters and annotate any SQL queries/updates you have with the
decorator that fits your needs.

Installation
============

.. code-block::

    pip install dy-sql

Component Breakdown
===================
* **set_default_connection_parameters** - this function needs to be used to set the database parameters on
  initialization so that when a decorator function is called, it can setup a connection pool to a correct database
* **is_set_current_database_supported** - this function may be used to determine if the ``*_current_database`` methods
  may be used or not
* **set_current_database** - (only supported on Python 3.7+) this function may be used to set the database name for the
  current async context (not thread), this is especially useful for multitenant applications
* **reset_current_database** - (only supported on Python 3.7+) helper method to reset the current database after
  ``set_current_database`` has been used in an async context
* **set_database_init_hook** - sets a method to call whenever a new database is initialized
* **QueryData** - a class that may be returned or yielded from ``sql*`` decorated methods which
  contains query information
* **DbMapResult** - base class that can be used when selecting data that helps to map the results of a
  query to an object in python
* **DbMapResultModel** - pydantic version of ``DbMapResult`` that allows easy mapping to pydantic models
* **@sqlquery** - decorator for select queries that can return a SQL result in a ``DbMapResult``
* **@sqlupdate** - decorator for any queries that can change data in the database, this can take a set of
  values and yield multiple operations back for insertions or updates inside of a transaction
* **@sqlexists** - decorator for a simplified select query that will return true if a record exists and false otherwise
* **XDbTestManager** - test manager classes that may be used for testing purposes

Database Preparation
====================
In order to initialize a connection pool for the ``sql*`` decorators, the database needs to first be set up
using the ``set_default_connection_parameters`` method.

.. code-block:: python

    from dysql import set_database_parameters

    def set_database_from_config():
        maria_db_config = {...}
        set_database_parameters(
            maria_db_host,
            maria_db_user,
            maria_db_password,
            maria_db_database,
            port=maria_db_port,
            charset=maria_db_charset
        )

Note: the keyword arguments are not required and have standard default values,
the port for example defaults to 3306

Database Init Hook
==================
At times, it is necessary to perform post-initialization tasks on the database engine after it has been created.
The ``set_database_init_hook`` method may be used in this case. As an example, to instrument the engine using
``opentelemetry-instrumentation-sqlalchemy``, the following code may be used:

.. code-block:: python

    from typing import Optional
    # Used for type-hints only
    import sqlalchemy
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from dysql import set_database_init_hook

    def _instrument_engine(database_name: Optional[str], engine: sqlalchemy.engine.Engine) -> None:
        # The database name is unused in this case
        _ = database_name
        SQLAlchemyInstrumentor().instrument(engine=engine)

    set_database_init_hook(_instrument_engine)


Multitenancy
============
In some applications, it may be useful to set a database other than the default database in order to support
database-per-tenant configurations. This may be done using the ``set_current_database`` and ``reset_current_database``
methods.

.. code-block:: python

    from dysql import reset_current_database, set_current_database

    def use_database_for_query():
        set_database_parameters(
            ...
            'db1',
        )
        set_current_database('db2')
        try:
            # Queries db2 and not db1
            query_database()
        finally:
            reset_current_database()

.. warning::
    These methods are only supported in Python 3.7+ due to their use of the ``contextvars`` module. The
    ``is_set_current_database_supported`` method is provided to help tell if these methods may be used.

Decorators
==========
Decorators are an easy way for us to tell a function to be a 'query' and return
a result without having to have a big chunk of boiler plate code. Once the
database has been prepared, calling a ``sql*`` decorated function will initialize
the database, parse the value returned in your function, make a corresponding
parameterized query and return the results.

The basic structure is to decorate a method that returns information about the query.
There are multiple options for returning a query, below is a summary of some of the possibilities:

* return a ``QueryData`` object that possibly contains ``query_params`` and/or ``template_params``
* (not available for all ``sql*`` decorators) yield one or more ``QueryData`` objects,
  each containing ``query_params`` and/or ``template_params``

DbMapResult
~~~~~~~~~~~
This class is used in the default mapper (see below) for any ``sqlquery`` decorated method. This class may also be
overridden as shown below. The default class wraps and returns the results of a query for easy access to the data
from the query. For example, if you use the query ``SELECT id, name FROM table``, it would return a list of
``DbMapResult`` objects where each contains the ``id`` and ``name`` fields. You could then easily loop through
and access the properties as shown in the following example:

.. code-block:: python

    @sqlquery()
    def get_items_from_sql_query():
        return QueryData("SELECT id, name FROM table")

    def get_and_process_items():
        for item in get_items_from_sql_query():
            # we are able to access properties on the object
            print('{name} goes with {id}'.format(item.name, item.id))

We can inherit from ``DbMapResult`` and override the way our data maps into the
object. This is primarily helpful in cases where we end up with multiple rows
such as a query for a 1-to-many relationship.

.. code-block:: python

    class ExampleMap(DbMapResult):
        def map_result(self, result):
            # we know we are mapping multiple rows to a single result
            if self.id is None:
                # in our case we know the id is the same so we only set it the first time
                self.id = result['id']
                # initialize our array
                self.item_names = []

        # we know that every result for a given id has a unique item_name
        self.item_names.append(result['item_name'])

    @sqlquery(mapping=ExampleMap)
    def get_table_items()
        return QueryData("""
            SELECT id, name, item_name FROM table
                JOIN table_item ON table.id = table_item.table_id
                JOIN item ON item.id = table_item.item_id
        """)

    def print_item_names()
        for table_item in get_table_items():
            for item_name in table_item.item_names:
                print(f'table name {table_item.name} has item {item_name}')

DbMapResultModel (pydantic)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If pydantic models are desired to be used, there is a record mapper available. Note that pydantic must be installed,
which is available as an extra package:

.. code-block::

    pip install dy-sql[pydantic]

This model attempts to make mapping records easier, but there are shortcomings of it in more complex cases.
Most fields will "just work" as defined by the type annotations.

.. code-block:: python

    from dysql.pydantic_mappers import DbMapResultModel

    class PydanticDbModel(DbMapResultModel):
        id: int
        field_str: str
        field_int: int
        field_bool: bool

Mapping a record onto this class will automatically convert types as defined by the type annotations. No ``map_record``
method needs to be defined since the pydantic model has everything necessary to map database fields.

Lists, sets, dicts, csv strings, and json strings (when using the RecordCombiningMapper) require additional configuration on the model class.

.. code-block:: python

    from dysql.pydantic_mappers import DbMapResultModel

    class ComplexDbModel(DbMapResultModel):
        # if any data has been aggregated or saved into a string as a comma delimited list, this will convert to a list
        # NOTE this only does simple splitting and is not fully rfc4180 compatible
        _csv_list_fields: Set[str] = {'list_from_string'}
        # List fields (type does not matter)
        _list_fields: Set[str] = {'list1'}
        # Set fields (type does not matter)
        _set_fields: Set[str] = {'set1'}
        # Dictionary key fields as DB field name => model field name
        _dict_key_fields: Dict[str, str] = {'key1': 'dict1', 'key2': 'dict2'}
        # Dictionary value fields as model field name => DB field name (this is reversed from _dict_key_fields!)
        _dict_value_mappings: Dict[str, str] = {'dict1': 'val1', 'dict2': 'val2'}
        # JSON string fields. Type can be any dictionary type but for larger json objects its safe to stay with `dict`
        _json_fields: Set[str] = {'json1', 'json2'}

        id: int = None
        list_from_string: List[str]
        list1: List[str]
        set1: Set[str] = set()
        dict1: Dict[str, Any] = {}
        dict2: Dict[str, int] = {}
        json1: dict
        json2: dict

.. note::

    csv strings can be useful in queries where you want to group by an id and then ``group_concat`` some field

    json strings are a handy way to extract json blobs into a python dictionary for ease of use without manually processing
    each field everytime you need something.

In this case, the ``_`` prefixed properties tell the model which fields should be treated differently when combining
multiple rows into a single object. For an example of how this works with database rows, see the
``test_pydantic_mappers.py`` file in the source repository.

Note that validation **does** occur the very first time ``map_record`` is called, but not on subsequent runs. Therefore
if you desire better validation for list, set, or dict fields, this must most likely be done outside of dysql/pydantic.
Additionally, lists, sets, and dicts will ignore null values from the database. Therefore you must provide default
values for these fields when used or else validation will fail.

@sqlquery
~~~~~~~~~
This is for making SQL ``select`` calls. An optional mapper may be specified to
change the behavior of what is returned from a decorated method. The default
mapper can combine multiple records into a single result if there is an
``id`` field present in each record. Mappers available:

* ``RecordCombiningMapper`` (default) - Returns a list of results where multiple records that can be combined with the
  same unique identifer. An optional ``record_mapper`` value may be passed to the constructor to change
  how records are mapped to result. By default the ``record_mapper`` used is ``DbMapResult``. The base identifier
  is the column ``id`` but an array of columns can be used to create a unique key lookup for combining records.

.. note::
    The ``_key_columns`` field of the ``DbMapResultModel`` is an array containing only the ``id`` but can
    be overriden in derived classes. For example, setting  ``_key_columns = [ 'a', 'b' ]`` in your derived class
    would make it so you class would use the values of columns `a` and `b` in order to uniquely identify
    records when being combined.

* ``SingleRowMapper`` - returns an object for the first record from the database (even if multiple records are
  returned). An optional ``record_mapper`` value may be passed to the constructor to change how this first record is
  mapped to the result.
* ``SingleColumnMapper`` - Returns a list of scalars with the first column from every record, even if multiple columns
  are returned from the database.
* ``SingleRowAndColumnMapper`` - Returns a single scalar value even if multiple records and columns are returned
  from the database.
* ``CountMapper`` - alias for ``SingleRowAndColumnMapper`` to make it clear that it may be used for ``count`` queries.
* ``KeyValueMapper`` - returns a dictionary mapping 1 column to the keys and 1 column to the values.
  By default the key is mapped to the first column and value is mapped to the second column. You can override the key_column
  and value_columns by specifying the name of the columns you want for each. You can also pass in a has_multiple_values
  which defaults to False. Doing so will allow you to get a dictionary of lists based on the keys and values you specify.
* Custom mappers may be made by extending the ``BaseMapper`` class and implementing the ``map_records`` method.

basic query with conditions hardcoded into query and default mapper

.. code-block:: python

    def get_items():
        items = select_items_for_joe()
        # ... work on items

    @sqlquery()
    def select_items_for_joe()
        return QueryData("SELECT * FROM table WHERE name='joe'")

basic query with params passed as a dict

.. code-block:: python

    def get_items():
        items = select_items_for_name('joe')
        # ... work on items, which contains all records matching the name

    @sqlquery()
    def select_items_for_name(name)
        return QueryData("SELECT * FROM table WHERE name=:name", query_params={'name': name})

query that only returns a single result from the first row

.. code-block:: python

    def get_joe_id():
        result = get_item_for_name('joe')
        return result.get('id')

    # Either an instance or class may be used as the mapper parameter
    @sqlquery(mapper=SingleRowMapper())
    def get_item_for_name(name)
        return QueryData("SELECT id, name FROM table WHERE name=:name", query_params={'name': name})

alternative to the above query that returns the id directly

.. code-block:: python

    def get_joe_id():
        return get_id_for_name('joe')

    @sqlquery(mapper=SingleRowAndColumnMapper)
    def get_id_for_name(name)
        return QueryData("SELECT id FROM table WHERE name=:name", query_params={'name': name})

query that returns a list of scalar values containing the list of distinct names available

.. code-block:: python

    def get_unique_names():
        return get_names_from_items()

    @sqlquery(mapper=SingleColumnMapper)
    def get_names_from_items()
        return QueryData("SELECT DISTINCT(name) FROM table")

basic count query that only returns the scalar value returned for the count

.. code-block:: python

    def get_count_for_joe():
        return get_count_for_name('joe')

    @sqlquery(mapper=CountMapper)
    def get_count_for_name(name):
        return QueryData("SELECT COUNT(*) FROM table WHERE name=:name", query_params={'name': name})


basic query returning dictionary

.. code-block:: python

    @sqlquery(mapper=KeyValueMapper())
    def get_status_by_name():
        return QueryData("SELECT name, status FROM table")

query returning a dictionary where we are specifying the keys. Note that the columns are returning in a different order

.. code-block:: python

    @sqlquery(mapper=KeyValueMapper(key_column='name', value_column='status'))
    def get_status_by_name():
        return QueryData("SELECT status, name FROM table")

query returning a dictionary where there are multiple results under each key. Note that here we are essentially grouping under status

.. code-block:: python

    @sqlquery(mapper=KeyValueMapper(key_column='status', value_column='name', has_multiple_values=True))
    def get_status_by_name():
        return QueryData("SELECT status, name FROM table")


@sqlupdate
~~~~~~~~~~
Handles any SQL that is not a select. This is primarily, but not limited to, ``insert``, ``update``, and ``delete``.


.. code-block:: python

    @sqlupdate()
    def insert_items(item_dict):
        return QueryData("INSERT INTO", template_params={'in__item_id':item_id_list})

You can yield multiple QueryData objects. This is done in a transaction and it can be helpful for data integrity or just
a nice clean way to run a set of updates.

.. code-block:: python

    @sqlupdate()
    def insert_items(item_dict):
        insert_values_1, insert_params_1 = TemplateGenerator.values('table1values', _get_values_for_1_from_items(item_dict))
        insert_values_2, insert_params_2 = TemplateGenerator.values('table2values', _get_values_for_2_from_items(item_dict))
        yield QueryData(f'INSERT INTO table_1 {insert_values_1}', query_params=insert_values_params_1)
        yield QueryData(f'INSERT INTO table_2 {insert_values_2}', query_params=insert_values_params_2)

if needed you can assign a callback to be ran after a query or set of queries completes successfully

.. code-block:: python

    @sqlupdate(on_success=_handle_insert_success)
    def insert_items_with_callback(item_dict):
        insert_values_1, insert_params_1 = TemplateGenerator.values('table1values', _get_values_for_1_from_items(item_dict))
        insert_values_2, insert_params_2 = TemplateGenerator.values('table2values', _get_values_for_2_from_items(item_dict))
        yield QueryData(f'INSERT INTO table_1 {insert_values_1}', query_params=insert_values_params_1)
        yield QueryData(f'INSERT INTO table_2 {insert_values_2}', query_params=insert_values_params_2)

    def _handle_insert_success(item_dict):
        #  callback logic here happens after the transaction is complete

@sqlexists
~~~~~~~~~~
This wraps a SQL query to determine if a row exists or not. If at least one row is returned from the query, it will
return True, otherwise False. The query you give here can return anything you want but as good practice,
try to always select as little as possible. For example, below we are just returning 1 because the value itself
isn't used, we just need to know there are records available.

.. code-block:: python

    @sqlexists()
    def item_exists(item_id)
        return QueryData("SELECT 1 FROM table WHERE id=:id", query_params={'id': item_id})

Ultimately, the above query becomes ``SELECT EXISTS (SELECT 1 FROM table WHERE id=:id)``.
You'll notice the inner select value isn't actually used in the return.

Decorator templates
===================

Templates and generators for these templates are also provided to simplify SQL query strings.


**in** template - this template will allow you to pass a list as a single parameter and have the `IN`
condition build out for you. This allows you to more dynamically include values in your queries.

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list):
        return QueryData("SELECT * FROM table WHERE {in__item_id}",
                        template_params={'in__item_id': item_id_list})


you can also use the TemlpateGenerate.in_column method to get back a tuple of query and params

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list):
        in_query, in_params = TemplateGenerators.in_column('key', item_id_list)
        # NOTE: the query string is using an f-string and passing into query_params instead of template_params
        return QueryData(f"SELECT * FROM table WHERE {in_query}", query_params=in_params)


**in and not in multi column** - this template works the same as the in and not in template but it will allow you to
pass a list of tuples to an in clause allowing you to match against multiple columns.
`NOTE: this is only available through the TemplateGenerators using query_params and not through the the template_params method`

.. code-block:: python

    @sqlquery()
    def select_multi(tuple_list):
        in_query, in_params = TemplateGenerators.in_multi_column('(key1, key2)', tuple_list)
        return QueryData(f"SELECT * FROM table WHERE {in_query}", query_params=in_params)


.. code-block:: python

    @sqlquery()
    def select_multi(tuple_list):
        in_query, in_params = TemplateGenerators.not_in_multi_column('(key1, key2)', tuple_list)
        return QueryData(f"SELECT * FROM table WHERE {in_query}", query_params=in_params)


**not_in** template -  this template will allow you to pass a list as a single parameter and have the `NOT IN`
condition build out for you. This allows you more dynamically exclude values in your queries.

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list)
        return QueryData("SELECT * FROM table WHERE {not_in__item_id}",
                        template_params={'not_in__item_id': item_id_list})




you can also use the TemplateGenerators.not_in_column method to get back a tuple of query and params

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list):
        not_in_query, not_in_params = TemplateGenerators.not_in_column('key', item_id_list)
        # NOTE: the query string is using an f-string and passing into query_params instead of template_params
        return QueryData(f"SELECT * FROM table WHERE {not_in_query}", query_params=not_in_params)


**values** template - when inserting and you have multiple records to insert, this allows you to pass
multiple records for insert in a single INSERT statement.

.. code-block:: python

    @sqlquery()
    def insert_items(items):
        return QueryData("INSERT_INTO table(column_a, column_b) {values__items}",
                        template_params={'values__items': item_id_list})

You can write queries that combine ``template_params`` and ``query_params`` as well..

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list, name):
        return QueryData("SELECT * FROM table WHERE {in__item_id} and name=:name",
                        template_params={'in__item_id': item_id_list},
                        query_params={'name': name})

Testing with Managers
=====================

During testing, it may be useful to hook up a real database to the tests. However, this can be difficult to maintain
schema and isolate databases during testing. Database test managers exist for this reason. Usage is very simple with
pytest.

.. code-block:: python

    @pytest.fixture(scope='module', autouse=True)
    def setup_db(self):
        # Pass in the database name and any optional params
        with MariaDbTestManager(f'testdb_{self.__class__.__name__.lower()}'):
            yield

The Maria database test manager is shown used above, but future implementations may be added for other SQL backends.

######################
 Dynamic SQL (dy-sql)
######################

This project consists of a set of python decorators that eases integration with SQL databases. These decorators
may trigger queries, inserts, updates, and deletes.

The decorators are a way to help us map our data in python to SQL queries and vice versa.
When we select, insert, update, or delete the queries, we pass the data we want
to insert along with a well defined query.

This is designed to be done with minimal setup and coding. You need to specify 
the database connection parameters and annotate any SQL queries/updates you have with the
decorator that fits your needs.

Component Breakdown
===================
* **set_database_parameters** - this function needs to be used to set the database parameters so when a decorator function is called, it can initialize a connection pool to a correct database.
* **DbMapResult** - this is a base class that can be used when selecting out data that helps to map the results of a query to an object in python
* **@sqlquery** - this is a decorator for select queries that can return a sql result in a DbMapResult
* **@sqlinsert** - this is a decorator for any queries that can change data in the database. This can take a set of values and yield multiple operations back for insertions inside of a transaction.
* **@sqlexists** - this is a decorator that does a select query but wraps it and handles the return values giving back a True or False

Database Preperation
====================
In order to initialize a connection pool when you first call a function with a
sql decorator, the database needs to be set up.
this is an init function to help with this. in your initialization code for your
app, call the init function like in the following example.

.. code-block:: python

    from dysql import set_database_parameters


    def set_database_from_config():
        maria_db_config = {...}
        set_database_parameters(
            maria_db_host,
            maria_db_user,
            maria_db_password,
            maria_db_databas,
            port=maria_db_port,
        )

Note: the keyword arguments are not required and have standard default values,
the port for example defaults to 3306

Decorators
==========
Decorators are an easy way for us to tell a function to be a 'query' and return
 a result without having to have a big chunk of boiler plate code. once the
 database has been prepared, calling a sql decorated function will initialize
 the database, parse the value returned in your function, make a corresponding
 parameterized query and return the results.

The basic structure is to decorate a method that returns a query string and its
 parameters should it have any parameters. There are multiple options for
 returning a query. below is a summary of some of the possibilities:

* return a QueryData object that possibly contains queryparams and or templateparams
* yield one or more QueryData object that possibly contains queryparams and or templateparams

DbMapResult
~~~~~~~~~~~
This wraps and returns the results of a query making it easier to access the data
you put into it for example if you make the query "SELECT id, name FROM table",
it would return a list of DbMapResult objects that contain fields for each of
those properties. You could then easily loop through and access the properties
like in the following example

.. code-block:: python

    @sqlquery()
    def get_items_from_sql_query():
        return QueryData("SELECT id, name FROM table")

    def get_and_process_items():
        for item in get_items_from_sql_query():
            # we are able to access properties on the object
            print('{name} goes with {id}'.format(item.name, item.id))

We can inherit from DbMapResult and override the way our data maps into the
object. This is primarily helpful in cases where we end up with multiple rows
because a query has a 1 to many relationship

.. code-block:: python

    class ExampleMap(DbMapResult):
        def map_result(self, result):
            # we know we are mapping multiple rows to a single result
            if self.id is None:
                #in our case we know the id is the same so we only set it the first time
                self.id = result['id']
                #initialize our array
                self.item_names = []

        #we know that every result for a given id has a unique item_name
        self.item_names.append(result['item_name'])

    @sqlquery(mapping=ExampleMap)
    def get_table_items()
        return QueryData("""
            SELECT id, name, item_name FROM table
            JOIN table_item ON table.id = table_item.table_id
            JOIN item ON item.id = table_item.item_id""")

    def print_item_names()
        for table_item in get_table_items():
            for item_name in table_item.item_names:
                print('table name {} has item {}'.format(table_item.name, item_name))

@sqlquery
~~~~~~~~~
This is for making sql select calls. An optional mapper may be specified to
change the behavior of what is returned from a decorated method. The default
mapper can combine multiple records into a single result if there is an
``id`` field present in each record. Mappers available:

* ``RecordCombiningMapper`` (default) - returns a list of results, with multiple records with the same ``id`` value
  being combined into a single result. An optional ``record_mapper`` value may be passed to the constructor to change
  how records are mapped to result.
* ``SingleRowMapper`` - returns an object for the first record from the database (even if multiple records are
  returned). An optional ``record_mapper`` value may be passed to the construct to change how this first record is
  mapped to the result.
* ``SingleColumnMapper`` - returns a list of scalars with the first column from every record, even if multiple columns
  are returned from the database.
* ``SingleRowAndColumnMapper`` - returns a single scalar value even if multiple records and columns are returned
  from the database.
* ``CountMapper`` - alias for ``SingleRowAndColumnMapper`` to make it clearer for counting queries.
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
        return QueryData("SELECT * FROM table WHERE name=:name", query_params={'name':name})

query that only returns a single result from the first row

.. code-block:: python

    def get_joe_id():
        result = get_item_for_name('joe')
        return result.get('id')

    # Either an instance or class may be used as the mapper parameter
    @sqlquery(mapper=SingleRowMapper())
    def get_item_for_name(name)
        return QueryData("SELECT id, name FROM table WHERE name=:name", query_params={'name':name})

alternative to the above query that returns the id directly

.. code-block:: python

    def get_joe_id():
        return get_id_for_name('joe')

    @sqlquery(mapper=SingleRowAndColumnMapper)
    def get_id_for_name(name)
        return QueryData("SELECT id FROM table WHERE name=:name", query_params={'name':name})

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
    def get_count_for_name(name)
        return QueryData("SELECT COUNT(*) FROM table WHERE name=:name", query_params={'name':name})


@sqlupdate
~~~~~~~~~~
Handles anything that is not a select. this is primarily, but not limited to, for insert, update, and delete.

.. code-block:: python

    @sqlquery()
    def select_items(item_dict)
        return QueryData("INSERT INTO", template_params={'in__item_id':item_id_list})

@sqlexists
~~~~~~~~~~
This wraps up a sql query conditionally and ultimately returns a boolean value to the caller. The query you give here can return anything you want but as good practice, try to always select as little as possible. For example, below we are just returning 1 because the value itself isn't used, we just need to know there are records avaliable

.. code-block:: python

    @sqlquery()
    def item_exists(item_id)
        return QueryData("SELECT 1 FROM table WHERE id=:id", query_params={'id':item_id})

Ultimately, the above query becomes "SELECT EXISTS (SELECT 1 FROM table WHERE id=:id)". You'll notice the inner select value isn't actually used

Decorator templates
===================

**in** template - this template will allow you to pass a list as a single parameter and have the `IN` condition build out for you. This allows you to more dynamically include values in your queries.

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list)
        return QueryData("SELECT * FROM table WHERE {in__item_id}",
                        template_params={'in__item_id':item_id_list})

**not_in** template -  this template will allow you to pass a list as a single parameter and have the `NOT IN` condition build out for you. This allows you more dynamically exclude values in your queries.

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list)
        return QueryData("SELECT * FROM table WHERE {not_in__item_id}",
                        template_params={'not_in__item_id':item_id_list})

**values** template - when inserting and you have multiple records to insert, this allows you to pass multiple records for insert in a single INSERT statement

.. code-block:: python

    @sqlquery()
    def insert_items(items)
        return QueryData("INSERT_INTO table(column_a, column_b) {values__items}",
                        template_params={'values__items':item_id_list})

You can write queries that contain templates and query_params used

.. code-block:: python

    @sqlquery()
    def select_items(item_id_list, name)
        return QueryData("SELECT * FROM table WHERE {in__item_id} and name=:name",
                        template_params={'in__item_id':item_id_list},
                        query_params={'name': name})


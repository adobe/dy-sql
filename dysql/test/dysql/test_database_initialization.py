# pylint: disable=protected-access

import pytest

import dysql.connections
from dysql import sqlquery, DBNotPreparedError, set_database_parameters, QueryData
from dysql.test.dysql import setup_mock_engine


class TestDatabaseInitialization:
    """
    these tests reflect what can happen when dealing with a config passing in a value that may not
    have been set. there are some parameters that are absolutely necessary, if they aren't set we
    want to make sure we are warning the user in different ways
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_engine():
        dysql.connections.DATABASES = {}
        return setup_mock_engine()

    def test_nothing_set(self):
        dysql.connections.DATABASES = {}
        with pytest.raises(DBNotPreparedError) as error:
            self.query()
        assert str(error.value) == "Unable to connect to a database, set_database_parameters must first be called"

    @staticmethod
    def test_user_required():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters('h', None, 'p', 'd')
        assert str(error.value) == 'Database parameter "user" is not set or empty and is required'

    @staticmethod
    def test_password_required():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters('h', 'u', None, 'd')
        assert str(error.value) == 'Database parameter "password" is not set or empty and is required'

    @staticmethod
    def test_host_required():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters(None, 'u', 'p', 'd')
        assert str(error.value) == 'Database parameter "host" is not set or empty and is required'

    @staticmethod
    def test_database_required():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters('h', 'u', 'p', None)
        assert str(error.value) == 'Database parameter "database" is not set or empty and is required'

    @staticmethod
    def test_user_required_empty_given():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters('h', '', 'p', 'd')
        assert str(error.value) == 'Database parameter "user" is not set or empty and is required'

    @staticmethod
    def test_password_required_empty_give():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters('h', 'u', '', 'd')
        assert str(error.value) == 'Database parameter "password" is not set or empty and is required'

    @staticmethod
    def test_host_required_empty_give():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters('', 'u', 'p', 'd')
        assert str(error.value) == 'Database parameter "host" is not set or empty and is required'

    @staticmethod
    def test_database_required_empty_given():
        with pytest.raises(DBNotPreparedError) as error:
            set_database_parameters('h', 'u', 'p', '')
        assert str(error.value) == 'Database parameter "database" is not set or empty and is required'

    def test_minimal_credentials(self, mock_engine):
        set_database_parameters('h', 'u', 'p', 'd')

        mock_engine.connect().execution_options().execute.return_value = []
        self.query()

    @staticmethod
    @sqlquery()
    def query():
        """
        This is used to call the code that would initialize the database on the first time.
        If there were any failures this is where we would expect to see them.
        """
        return QueryData("SELECT * FROM table")

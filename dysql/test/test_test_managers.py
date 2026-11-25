"""
Copyright 2026 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

import pytest
from unittest.mock import patch

import dysql.databases
from dysql.test_managers import MariaDbTestManager


class TestMariaDbTestManager:
    """
    Tests for MariaDbTestManager
    """

    @patch.object(MariaDbTestManager, "_run")
    def test_create_test_db_without_schema(self, mock_run):
        """Test that _create_test_db works without a schema_db_name"""
        manager = MariaDbTestManager(db_name="test_db")
        manager._create_test_db()

        # Should have called _run twice (DROP and CREATE)
        assert mock_run.call_count == 2

        # Verify DROP command
        drop_call = mock_run.call_args_list[0]
        assert "DROP DATABASE IF EXISTS test_db" in drop_call[0][0]

        # Verify CREATE command
        create_call = mock_run.call_args_list[1]
        assert "CREATE DATABASE IF NOT EXISTS test_db" in create_call[0][0]

    @patch.object(MariaDbTestManager, "_run")
    @pytest.mark.parametrize("command", ["--no-data", "--routines", "source_db"])
    def test_create_test_includes_commands(self, mock_run, command):
        """Test that _create_test_db includes --routines flag when copying schema"""
        manager = MariaDbTestManager(db_name="test_db", schema_db_name="source_db")
        manager._create_test_db()

        # Should have called _run three times (DROP, CREATE, and mysqldump)
        assert mock_run.call_count == 3

        # Verify mysqldump command includes command
        mysqldump_call = mock_run.call_args_list[2]
        mysqldump_command = mysqldump_call[0][0]

        assert "mariadb-dump" in mysqldump_command
        assert command in mysqldump_command

    @patch.object(MariaDbTestManager, "_run")
    def test_create_test_db_mysqldump_command_format(self, mock_run):
        """Test the exact format of the mysqldump command"""
        manager = MariaDbTestManager(db_name="test_db", schema_db_name="source_db")
        manager._create_test_db()

        mysqldump_call = mock_run.call_args_list[2]
        mysqldump_command = mysqldump_call[0][0]

        # Verify the command has the correct structure
        # mysqldump --no-data --routines -p{password} {schema_db_name} -h{host} | mariadb ...
        assert "| mariadb" in mysqldump_command
        assert "test_db" in mysqldump_command

    @patch.object(MariaDbTestManager, "_run")
    def test_tear_down_test_db(self, mock_run):
        """Test that _tear_down_test_db drops the database"""
        manager = MariaDbTestManager(db_name="test_db")
        manager._tear_down_test_db()

        assert mock_run.call_count == 1
        drop_call = mock_run.call_args_list[0]
        assert "DROP DATABASE IF EXISTS test_db" in drop_call[0][0]


@pytest.fixture(name="clean_databases")
def fixture_clean_databases():
    dysql.databases._DEFAULT_CONNECTION_PARAMS_BY_KEY.clear()
    dysql.databases.set_default_database_key(None)
    dysql.databases.reset_current_database()
    yield
    dysql.databases._DEFAULT_CONNECTION_PARAMS_BY_KEY.clear()
    dysql.databases.set_default_database_key(None)
    dysql.databases.reset_current_database()


@pytest.mark.usefixtures("clean_databases")
@patch.object(MariaDbTestManager, "_run")
class TestMariaDbTestManagerDefaultDatabase:
    """
    The test database becomes the default database while the manager is active, even if another database
    was registered first (for example by the application under test).
    """

    def test_test_db_is_default_while_active(self, _mock_run):
        dysql.databases.set_default_connection_parameters("h", "u", "p", "app_db")

        with MariaDbTestManager(db_name="test_db"):
            # Simulates code that does not see the current database (another thread or a reset context)
            dysql.databases.reset_current_database()
            assert dysql.databases._get_current_database_key() == "test_db"

        assert dysql.databases.get_default_database_key() == "app_db"

    def test_consecutive_managers(self, _mock_run):
        dysql.databases.set_default_connection_parameters("h", "u", "p", "app_db")

        with MariaDbTestManager(db_name="test_db1"):
            assert dysql.databases.get_default_database_key() == "test_db1"
        with MariaDbTestManager(db_name="test_db2"):
            assert dysql.databases.get_default_database_key() == "test_db2"

        assert dysql.databases.get_default_database_key() == "app_db"

    def test_restores_explicit_default(self, _mock_run):
        dysql.databases.set_default_connection_parameters("h", "u", "p", "app_db")
        dysql.databases.set_default_connection_parameters("h", "u", "p", "other_db")
        dysql.databases.set_default_database_key("other_db")

        with MariaDbTestManager(db_name="test_db"):
            assert dysql.databases.get_default_database_key() == "test_db"

        assert dysql.databases.get_default_database_key() == "other_db"

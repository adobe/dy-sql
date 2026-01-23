"""
Copyright 2026 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""

import pytest
from unittest.mock import patch

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

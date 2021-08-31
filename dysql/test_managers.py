"""
Copyright 2021 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import abc
import logging
import os
import subprocess
from time import sleep
from typing import Optional

from .connections import sqlquery
from .databases import is_set_current_database_supported, set_current_database, set_default_connection_parameters
from .mappers import CountMapper
from .query_utils import QueryData

LOGGER = logging.getLogger(__name__)


class DbTestManagerBase(abc.ABC):
    """
    Base class for all test managers. See individual implementations for usage details.
    """
    # pylint: disable=too-many-instance-attributes

    def __init__(
            self,
            host: str,
            username: str,
            password: str,
            db_name: str,
            schema_db_name: Optional[str],
            docker_container: Optional[str] = None,
            keep_db: bool = False,
            **connection_defaults,
    ):  # pylint: disable=too-many-arguments
        """
        Constructor, any unknown kwargs are passed directly to set_default_connection_parameters.

        :param host: the host to access the test database
        :param username: the username to access the test database
        :param password: the password to access the test database
        :param db_name: the name of the test database
        :param schema_db_name: the name of the DB to duplicate schema from (using mysqldump)
        :param docker_container: the name of the docker container where the database is running (if in docker)
        :param keep_db: This prevents teardown from removing the created database after running tests
                        which can be helpful in debugging
        """
        self.host = host
        self.username = username
        self.password = password
        self.db_name = db_name
        self.schema_db_name = schema_db_name
        self.docker_container = docker_container
        self.keep_db = keep_db
        self.connection_defaults = connection_defaults
        self.in_docker = self._is_running_in_docker()

    @staticmethod
    def _is_running_in_docker():
        if os.path.exists('/proc/1/cgroup'):
            with open('/proc/1/cgroup', 'rt', encoding='utf8') as fobj:
                contents = fobj.read()
                for marker in ('docker', 'kubepod', 'lxc'):
                    if marker in contents:
                        return True
        return False

    def __enter__(self):
        LOGGER.debug(f'Setting up database : {self.db_name}')
        # Set the host based on whether we are in buildrunner or not (to test locally)

        self._create_test_db()
        set_default_connection_parameters(
            self.host,
            self.username,
            self.password,
            self.db_name,
            **self.connection_defaults,
        )

        # Set the database if supported by the python runtime
        if is_set_current_database_supported():
            set_current_database(self.db_name)

        if self.schema_db_name:
            self._wait_for_tables_exist()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.keep_db:
            LOGGER.debug(f'Tearing down database : {self.db_name}')
            self._tear_down_test_db()

    @abc.abstractmethod
    def _create_test_db(self) -> None:
        """
        Must be overridden in implementing classes. Creates the test database from the self.schema_db_name
        database (if present) or an empty database desired.
        """

    @abc.abstractmethod
    def _get_tables_count(self, db_name: str) -> int:
        """
        Must be overridden in implementing classes. Retrieves the current table count for the database.
        :param db_name: the database to retrieve the count for
        :return: the number of tables present in the database
        """

    @abc.abstractmethod
    def _tear_down_test_db(self) -> None:
        """
        Must be overridden in implementing classes. Tears down the test database.
        """

    def _wait_for_tables_exist(self) -> None:
        """
        Waits for the tables to exist in the new test database.
        """
        tables_exist = False
        expected_count = self._get_tables_count(self.schema_db_name)
        while not tables_exist:
            sleep(.25)
            LOGGER.debug('Tables are still not ready')
            actual_count = self._get_tables_count(self.db_name)
            tables_exist = expected_count == actual_count

    def _run(self, command: str) -> subprocess.CompletedProcess:
        """
        Runs a command, wrapping the command in docker commands if necessary.
        :param command: the command to run
        :return: the completed process information
        """
        if not self.in_docker:
            command = f"docker exec {self.docker_container} bash -c '{command}'"

        try:
            LOGGER.debug(f"Executing : '{command}'")
            completed_process = subprocess.run(command, shell=True, timeout=30, check=True, capture_output=True)
            LOGGER.debug(f"Executed : {completed_process.stdout}")

            return completed_process
        except subprocess.CalledProcessError:
            LOGGER.exception(f'Error handling command : {command}')
            raise


class MariaDbTestManager(DbTestManagerBase):
    """
    May be used in testing to copy and create a database and schema. This class works specifically with
    Maria/MySQL databases.

    Example:

    @pytest.fixture(scope='module', autouse=True)
    def setup_db(self):
        # Pass in the database name and any optional params
        with MariaDbTestManager(f'testdb_{self.__class__.__name__.lower()}'):
            yield
    """
    # pylint: disable=too-few-public-methods

    def __init__(
            self,
            db_name: str,
            schema_db_name: Optional[str] = None,
            echo_queries: bool = False,
            keep_db: bool = False,
            pool_size=3,
    ):  # pylint: disable=too-many-arguments
        """
        :param db_name: the name you want for your test database
        :param schema_db_name: the name of the DB to duplicate schema from (using mysqldump)
        :param echo_queries: True if you want to see queries
        :param keep_db: This prevents teardown from removing the created DB after running tests
                        which can be helpful in debugging
        """
        super().__init__(
            os.getenv('MARIA_HOST', 'localhost'),
            os.getenv('MARIA_USERNAME', 'root'),
            os.getenv('MARIA_PASSWORD', 'password'),
            db_name,
            schema_db_name,
            port=3306,
            echo_queries=echo_queries,
            pool_size=pool_size,
            docker_container=os.getenv('MARIA_CONTAINER_NAME', 'mariadb'),
            keep_db=keep_db,
        )

    def _create_test_db(self) -> None:
        self._run(f'mysql -p{self.password} -h{self.host} -N -e "DROP DATABASE IF EXISTS {self.db_name}"')
        self._run(f'mysql -p{self.password} -h{self.host} -s -N -e "CREATE DATABASE IF NOT EXISTS {self.db_name}"')
        if self.schema_db_name:
            self._run(
                f'mysqldump --no-data -p{self.password} {self.schema_db_name} -h{self.host} '
                f'| mysql -p{self.password} {self.db_name} -h{self.host}'
            )

    def _tear_down_test_db(self) -> None:
        self._run(f'echo "DROP DATABASE IF EXISTS {self.db_name} " | mysql -p{self.password} -h{self.host}')

    @sqlquery(mapper=CountMapper())
    def _get_tables_count(self, db_name: str) -> int:
        # pylint: disable=unused-argument
        return QueryData(
            '''
            SELECT count(1)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=:db_name
            ''', query_params={'db_name': db_name})

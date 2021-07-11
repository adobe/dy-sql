import logging
import os
import subprocess
from time import sleep

from dysql import set_database_parameters, sqlquery, QueryData, CountMapper

LOGGER = logging.getLogger(__name__)


class TestDBManager:
    """
    TestDBManager helps to copy and create a schema that can be used for testing in better isolation without having to
    do anything manually outside the tests.

    Example:

    @pytest.fixture(scope='module', autouse=True)
    def setup_db(self):
        # Pass in the database name and any optional params
        with TestDBManager(f'testdb_{self.__class__.__name__.lower()}'):
            yield
    """

    def __init__(self, db_name, schema_db_name=None, echo_queries=False, keep_db=False):
        """
        :param db_name: the name you want for your test DB
        :param schema_db_name: the name of the DB to duplicate schema from (using mysqldump)
        :param echo_queries: True if you want to see queries
        :param keep_db: This prevents teardown from removing the created DB after running tests
                        which can be helpful in debugging
        """
        self.host = os.getenv('MARIA_HOST', 'localhost')
        self.schema_db_name = schema_db_name
        self.docker_container = os.getenv('MARIA_CONTAINER_NAME', 'mariadb')
        self.in_docker = self._is_running_in_docker()
        self.db_name = db_name
        self.echo_queries = echo_queries
        self.keep_db = keep_db

    @staticmethod
    def _is_running_in_docker():
        with open('/proc/1/cgroup', 'rt') as fobj:
            contents = fobj.read()
            for marker in ('docker', 'kubepod', 'lxc'):
                if marker in contents:
                    return True
        return False

    def __enter__(self):
        LOGGER.debug(f'Setting up database : {self.db_name}')
        # Set the host based on whether we are in buildrunner or not (to test locally)

        self._create_test_db()
        set_database_parameters(
            self.host,
            # because the way the basic mariadb container is set up, its easy for us to test this way
            os.getenv('MARIA_USERNAME', 'root'),
            os.getenv('MARIA_PASSWORD', 'password'),
            self.db_name,
            port=3306,
            echo_queries=self.echo_queries,
            pool_size=3)

        if self.schema_db_name:
            self._wait_for_tables_exist()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.keep_db:
            LOGGER.debug(f'Tearing down database : {self.db_name}')
            self._tear_down_test_db()

    def _create_test_db(self):
        self._run(f'mysql -ppassword -h{self.host} -N -e "DROP DATABASE IF EXISTS {self.db_name}"')
        self._run(f'mysql -ppassword -h{self.host} -s -N -e "CREATE DATABASE IF NOT EXISTS {self.db_name}"')
        if self.schema_db_name:
            self._run(
                f'mysqldump --no-data -ppassword {self.schema_db_name} -h{self.host} '
                f'| mysql -ppassword {self.db_name} -h{self.host}'
            )

    def _wait_for_tables_exist(self):

        @sqlquery(mapper=CountMapper())
        def get_tables_count(db_name):
            return QueryData(
                '''
                SELECT count(1)
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=:db_name
                ''', query_params={'db_name': db_name})

        tables_exist = False
        expected_count = get_tables_count(self.schema_db_name)
        while not tables_exist:
            sleep(.25)
            LOGGER.debug('Tables are still not ready')
            actual_count = get_tables_count(self.db_name)
            tables_exist = expected_count == actual_count

    def _tear_down_test_db(self):
        self._run(f'echo "DROP DATABASE IF EXISTS {self.db_name} " | mysql -ppassword -h{self.host}')

    def _run(self, command):

        if not self.in_docker:
            command = f"docker exec -d {self.docker_container} bash -c '{command}'"

        try:
            LOGGER.debug(f"Executing : '{command}'")
            completed_process = subprocess.run(command, shell=True, timeout=30, check=True, capture_output=True)
            LOGGER.debug(f"Executed : {completed_process.stdout}")

            return completed_process.stdout
        except subprocess.CalledProcessError as exc:
            LOGGER.error(f'Error handling command : {command}')
            raise exc

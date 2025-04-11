import unittest
from abc import ABC, abstractmethod

from model.schema import Table
from test.duckdb.db import TestDuckDB, DuckDB
from test.executionserver.testutils import TestExecutionServer


class ExecutionServerTest(unittest.TestCase, ABC):

    @classmethod
    def setUpClass(cls):
        cls.execution_server = TestExecutionServer("../executionserver")
        cls.execution_server.start()
        cls.duckdb = TestDuckDB()
        cls.duckdb.start()

    @classmethod
    def tearDownClass(cls):
        cls.execution_server.stop()
        if cls.duckdb:
            cls.duckdb.stop()
            cls.duckdb = None

    @classmethod
    def create_table(cls, table: Table):
        cls.duckdb.db.create_table(table)

    @classmethod
    def load_csv(cls, table: Table, path: str):
        cls.duckdb.db.load_csv(table, path)

    @classmethod
    def get_duckdb_path(cls):
        return cls.duckdb.db.get_db_path()

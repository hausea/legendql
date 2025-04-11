import json

from model.schema import Table, Database
from ql.rawlegendql import RawLegendQL
from runtime.pure.db.duckdb import DuckDBDatabaseType
from runtime.pure.executionserver.runtime import ExecutionServerRuntime
from test.executionserver.test import ExecutionServerTest

table = Table("employees", {"id": int, "departmentId": int, "first": str, "last": str})
database = Database("local::DuckDuckDatabase", [table])

class TestExecutionServerEvaluation(ExecutionServerTest):
    @classmethod
    def setUpClass(cls):
        ExecutionServerTest.setUpClass()
        ExecutionServerTest.create_table(table)
        ExecutionServerTest.load_csv(table, "../data/employees.csv")

    def test_execution_against_execution_server(self):
        runtime = ExecutionServerRuntime("local::DuckDuckRuntime", DuckDBDatabaseType(ExecutionServerTest.get_duckdb_path()), "http://localhost:6300", database)
        data_frame = (RawLegendQL.from_table(database, table)
                      .select("id", "departmentId", "first", "last")
                      .bind(runtime))

        result = data_frame.eval()
        print(json.dumps(result, indent=4))

from model.schema import Table, Database
from ql.query import Query
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
        data_frame = (Query.from_table(database, table)
                      .select("id", "departmentId", "first", "last")
                      .bind(runtime))

        result = data_frame.eval().data()

        self.assertEqual(result.relation, "#>{local::DuckDuckDatabase.employees}#->select(~[id, departmentId, first, last])->from(local::DuckDuckRuntime)")
        self.assertEqual(result.sql, 'select "employees_0".id as "id", "employees_0".departmentId as "departmentId", "employees_0".first as "first", "employees_0".last as "last" from employees as "employees_0"')
        self.assertEqual(",".join(result.header), "id,departmentId,first,last")
        self.assertEqual(len(result.rows), 2)
        self.assertEqual(",".join(map(lambda r: str(r), result.rows[0])), "1,1, John, Doe")
        self.assertEqual(",".join(map(lambda r: str(r), result.rows[1])), "2,1, Jane, Doe")

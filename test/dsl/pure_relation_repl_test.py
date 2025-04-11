import unittest

from model.schema import Table, Database
from ql.legendql import LegendQL
from runtime.pure.repl.repl_utils import is_repl_running, send_to_repl, load_csv_to_repl
from runtime.pure.repl.runtime import ReplRuntime


class TestReplEvaluation(unittest.TestCase):

    def setUp(self):
        if not is_repl_running():
            self.skipTest("REPL is not running")
        load_csv_to_repl("../data/employees.csv", "local::DuckDuckConnection", "employees")
        load_csv_to_repl("../data/departments.csv", "local::DuckDuckConnection", "departments")

    def tearDown(self):
        send_to_repl("drop local::DuckDuckConnection employees")
        send_to_repl("drop local::DuckDuckConnection departments")

    def test_simple_select(self):
        table = Table("employees", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        runtime = ReplRuntime("local::DuckDuckRuntime")

        data_frame = (LegendQL.from_table(database, table)
                      .select(lambda r: [r.id, r.departmentId, r.first, r.last])
                      .bind(runtime))
        results = data_frame.eval()
        self.assertEqual("""> +--------+--------------+------------+------------+
|   id   | departmentId |   first    |    last    |
| BIGINT |    BIGINT    | VARCHAR(0) | VARCHAR(0) |
+--------+--------------+------------+------------+
|   1    |      1       |    John    |     Doe    |
|   2    |      1       |    Jane    |     Doe    |
+--------+--------------+------------+------------+
2 rows -- 4 columns""", results[:results.rfind("columns") + 7])

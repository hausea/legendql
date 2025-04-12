import unittest

from model.metamodel import BinaryExpression, OperandExpression, ColumnAliasExpression, LiteralExpression, \
    IntegerLiteral, EqualsBinaryOperator, \
    FunctionExpression, CountFunction, InnerJoinType, ColumnReferenceExpression, ComputedColumnAliasExpression, \
    MapReduceExpression, LambdaExpression, VariableAliasExpression
from model.schema import Database, Table
from ql.query import Query
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
        runtime = ReplRuntime("local::DuckDuckRuntime")
        table = Table("employees", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (Query.from_table(database, table)
                      .select("id", "departmentId", "first", "last")
                      .bind(runtime))
        results = data_frame.eval().data()
        self.assertEqual("""> +--------+--------------+------------+------------+
|   id   | departmentId |   first    |    last    |
| BIGINT |    BIGINT    | VARCHAR(0) | VARCHAR(0) |
+--------+--------------+------------+------------+
|   1    |      1       |    John    |     Doe    |
|   2    |      1       |    Jane    |     Doe    |
+--------+--------------+------------+------------+
2 rows -- 4 columns""", results[:results.rfind("columns") + 7])

    def test_complex_query(self):
        runtime = ReplRuntime("local::DuckDuckRuntime")
        table = Table("employees", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (Query.from_table(database, table)
                      .filter(LambdaExpression(["r"], BinaryExpression(OperandExpression(ColumnAliasExpression("r", ColumnReferenceExpression("departmentId"))), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator())))
                      .select("departmentId")
                      .extend([ComputedColumnAliasExpression("newCol", LambdaExpression(["x"], ColumnAliasExpression("x", ColumnReferenceExpression("departmentId"))))])
                      .group_by([ColumnReferenceExpression("newCol")],
                   [ComputedColumnAliasExpression("count",
                                                 MapReduceExpression(
                                                     LambdaExpression(["x"], ColumnAliasExpression("x", ColumnReferenceExpression("newCol"))),
                                                     LambdaExpression(["x"], FunctionExpression(CountFunction(), [VariableAliasExpression("x")]))))])
                      .limit(1)
                      .join("local::DuckDuckDatabase", "departments", InnerJoinType(), LambdaExpression(["a", "b"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("newCol"))), OperandExpression(ColumnAliasExpression("b", ColumnReferenceExpression("id"))), EqualsBinaryOperator())))
                      .select("id")
                      .bind(runtime))
        results = data_frame.eval().data()
        self.assertEqual("""> +--------+
|   id   |
| BIGINT |
+--------+
|   1    |
+--------+
1 rows -- 1 columns""", results[:results.rfind("columns") + 7])

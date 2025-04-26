import unittest
from datetime import datetime

from dialect.sqlglot.dialect import NonExecutableSQLGlotRuntime
from model.metamodel import (
    IntegerLiteral, InnerJoinType, BinaryExpression, ColumnAliasExpression, LiteralExpression,
    EqualsBinaryOperator, OperandExpression, FunctionExpression, CountFunction, AddBinaryOperator,
    SubtractBinaryOperator, MultiplyBinaryOperator, DivideBinaryOperator, ColumnReferenceExpression,
    ComputedColumnAliasExpression, MapReduceExpression, LambdaExpression, VariableAliasExpression,
    AverageFunction, OrderByExpression, AscendingOrderType, DescendingOrderType, IfExpression,
    GreaterThanBinaryOperator, DateLiteral, ModuloFunction, ExponentFunction
)
from model.schema import Database, Table
from legendql.query import Query


class TestClauseToSQLGlotDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select_duckdb(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .select("column")
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual('SELECT "column" FROM "test_db"."table"', sql)

    def test_simple_select_postgres(self):
        runtime = NonExecutableSQLGlotRuntime("PostgresRuntime", dialect="postgres")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .select("column")
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual('SELECT "column" FROM "test_db"."table"', sql)

    def test_simple_select_with_filter(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .select("column")
                      .filter(LambdaExpression(["a"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator())))
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual('SELECT "column" FROM "test_db"."table" WHERE "column" = 1', sql)

    def test_simple_select_with_extend(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .select("column")
                      .extend([ComputedColumnAliasExpression("a", LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column"))))])
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual('SELECT "column", "column" AS "a" FROM "test_db"."table"', sql)

    def test_simple_select_with_groupBy(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .select("column", "column2")
                      .group_by([ColumnReferenceExpression("column"), ColumnReferenceExpression("column2")],
                   [ComputedColumnAliasExpression("count",
                                                  MapReduceExpression(
                                                      LambdaExpression(["a"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column2"))), AddBinaryOperator())),
                                                      LambdaExpression(["a"], FunctionExpression(CountFunction(), [VariableAliasExpression("a")])))),
                             ComputedColumnAliasExpression("avg",
                                                  MapReduceExpression(
                                                      LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column"))),
                                                      LambdaExpression(["a"], FunctionExpression(AverageFunction(), [VariableAliasExpression("a")]))))
                    ])
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT "column", "column2", COUNT(*) AS count, AVG("column") AS avg FROM "test_db"."table" GROUP BY "column", "column2"',
            sql)

    def test_simple_select_with_limit(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .select("column")
                      .limit(10)
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual('SELECT "column" FROM "test_db"."table" LIMIT 10', sql)

    def test_simple_select_with_join(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .select("column")
                      .join("test_db", "table2", InnerJoinType(), LambdaExpression(["a", "b"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(ColumnAliasExpression("b", ColumnReferenceExpression("column"))), EqualsBinaryOperator())))
                      .select("column2")
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual('SELECT "column2" FROM "test_db"."table" INNER JOIN "test_db"."table2" ON "column" = "column"', sql)

    def test_multiple_extends(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .extend([ComputedColumnAliasExpression("a", LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column")))), ComputedColumnAliasExpression("b", LambdaExpression(["b"], ColumnAliasExpression("b", ColumnReferenceExpression("column"))))])
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT *, "column" AS "a", "column" AS "b" FROM "test_db"."table"',
            sql)

    def test_math_binary_operators(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .extend([
                                  ComputedColumnAliasExpression("add", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=AddBinaryOperator()))),
                                  ComputedColumnAliasExpression("subtract", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=SubtractBinaryOperator()))),
                                  ComputedColumnAliasExpression("multiply", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=MultiplyBinaryOperator()))),
                                  ComputedColumnAliasExpression("divide", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=DivideBinaryOperator()))),
                              ])
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT *, "column" + "column" AS "add", "column" - "column" AS "subtract", "column" * "column" AS "multiply", "column" / "column" AS "divide" FROM "test_db"."table"',
            sql)

    def test_single_rename(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .rename(('column', 'newColumn'))
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT "column" AS "newColumn" FROM "test_db"."table"',
            sql)

    def test_multiple_renames(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .rename(('columnA', 'newColumnA'), ('columnB', 'newColumnB'))
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT "columnA" AS "newColumnA", "columnB" AS "newColumnB" FROM "test_db"."table"',
            sql)

    def test_offset(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .offset(5)
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT * FROM "test_db"."table" OFFSET 5',
            sql)

    def test_order_by(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .order_by(
                OrderByExpression(direction=AscendingOrderType(), expression=ColumnReferenceExpression(name="columnA")),
                         OrderByExpression(direction=DescendingOrderType(), expression=ColumnReferenceExpression(name="columnB")))
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT * FROM "test_db"."table" ORDER BY "columnA" ASC, "columnB" DESC',
            sql)

    def test_conditional(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "columnA": int, "columnB": int})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .extend([ComputedColumnAliasExpression("conditional", LambdaExpression(["a"], IfExpression(test=BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("columnA"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("columnB"))), operator=GreaterThanBinaryOperator()), body=ColumnAliasExpression("a", ColumnReferenceExpression("columnA")), orelse=ColumnAliasExpression("a", ColumnReferenceExpression("columnB")))))])
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT *, CASE WHEN "columnA" > "columnB" THEN "columnA" ELSE "columnB" END AS "conditional" FROM "test_db"."table"',
            sql)

    def test_date(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "columnA": int, "columnB": int})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .extend([
                        ComputedColumnAliasExpression("dateGreater", LambdaExpression(parameters=["a"], expression=BinaryExpression(left=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 11)))), right=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 12)))), operator=GreaterThanBinaryOperator()))),
                        ComputedColumnAliasExpression("dateTimeGreater", LambdaExpression(parameters=["a"], expression=BinaryExpression(left=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 11, 10, 0, 0)))), right=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 12, 10, 0, 0)))), operator=GreaterThanBinaryOperator()))),
                      ])
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT *, CAST(\'2025-04-11T00:00:00\' AS DATE) > CAST(\'2025-04-12T00:00:00\' AS DATE) AS "dateGreater", CAST(\'2025-04-11T10:00:00\' AS DATE) > CAST(\'2025-04-12T10:00:00\' AS DATE) AS "dateTimeGreater" FROM "test_db"."table"',
            sql)

    def test_modulo_and_exponent(self):
        runtime = NonExecutableSQLGlotRuntime("DuckDBRuntime", dialect="duckdb")
        table = Table("table", {"id": int, "columnA": int, "columnB": int})
        database = Database("test_db", [table])
        data_frame = (Query.from_table(database, table)
                      .extend([
                        ComputedColumnAliasExpression("modulo", LambdaExpression(["a"], FunctionExpression(parameters=[ColumnAliasExpression("a", ColumnReferenceExpression("column")), LiteralExpression(literal=IntegerLiteral(2))], function=ModuloFunction()))),
                        ComputedColumnAliasExpression("exponent", LambdaExpression(["a"], FunctionExpression(parameters=[ColumnAliasExpression("a", ColumnReferenceExpression("column")), LiteralExpression(literal=IntegerLiteral(2))], function=ExponentFunction())))
                      ])
                      .bind(runtime))
        sql = data_frame.executable_to_string()
        self.assertEqual(
            'SELECT *, MOD("column", 2) AS "modulo", POW("column", 2) AS "exponent" FROM "test_db"."table"',
            sql)

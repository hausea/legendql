import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import IntegerLiteral, InnerJoinType, BinaryExpression, ColumnAliasExpression, LiteralExpression, \
    EqualsBinaryOperator, OperandExpression, FunctionExpression, \
    CountFunction, AddBinaryOperator, SubtractBinaryOperator, MultiplyBinaryOperator, DivideBinaryOperator, \
    ColumnReferenceExpression, ComputedColumnAliasExpression, MapReduceExpression, LambdaExpression, \
    VariableAliasExpression, \
    AverageFunction, OrderByExpression, AscendingOrderType, DescendingOrderType
from model.schema import Database, Table
from ql.rawlegendql import RawLegendQL


class TestClauseToPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .select("column")
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_filter(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .select("column")
                      .filter(LambdaExpression(["a"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator())))
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->filter(a | $a.column==1)->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_extend(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .select("column")
                      .extend([ComputedColumnAliasExpression("a", LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column"))))])
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->extend(~[a:a | $a.column])->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_groupBy(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
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
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->select(~[column, column2])->groupBy(~[column, column2], ~[count:a | $a.column+$a.column2 : a | $a->count(), avg:a | $a.column : a | $a->avg()])->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_simple_select_with_limit(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .select("column")
                      .limit(10)
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->limit(10)->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_join(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .select("column")
                      .join("local::DuckDuckDatabase", "table2", InnerJoinType(), LambdaExpression(["a", "b"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(ColumnAliasExpression("b", ColumnReferenceExpression("column"))), EqualsBinaryOperator())))
                      .select("column2")
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->join(#>{local::DuckDuckDatabase.table2}#, JoinKind.INNER, {a, b | $a.column==$b.column})->select(~[column2])->from(local::DuckDuckRuntime)", pure_relation)

    def test_multiple_extends(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .extend([ComputedColumnAliasExpression("a", LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column")))), ComputedColumnAliasExpression("b", LambdaExpression(["b"], ColumnAliasExpression("b", ColumnReferenceExpression("column"))))])
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[a:a | $a.column, b:b | $b.column])->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_math_binary_operators(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .extend([
                                  ComputedColumnAliasExpression("add", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=AddBinaryOperator()))),
                                  ComputedColumnAliasExpression("subtract", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=SubtractBinaryOperator()))),
                                  ComputedColumnAliasExpression("multiply", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=MultiplyBinaryOperator()))),
                                  ComputedColumnAliasExpression("divide", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=DivideBinaryOperator()))),
                              ])
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[add:a | $a.column+$a.column, subtract:a | $a.column-$a.column, multiply:a | $a.column*$a.column, divide:a | $a.column/$a.column])->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_single_rename(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .rename(('column', 'newColumn'))
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->rename(~column, ~newColumn)->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_multiple_renames(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .rename(('columnA', 'newColumnA'), ('columnB', 'newColumnB'))
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->rename(~columnA, ~newColumnA)->rename(~columnB, ~newColumnB)->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_offset(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .offset(5)
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->drop(5)->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_order_by(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (RawLegendQL.from_table(database, table)
                      .order_by(
                OrderByExpression(direction=AscendingOrderType(), expression=ColumnReferenceExpression(name="columnA")),
                         OrderByExpression(direction=DescendingOrderType(), expression=ColumnReferenceExpression(name="columnB")))
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->sort([~columnA->ascending(), ~columnB->descending()])->from(local::DuckDuckRuntime)",
            pure_relation)

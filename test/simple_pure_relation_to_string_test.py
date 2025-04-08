import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import IntegerLiteral, InnerJoinType, BinaryExpression, ReferenceExpression, LiteralExpression, \
    EqualsBinaryOperator, OperandExpression, AliasExpression, ExtendExpression, GroupByExpression, FunctionExpression, \
    CountFunction, AddBinaryOperator, SubtractBinaryOperator, MultiplyBinaryOperator, DivideBinaryOperator
from ql.legendql import LegendQL


class TestPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
         .select("column")
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_filter(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
         .select("column")
         .filter(BinaryExpression(OperandExpression(ReferenceExpression("a", "column")), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator()))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->filter(a | $a.column==1)->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_extend(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
         .select("column")
         .extend([ExtendExpression("a", ReferenceExpression("a", "column"))])
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->extend(~[a:a | $a.column])->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_groupBy(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
         .select("column")
         .group_by(["column"], [GroupByExpression("count", ReferenceExpression("a", "column"), FunctionExpression(CountFunction(), [AliasExpression("a")]))])
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->groupBy(~[column], ~[count: a | $a.column : a | $a->count()])->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_limit(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
          .select("column")
          .limit(10)
          .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->limit(10)->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_join(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
          .select("column")
          .join("local::DuckDuckDatabase", "table2", InnerJoinType(), BinaryExpression(OperandExpression(ReferenceExpression("a", "column")), OperandExpression(ReferenceExpression("b", "column")), EqualsBinaryOperator()))
          .select("column2")
          .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->join(#>{local::DuckDuckDatabase.table2}#, JoinKind.INNER, {a, b | $a.column==$b.column})->select(~[column2])->from(local::DuckDuckRuntime)", pure_relation)

    def test_multiple_extends(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
                      .extend([ExtendExpression("a", ReferenceExpression("a", "column")), ExtendExpression("b", ReferenceExpression("b", "column"))])
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[a:a | $a.column, b:b | $b.column])->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_add_math_binary_operators(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "table")
                      .extend([
                                  ExtendExpression("add", BinaryExpression(left=OperandExpression(ReferenceExpression("a", "column")), right=OperandExpression(ReferenceExpression("a", "column")), operator=AddBinaryOperator())),
                                  ExtendExpression("subtract", BinaryExpression(left=OperandExpression(ReferenceExpression("a", "column")), right=OperandExpression(ReferenceExpression("a", "column")), operator=SubtractBinaryOperator())),
                                  ExtendExpression("multiply", BinaryExpression(left=OperandExpression(ReferenceExpression("a", "column")), right=OperandExpression(ReferenceExpression("a", "column")), operator=MultiplyBinaryOperator())),
                                  ExtendExpression("divide", BinaryExpression(left=OperandExpression(ReferenceExpression("a", "column")), right=OperandExpression(ReferenceExpression("a", "column")), operator=DivideBinaryOperator())),
                              ])
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[add:a | $a.column+$a.column, subtract:a | $a.column-$a.column, multiply:a | $a.column*$a.column, divide:a | $a.column/$a.column])->from(local::DuckDuckRuntime)",
            pure_relation)

import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import ReferenceExpression, FilterClause, UnaryExpression, OperandExpression, NotUnaryOperator, \
    BinaryExpression, EqualsBinaryOperator, Literal, LiteralExpression, StringLiteral
from ql.legendql import LegendQL


class TestPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_filter_clause(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase", "table")
        data_frame = (LegendQL.create()
                      .filter(FilterClause(BinaryExpression(left=OperandExpression(ReferenceExpression("colA", "colA")), right=OperandExpression(ReferenceExpression("colB", "colB")), operator=EqualsBinaryOperator())))
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->filter(x | $x.colA==$x.colB)", pure_relation)
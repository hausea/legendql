import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import SelectionClause, ReferenceExpression
from ql.legendql import LegendQL


class TestPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select_clause(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase", "table")
        data_frame = (LegendQL.create()
         .select(SelectionClause([ReferenceExpression("column", "col")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[col])", pure_relation)

    def test_multiple_select_clause(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase", "table")
        data_frame = (LegendQL.create()
         .select(SelectionClause([ReferenceExpression("colA", "colA"), ReferenceExpression("colB", "colB")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[colA, colB])", pure_relation)

import unittest
import pandas as pd
from model.schema import Table, Database
from model.metamodel import (
    ColumnReferenceExpression, BinaryExpression, OperandExpression, EqualsBinaryOperator,
    LiteralExpression, IntegerLiteral, StringLiteral, BooleanLiteral,
    GreaterThanBinaryOperator, LessThanBinaryOperator, AddBinaryOperator, MultiplyBinaryOperator,
    ColumnAliasExpression, ComputedColumnAliasExpression, GroupByExpression, OrderByExpression,
    AscendingOrderType, DescendingOrderType, JoinExpression
)
from legendql.pandas_parser import PandasParser
from legendql.pandas_ql import PandasQL
from dialect.purerelation.dialect import NonExecutablePureRuntime


class TestPandasParser(unittest.TestCase):
    
    def setUp(self):
        self.df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'department_id': [101, 102, 101],
            'salary': [50000, 60000, 55000]
        })
        
        self.table = Table("test_table", {
            'id': int,
            'name': str,
            'department_id': int,
            'salary': int
        })
    
    def test_parse_select(self):
        """Test parsing of select operation"""
        columns = ['id', 'name']
        result = PandasParser.parse_select(self.df, columns, self.table)
        
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ColumnReferenceExpression)
        self.assertIsInstance(result[1], ColumnReferenceExpression)
        
        self.assertEqual(result[0].name, 'id')
        self.assertEqual(result[1].name, 'name')
        
        self.assertIn('id', self.table.columns)
        self.assertIn('name', self.table.columns)
    
    def test_parse_filter_equals(self):
        """Test parsing of filter operation with equals condition"""
        condition = "id == 2"
        result = PandasParser.parse_filter(self.df, condition, self.table)
        
        self.assertIsInstance(result, BinaryExpression)
        self.assertIsInstance(result.operator, EqualsBinaryOperator)
        
        self.assertIsInstance(result.left, OperandExpression)
        self.assertIsInstance(result.left.expression, ColumnReferenceExpression)
        self.assertEqual(result.left.expression.name, 'id')
        
        self.assertIsInstance(result.right, OperandExpression)
        self.assertIsInstance(result.right.expression, LiteralExpression)
        self.assertIsInstance(result.right.expression.literal, IntegerLiteral)
        self.assertEqual(result.right.expression.literal.value(), 2)
    
    def test_parse_filter_greater_than(self):
        """Test parsing of filter operation with greater than condition"""
        condition = "salary > 55000"
        result = PandasParser.parse_filter(self.df, condition, self.table)
        
        self.assertIsInstance(result, BinaryExpression)
        self.assertIsInstance(result.operator, GreaterThanBinaryOperator)
        
        self.assertIsInstance(result.left, OperandExpression)
        self.assertIsInstance(result.left.expression, ColumnReferenceExpression)
        self.assertEqual(result.left.expression.name, 'salary')
        
        self.assertIsInstance(result.right, OperandExpression)
        self.assertIsInstance(result.right.expression, LiteralExpression)
        self.assertIsInstance(result.right.expression.literal, IntegerLiteral)
        self.assertEqual(result.right.expression.literal.value(), 55000)
    
    def test_parse_filter_less_than(self):
        """Test parsing of filter operation with less than condition"""
        condition = "salary < 60000"
        result = PandasParser.parse_filter(self.df, condition, self.table)
        
        self.assertIsInstance(result, BinaryExpression)
        self.assertIsInstance(result.operator, LessThanBinaryOperator)
        
        self.assertEqual(result.left.expression.name, 'salary')
        self.assertEqual(result.right.expression.literal.value(), 60000)
    
    def test_parse_rename(self):
        """Test parsing of rename operation"""
        rename_dict = {'id': 'employee_id', 'department_id': 'dept_id'}
        result = PandasParser.parse_rename(self.df, rename_dict, self.table)
        
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ColumnAliasExpression)
        self.assertIsInstance(result[1], ColumnAliasExpression)
        
        self.assertEqual(result[0].alias, 'employee_id')
        self.assertEqual(result[0].reference.name, 'id')
        self.assertEqual(result[1].alias, 'dept_id')
        self.assertEqual(result[1].reference.name, 'department_id')
        
        self.assertIn('employee_id', self.table.columns)
        self.assertIn('dept_id', self.table.columns)
    
    def test_parse_extend(self):
        """Test parsing of extend operation"""
        expressions = {
            'bonus': 'salary * 0.1',
            'total': 'salary + bonus'
        }
        result = PandasParser.parse_extend(self.df, expressions, self.table)
        
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ComputedColumnAliasExpression)
        self.assertIsInstance(result[1], ComputedColumnAliasExpression)
        
        self.assertEqual(result[0].alias, 'bonus')
        self.assertIsInstance(result[0].expression, BinaryExpression)
        self.assertIsInstance(result[0].expression.operator, MultiplyBinaryOperator)
        self.assertEqual(result[0].expression.left.expression.name, 'salary')
        
        self.assertEqual(result[1].alias, 'total')
        self.assertIsInstance(result[1].expression, BinaryExpression)
        self.assertIsInstance(result[1].expression.operator, AddBinaryOperator)
        self.assertEqual(result[1].expression.left.expression.name, 'salary')
        self.assertEqual(result[1].expression.right.expression.name, 'bonus')
        
        self.assertIn('bonus', self.table.columns)
        self.assertIn('total', self.table.columns)
    
    def test_parse_group_by(self):
        """Test parsing of group by operation"""
        by = 'department_id'
        agg = {
            'avg_salary': 'avg(salary)',
            'count': 'count(department_id)'
        }
        result = PandasParser.parse_group_by(self.df, by, agg, self.table)
        
        self.assertIsInstance(result, GroupByExpression)
        
        self.assertEqual(len(result.selections), 1)
        self.assertIsInstance(result.selections[0], ColumnReferenceExpression)
        self.assertEqual(result.selections[0].name, 'department_id')
        
        self.assertEqual(len(result.expressions), 2)
        self.assertIsInstance(result.expressions[0], ComputedColumnAliasExpression)
        self.assertIsInstance(result.expressions[1], ComputedColumnAliasExpression)
        
        self.assertEqual(result.expressions[0].alias, 'avg_salary')
        self.assertEqual(result.expressions[1].alias, 'count')
        
        self.assertIn('avg_salary', self.table.columns)
        self.assertIn('count', self.table.columns)
    
    def test_parse_order_by(self):
        """Test parsing of order by operation"""
        by = ['department_id', 'salary']
        ascending = [True, False]
        result = PandasParser.parse_order_by(self.df, by, ascending, self.table)
        
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], OrderByExpression)
        self.assertIsInstance(result[1], OrderByExpression)
        
        self.assertIsInstance(result[0].direction, AscendingOrderType)
        self.assertIsInstance(result[0].expression, ColumnReferenceExpression)
        self.assertEqual(result[0].expression.name, 'department_id')
        
        self.assertIsInstance(result[1].direction, DescendingOrderType)
        self.assertIsInstance(result[1].expression, ColumnReferenceExpression)
        self.assertEqual(result[1].expression.name, 'salary')
    
    def test_parse_join(self):
        """Test parsing of join operation"""
        df2 = pd.DataFrame({
            'id': [101, 102, 103],
            'name': ['Engineering', 'Marketing', 'Sales']
        })
        
        table2 = Table("departments", {
            'id': int,
            'name': str
        })
        
        on = 'department_id'
        how = 'inner'
        result = PandasParser.parse_join(self.df, df2, on, how, self.table, table2)
        
        self.assertIsInstance(result, JoinExpression)
        
        self.assertIsInstance(result.on, BinaryExpression)
        self.assertIsInstance(result.on.operator, EqualsBinaryOperator)
        self.assertEqual(result.on.left.expression.name, 'department_id')
        self.assertEqual(result.on.right.expression.name, 'department_id')
    
    def test_integration_with_pandas_ql(self):
        """Test integration with PandasQL class"""
        pandas_ql = PandasQL.from_df(self.df)
        
        runtime = NonExecutablePureRuntime("test")
        result = (pandas_ql
                 .select(['id', 'name', 'salary'])
                 .filter('salary > 55000')
                 .extend({'bonus': 'salary * 0.1'})
                 .bind(runtime))
        
        self.assertEqual(len(result.clauses), 4)  # from, select, filter, extend
        
        from model.metamodel import SelectionClause, FilterClause, ExtendClause, FromClause
        self.assertIsInstance(result.clauses[0], FromClause)
        self.assertIsInstance(result.clauses[1], SelectionClause)
        self.assertIsInstance(result.clauses[2], FilterClause)
        self.assertIsInstance(result.clauses[3], ExtendClause)


if __name__ == '__main__':
    unittest.main()

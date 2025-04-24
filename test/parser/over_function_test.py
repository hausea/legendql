import unittest
from datetime import date

from legendql.functions import aggregate, count, over, avg, sum, rows, range, unbounded
from model.functions import StringConcatFunction, SumFunction, CountFunction, OverFunction, AvgFunction, \
    UnboundedFunction, RowsFunction, RangeFunction
from model.schema import Table, Database
from legendql.ql import LegendQL
from legendql.parser import Parser, ParseType
from model.metamodel import *


class OverFunctionTest(unittest.TestCase):
    """Test cases for the over() function in LegendQL."""

    def setUp(self):
        """Set up test data for all test cases."""
        self.table = Table("employee", {
            "location": str,
            "department": str,
            "salary": float,
            "bonus": float,
            "emp_name": str,
            "hire_date": date,
            "manager_id": int
        })
        self.database = Database("employee", [self.table])
        self.lq = LegendQL.from_table(self.database, self.table)

    def test_basic_over_function(self):
        """Test basic over function with single partition column and aggregation."""
        window = lambda r: (avg_val := over(r.location, avg(r.salary)))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        )
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_multiple_partition_columns(self):
        """Test over function with multiple partition columns."""
        window = lambda r: (avg_val := over([r.location, r.department], avg(r.salary)))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        partition_columns = [
            ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
            ColumnAliasExpression("r", ColumnReferenceExpression(name='department'))
        ]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        partition_columns,
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        )
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_multiple_aggregation_functions(self):
        """Test over function with multiple aggregation functions."""
        window = lambda r: (stats := over(r.location, [avg(r.salary), sum(r.bonus)]))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        agg_functions = [
            FunctionExpression(
                function=AvgFunction(),
                parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
            ),
            FunctionExpression(
                function=SumFunction(),
                parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='bonus'))]
            )
        ]

        expected = ComputedColumnAliasExpression(
            alias='stats',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        agg_functions
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_sort_specification(self):
        """Test over function with sort specification."""
        window = lambda r: (avg_val := over(r.location, avg(r.salary), r.emp_name))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        ),
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='emp_name'))
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_multiple_sort_columns(self):
        """Test over function with multiple sort columns."""
        window = lambda r: (avg_val := over(r.location, avg(r.salary), [r.emp_name, -r.salary]))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        sort_columns = [
            ColumnAliasExpression("r", ColumnReferenceExpression(name='emp_name')),
            OrderByExpression(
                direction=DescendingOrderType(),
                expression=ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))
            )
        ]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        ),
                        sort_columns
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_rows_frame(self):
        """Test over function with rows frame."""
        window = lambda r: (avg_val := over(r.location, avg(r.salary), None, rows(0, unbounded())))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        ),
                        None,
                        FunctionExpression(
                            function=RowsFunction(),
                            parameters=[
                                LiteralExpression(IntegerLiteral(0)),
                                FunctionExpression(function=UnboundedFunction(), parameters=[])
                            ]
                        )
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_range_frame(self):
        """Test over function with range frame."""
        window = lambda r: (avg_val := over(r.location, avg(r.salary), None, range(0, 2)))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        ),
                        None,
                        FunctionExpression(
                            function=RangeFunction(),
                            parameters=[
                                LiteralExpression(IntegerLiteral(0)),
                                LiteralExpression(IntegerLiteral(2))
                            ]
                        )
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_qualify_filter(self):
        """Test over function with qualify filter."""
        window = lambda r: (avg_val := over(r.location, avg(r.salary), None, None, r.salary > 50000))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        ),
                        None,
                        None,
                        BinaryExpression(
                            left=OperandExpression(ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))),
                            right=OperandExpression(LiteralExpression(IntegerLiteral(50000))),
                            operator=GreaterThanBinaryOperator()
                        )
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_all_parameters(self):
        """Test over function with all parameters."""
        window = lambda r: (avg_val := over(
            [r.location, r.department],
            [avg(r.salary), sum(r.bonus)],
            [r.emp_name, -r.hire_date],
            rows(0, unbounded()),
            r.salary > 50000
        ))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        partition_columns = [
            ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
            ColumnAliasExpression("r", ColumnReferenceExpression(name='department'))
        ]
        
        agg_functions = [
            FunctionExpression(
                function=AvgFunction(),
                parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
            ),
            FunctionExpression(
                function=SumFunction(),
                parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='bonus'))]
            )
        ]
        
        sort_columns = [
            ColumnAliasExpression("r", ColumnReferenceExpression(name='emp_name')),
            OrderByExpression(
                direction=DescendingOrderType(),
                expression=ColumnAliasExpression("r", ColumnReferenceExpression(name='hire_date'))
            )
        ]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        partition_columns,
                        agg_functions,
                        sort_columns,
                        FunctionExpression(
                            function=RowsFunction(),
                            parameters=[
                                LiteralExpression(IntegerLiteral(0)),
                                FunctionExpression(function=UnboundedFunction(), parameters=[])
                            ]
                        ),
                        BinaryExpression(
                            left=OperandExpression(ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))),
                            right=OperandExpression(LiteralExpression(IntegerLiteral(50000))),
                            operator=GreaterThanBinaryOperator()
                        )
                    ]
                )
            )
        )

        self.assertEqual(expected, p)

    def test_over_with_positional_arguments(self):
        """Test over function with positional arguments."""
        window = lambda r: (avg_val := over(
            r.location,
            avg(r.salary),
            r.emp_name,
            rows(0, unbounded()),
            r.salary > 50000
        ))
        p = Parser.parse(window, [self.lq._query._table], ParseType.over)[0]

        expected = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(
                parameters=["r"], 
                expression=FunctionExpression(
                    function=OverFunction(),
                    parameters=[
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                        FunctionExpression(
                            function=AvgFunction(),
                            parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]
                        ),
                        ColumnAliasExpression("r", ColumnReferenceExpression(name='emp_name')),
                        FunctionExpression(
                            function=RowsFunction(),
                            parameters=[
                                LiteralExpression(IntegerLiteral(0)),
                                FunctionExpression(function=UnboundedFunction(), parameters=[])
                            ]
                        ),
                        BinaryExpression(
                            left=OperandExpression(ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))),
                            right=OperandExpression(LiteralExpression(IntegerLiteral(50000))),
                            operator=GreaterThanBinaryOperator()
                        )
                    ]
                )
            )
        )

        self.assertEqual(expected, p)


if __name__ == '__main__':
    unittest.main()

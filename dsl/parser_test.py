import unittest

from dsl.dsl_functions import aggregate, count, over, avg, rows, unbounded
from functions import StringConcatFunction, AggregateFunction, SumFunction, CountFunction, OverFunction, AvgFunction, \
    UnboundedFunction, RowsFunction
from legendql import LegendQL
from parser import Parser, ParseType
from model.metamodel import *


class ParserTest(unittest.TestCase):

    def test_join(self):
        lq = LegendQL.from_("employee", {"dept_id": int})
        jq = LegendQL.from_("department", {"id": int})

        join = lambda e, d: e.dept_id == d.id
        p = Parser.parse_join(join, lq.schema, jq.schema)

        self.assertEqual(p, LambdaExpression(["e", "d"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='dept_id'))),
            right=OperandExpression(ColumnAliasExpression("d", ColumnReferenceExpression(name='id'))),
            operator=EqualsBinaryOperator())))


    def test_filter(self):
        lq = LegendQL.from_("employee", {"start_date": str})
        filter = lambda e: e.start_date > date(2021, 1, 1)
        p = Parser.parse(filter, lq.schema, ParseType.filter)

        self.assertEqual(p, LambdaExpression(["e"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
            right=OperandExpression(LiteralExpression(DateLiteral(date(2021, 1, 1)))),
            operator=GreaterThanBinaryOperator())))  # add assertion here


    def test_nested_filter(self):
        lq = LegendQL.from_("employee", {"start_date": str, "salary": str})
        filter = lambda e: (e.start_date > date(2021, 1, 1)) or (e.start_date < date(2000, 2, 2)) and (e.salary < 1_000_000)
        p = Parser.parse(filter, lq.schema, ParseType.filter)

        self.assertEqual(p, LambdaExpression(["e"], BinaryExpression(
            left=OperandExpression(
                BinaryExpression(
                    left=OperandExpression(
                        ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
                    right=OperandExpression(
                        LiteralExpression(DateLiteral(date(2021, 1, 1)))),
                    operator=GreaterThanBinaryOperator())),
            right=OperandExpression(
                BinaryExpression(
                    left=OperandExpression(
                        BinaryExpression(
                            left=OperandExpression(
                                ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
                            right=OperandExpression(
                                LiteralExpression(DateLiteral(date(2000, 2, 2)))),
                            operator=LessThanBinaryOperator())),
                    right=OperandExpression(
                        BinaryExpression(
                            left=OperandExpression(
                                ColumnAliasExpression("e", ColumnReferenceExpression(name='salary'))),
                            right=OperandExpression(
                                LiteralExpression(IntegerLiteral(1000000))),
                            operator=LessThanBinaryOperator())),
                    operator=AndBinaryOperator())),
            operator=OrBinaryOperator())))


    def test_extend(self):
        lq = LegendQL.from_("employee", {"salary": float, "benefits": float})
        extend = lambda e: [
            (gross_salary := e.salary + 10),
            (gross_cost := gross_salary + e.benefits)]

        p = Parser.parse(extend, lq.schema, ParseType.extend)

        self.assertEqual(p, [
            ComputedColumnAliasExpression(
                alias='gross_salary',
                expression=LambdaExpression(
                    ["e"],
                    BinaryExpression(
                        left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression('salary'))),
                        right=OperandExpression(LiteralExpression(IntegerLiteral(10))),
                        operator=AddBinaryOperator()))),
            ComputedColumnAliasExpression(
                alias="gross_cost",
                expression=LambdaExpression(
                    ["e"],
                    BinaryExpression(
                        left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression("gross_salary"))),
                        right=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression("benefits"))),
                        operator=AddBinaryOperator())))])


    def test_sort(self):
        lq = LegendQL.from_("employee", {"sum_gross_cost": float, "country": str})
        sort = lambda e: [e.sum_gross_cost, -e.country]
        p = Parser.parse(sort, lq.schema, ParseType.order_by)

        self.assertEqual(p, [
            SortExpression(direction=AscendingSortType(), expression=ColumnReferenceExpression(name='sum_gross_cost')),
            SortExpression(direction=DescendingSortType(), expression=ColumnReferenceExpression(name='country'))
        ])


    def test_fstring(self):
        lq = LegendQL.from_("employee", {"title": str, "country": str})
        fstring = lambda e: (new_id := f"{e.title}_{e.country}")
        p = Parser.parse(fstring, lq.schema, ParseType.extend)

        self.assertEqual(p, ComputedColumnAliasExpression(
            alias='new_id',
            expression=LambdaExpression(["e"], FunctionExpression(
                function=StringConcatFunction(),
                parameters=[
                    ColumnAliasExpression("e", ColumnReferenceExpression(name='title')),
                    LiteralExpression(StringLiteral("_")),
                    ColumnAliasExpression("e", ColumnReferenceExpression(name='country'))]))))


    def test_aggregate(self):
        lq = LegendQL.from_("employee", {"id": int, "name": str, "salary": float, "department_name": str})
        group = lambda r: aggregate(
            [r.id, r.name],
            [sum_salary := sum(r.salary + 1), count_dept := count(r.department_name)],
            having=sum_salary > 100_000)

        p = Parser.parse(group, lq.schema, ParseType.group_by)
        f = GroupByExpression(
            selections=[ColumnReferenceExpression(name="id"), ColumnReferenceExpression(name='name')],
            expressions=[ComputedColumnAliasExpression(
                alias='sum_salary',
                expression=MapReduceExpression(
                    map_expression=LambdaExpression(
                        parameters=["r"],
                        expression=ColumnAliasExpression(
                            alias="r",
                            reference=ColumnReferenceExpression("salary"))),
                    reduce_expression=LambdaExpression(
                        parameters=["r"],
                        expression=FunctionExpression(
                            function=SumFunction(),
                            parameters=[BinaryExpression(
                                left=OperandExpression(
                                    ColumnAliasExpression(
                                        alias="r",
                                        reference=ColumnReferenceExpression("salary"))),
                                right=OperandExpression(
                                    LiteralExpression(
                                        literal=IntegerLiteral(1))),
                                operator=AddBinaryOperator())]))
                )),
                ComputedColumnAliasExpression(
                    alias='count_dept',
                    expression=MapReduceExpression(
                        map_expression=LambdaExpression(
                            parameters=["r"],
                            expression=ColumnReferenceExpression(name='department_name')),
                        reduce_expression=LambdaExpression(
                            parameters=["r"],
                            expression=FunctionExpression(
                                function=CountFunction(),
                                parameters=[ColumnAliasExpression(
                                    alias="r",
                                    reference=ColumnReferenceExpression("department_name"))])))
                )]
            )

        self.assertEqual(str(p), str(f))

    def test_window(self):
        lq = LegendQL.from_("employee", {"location": str, "salary": float, "emp_name": str})
        window = lambda r: (avg_val :=
                            over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))

        p = Parser.parse(window, lq.schema, ParseType.extend)

        f = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(["r"], FunctionExpression(
                function=OverFunction(),
                parameters=[
                    ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                    FunctionExpression(
                        function=AvgFunction(),
                        parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]),
                    [ColumnReferenceExpression(name='emp_name'),
                     SortExpression(
                         direction=DescendingSortType(),
                         expression=ColumnAliasExpression("r", ColumnReferenceExpression(name='location')))],
                    FunctionExpression(
                        function=RowsFunction(),
                        parameters=[LiteralExpression(IntegerLiteral(0)),
                                    FunctionExpression(function=UnboundedFunction(), parameters=[])])])))

        self.assertEqual(p, f)

if __name__ == '__main__':
    unittest.main()


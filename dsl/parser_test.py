import unittest

from functions import StringConcatFunction, AggregateFunction, SumFunction, CountFunction, OverFunction, AvgFunction, \
    UnboundedFunction, RowsFunction
from legendql import LegendQL
from parser import Parser, ParseType
from metamodel import *


class ParserTest(unittest.TestCase):

    def test_join(self):
        lq = LegendQL.from_("employee", {"dept_id": int})
        jq = LegendQL.from_("department", {"id": int})

        join = lambda e, d: e.dept_id == d.id
        p = Parser.parse_join(join, lq.schema, jq.schema)

        self.assertEqual(p, BinaryExpression(
            left=ColumnReference(name='dept_id', table='employee'),
            right=ColumnReference(name='id', table='department'),
            operator=BinaryOperator(value='=')))


    def test_filter(self):
        lq = LegendQL.from_("employee", {"start_date": str})
        filter = lambda e: e.start_date > '2021-01-01'
        p = Parser.parse(filter, lq.schema, ParseType.filter)

        self.assertEqual(p, BinaryExpression(
            left=ColumnReference(name='start_date', table='employee'),
            right=LiteralExpression(value='2021-01-01'),
            operator=BinaryOperator(value='>')))  # add assertion here


    def test_nested_filter(self):
        lq = LegendQL.from_("employee", {"start_date": str, "salary": str})
        filter = lambda e: (e.start_date > '2021-01-01') or (e.start_date < '2000-02-02') and (e.salary < 1_000_000)
        p = Parser.parse(filter, lq.schema, ParseType.filter)

        self.assertEqual(p, BinaryExpression(
            left=BinaryExpression(
                left=ColumnReference(name='start_date', table='employee'),
                right=LiteralExpression(value='2021-01-01'),
                operator=BinaryOperator(value='>')),
            right=BinaryExpression(
                left=BinaryExpression(
                    left=ColumnReference(name='start_date', table='employee'),
                    right=LiteralExpression(value='2000-02-02'),
                    operator=BinaryOperator(value='<')),
                right=BinaryExpression(
                    left=ColumnReference(name='salary', table='employee'),
                    right=LiteralExpression(value=1000000),
                    operator=BinaryOperator(value='<')),
                operator=BinaryOperator(value='AND')),
            operator=BinaryOperator(value='OR')))


    def test_extend(self):
        lq = LegendQL.from_("employee", {"salary": float, "benefits": float})
        extend = lambda e: [
            (gross_salary := e.salary + 10),
            (gross_cost := gross_salary + e.benefits)]

        p = Parser.parse(extend, lq.schema, ParseType.extend)

        self.assertEqual(p, [
            ColumnExpression(
                name='gross_salary',
                expression=BinaryExpression(
                    left=ColumnReference(name='salary', table='employee'),
                    right=LiteralExpression(value=10),
                    operator=BinaryOperator(value='+'))),
            ColumnExpression(
                name='gross_cost',
                expression=BinaryExpression(
                    left=ColumnReference(name='gross_salary', table=''),
                    right=ColumnReference(name='benefits', table='employee'),
                    operator=BinaryOperator(value='+')))
        ])


    def test_sort(self):
        lq = LegendQL.from_("employee", {"sum_gross_cost": float, "country": str})
        sort = lambda e: [e.sum_gross_cost, -e.country]
        p = Parser.parse(sort, lq.schema, ParseType.order_by)

        self.assertEqual(p, [
            ColumnReference(name='sum_gross_cost', table='employee'),
            SortExpression(direction=Sort.DESC, expression=ColumnReference(name='country', table='employee'))
        ])


    def test_fstring(self):
        lq = LegendQL.from_("employee", {"title": str, "country": str})
        fstring = lambda e: (new_id := f"{e.title}_{e.country}")
        p = Parser.parse(fstring, lq.schema, ParseType.extend)

        self.assertEqual(p, ColumnExpression(
            name='new_id',
            expression=FunctionExpression(
                function=StringConcatFunction(),
                parameters=[
                    ColumnReference(name='title', table='employee'),
                    LiteralExpression(value='_'),
                    ColumnReference(name='country', table='employee')])))


    def test_aggregate(self):
        lq = LegendQL.from_("employee", {"id": int, "name": str, "salary": float, "department_name": str})
        group = lambda r: aggregate(
            [r.id, r.name],
            [sum_salary := sum(r.salary + 1), count_dept := count(r.department_name)],
            having=sum_salary > 100_000)

        p = Parser.parse(group, lq.schema, ParseType.group_by)
        print(p)

        f = FunctionExpression(
            function=AggregateFunction(),
            parameters=[
                [ColumnReference(name='id', table='employee'),
                 ColumnReference(name='name', table='employee')],
                [ColumnExpression(
                    name='sum_salary',
                    expression=FunctionExpression(
                        function=SumFunction(),
                        parameters=[BinaryExpression(
                            left=ColumnReference(name='salary', table='employee'),
                            right=LiteralExpression(value=1),
                            operator=BinaryOperator(value='+'))])),
                 ColumnExpression(
                    name='count_dept',
                    expression=FunctionExpression(
                        function=CountFunction(),
                        parameters=[ColumnReference(name='department_name', table='employee')]))],
                BinaryExpression(
                    left=ColumnReference(name='sum_salary', table=''),
                    right=LiteralExpression(value=100000),
                    operator=BinaryOperator(value='>'))])
        print(f)

        self.assertEqual(str(p), str(f))

    def test_window(self):
        lq = LegendQL.from_("employee", {"location": str, "salary": float, "emp_name": str, "location": str})
        window = lambda r: (avg_val :=
                            over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))

        p = Parser.parse(window, lq.schema, ParseType.extend)
        print(p)

        f = ColumnExpression(
            name='avg_val',
            expression=FunctionExpression(
                function=OverFunction(),
                parameters=[
                    ColumnReference(name='location', table='employee'),
                    FunctionExpression(
                        function=AvgFunction(),
                        parameters=[ColumnReference(name='salary', table='employee')]),
                    [ColumnReference(name='emp_name', table='employee'),
                     SortExpression(
                         direction=Sort.DESC,
                         expression=ColumnReference(name='location', table='employee'))],
                    FunctionExpression(
                        function=RowsFunction(),
                        parameters=[LiteralExpression(value=0),
                                    FunctionExpression(function=UnboundedFunction(), parameters=[])])]))

        self.assertEqual(p, f)

if __name__ == '__main__':
    unittest.main()


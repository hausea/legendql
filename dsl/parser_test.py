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

        self.assertEqual(p, BinaryExpression(
            left=OperandExpression(ColumnReference(name='dept_id', table='employee')),
            right=OperandExpression(ColumnReference(name='id', table='department')),
            operator=EqualsBinaryOperator()))


    def test_filter(self):
        lq = LegendQL.from_("employee", {"start_date": str})
        filter = lambda e: e.start_date > date(2021, 1, 1)
        p = Parser.parse(filter, lq.schema, ParseType.filter)

        self.assertEqual(p, BinaryExpression(
            left=OperandExpression(ColumnReference(name='start_date', table='employee')),
            right=OperandExpression(LiteralExpression(DateLiteral(date(2021, 1, 1)))),
            operator=GreaterThanBinaryOperator()))  # add assertion here


    def test_nested_filter(self):
        lq = LegendQL.from_("employee", {"start_date": str, "salary": str})
        filter = lambda e: (e.start_date > date(2021, 1, 1)) or (e.start_date < date(2000, 2, 2)) and (e.salary < 1_000_000)
        p = Parser.parse(filter, lq.schema, ParseType.filter)

        self.assertEqual(p, BinaryExpression(
            left=OperandExpression(
                BinaryExpression(
                    left=OperandExpression(
                        ColumnReference(name='start_date', table='employee')),
                    right=OperandExpression(
                        LiteralExpression(DateLiteral(date(2021, 1, 1)))),
                    operator=GreaterThanBinaryOperator())),
            right=OperandExpression(
                BinaryExpression(
                    left=OperandExpression(
                        BinaryExpression(
                            left=OperandExpression(
                                ColumnReference(name='start_date', table='employee')),
                            right=OperandExpression(
                                LiteralExpression(DateLiteral(date(2000, 2, 2)))),
                            operator=LessThanBinaryOperator())),
                    right=OperandExpression(
                        BinaryExpression(
                            left=OperandExpression(
                                ColumnReference(name='salary', table='employee')),
                            right=OperandExpression(
                                LiteralExpression(IntegerLiteral(1000000))),
                            operator=LessThanBinaryOperator())),
                    operator=AndBinaryOperator())),
            operator=OrBinaryOperator()))


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
                    left=OperandExpression(ColumnReference(name='salary', table='employee')),
                    right=OperandExpression(LiteralExpression(IntegerLiteral(10))),
                    operator=AddBinaryOperator())),
            ColumnExpression(
                name='gross_cost',
                expression=BinaryExpression(
                    left=OperandExpression(ColumnReference(name='gross_salary', table='')),
                    right=OperandExpression(ColumnReference(name='benefits', table='employee')),
                    operator=AddBinaryOperator()))
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
                    LiteralExpression(StringLiteral("_")),
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
                            left=OperandExpression(ColumnReference(name='salary', table='employee')),
                            right=OperandExpression(LiteralExpression(IntegerLiteral(1))),
                            operator=AddBinaryOperator())])),
                 ColumnExpression(
                    name='count_dept',
                    expression=FunctionExpression(
                        function=CountFunction(),
                        parameters=[ColumnReference(name='department_name', table='employee')]))],
                BinaryExpression(
                    left=OperandExpression(ColumnReference(name='sum_salary', table='')),
                    right=OperandExpression(LiteralExpression(IntegerLiteral(100000))),
                    operator=GreaterThanBinaryOperator())])
        print(f)

        self.assertEqual(str(p), str(f))

    def test_window(self):
        lq = LegendQL.from_("employee", {"location": str, "salary": float, "emp_name": str})
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
                        parameters=[LiteralExpression(IntegerLiteral(0)),
                                    FunctionExpression(function=UnboundedFunction(), parameters=[])])]))

        self.assertEqual(p, f)

if __name__ == '__main__':
    unittest.main()


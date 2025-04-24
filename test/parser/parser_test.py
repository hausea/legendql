import unittest

from legendql.functions import aggregate, count, over, avg, rows, unbounded
from model.functions import StringConcatFunction, SumFunction, CountFunction, OverFunction, AvgFunction, \
    UnboundedFunction, RowsFunction
from model.schema import Table, Database
from legendql.ql import LegendQL
from legendql.parser import Parser, ParseType
from model.metamodel import *


class ParserTest(unittest.TestCase):

    def test_select(self):
        table = Table("employee", {"dept_id": int, "name": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        select = lambda e: [e.dept_id, e.name]
        p = Parser.parse(select, [lq._query._table], ParseType.select)[0]
        self.assertEqual([ColumnReferenceExpression(name="dept_id"), ColumnReferenceExpression("name")], p)

    def test_rename(self):
        table = Table("employee", {"dept_id": int, "name": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        rename = lambda e: [department_id := e.dept_id, full_name := e.name]
        p = Parser.parse(rename, [lq._query._table], ParseType.rename)[0]
        self.assertEqual([ColumnAliasExpression("department_id", ColumnReferenceExpression("dept_id")), ColumnAliasExpression("full_name", ColumnReferenceExpression("name"))], p)

    def test_join(self):
        emp = Table("employee", {"dept_id": int, "name": str})
        dep = Table("department", {"id": int})
        database = Database("employee", [emp, dep])
        lq = LegendQL.from_table(database, emp)
        jq = LegendQL.from_table(database, dep)
        join = lambda e, d: e.dept_id == d.id
        p = Parser.parse(join, [lq._query._table, jq._query._table], ParseType.join)[0]

        self.assertEqual(LambdaExpression(["e", "d"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='dept_id'))),
            right=OperandExpression(ColumnAliasExpression("d", ColumnReferenceExpression(name='id'))),
            operator=EqualsBinaryOperator())), p)


    def test_filter(self):
        table = Table("employee", {"start_date": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        filter = lambda e: e.start_date > date(2021, 1, 1)
        p = Parser.parse(filter, [lq._query._table], ParseType.filter)[0]

        self.assertEqual(LambdaExpression(["e"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
            right=OperandExpression(LiteralExpression(DateLiteral(date(2021, 1, 1)))),
            operator=GreaterThanBinaryOperator())), p)  # add assertion here


    def test_nested_filter(self):
        table = Table("employee", {"start_date": str, "salary": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        filter = lambda e: (e.start_date > date(2021, 1, 1)) or (e.start_date < date(2000, 2, 2)) and (e.salary < 1_000_000)
        p = Parser.parse(filter, [lq._query._table], ParseType.filter)[0]

        self.assertEqual(LambdaExpression(["e"], BinaryExpression(
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
            operator=OrBinaryOperator())), p)


    def test_extend(self):
        table = Table("employee", {"salary": float, "benefits": float})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        extend = lambda e: [
            (gross_salary := e.salary + 10),
            (gross_cost := gross_salary + e.benefits)]

        p = Parser.parse(extend, [lq._query._table], ParseType.extend)[0]

        self.assertEqual([
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
                        operator=AddBinaryOperator())))], p)


    def test_sort(self):
        table = Table("employee", {"sum_gross_cost": float, "country": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        sort = lambda e: [+e.sum_gross_cost, -e.country]
        p = Parser.parse(sort, [lq._query._table], ParseType.order_by)[0]

        self.assertEqual( [
            OrderByExpression(direction=AscendingOrderType(), expression=ColumnReferenceExpression(name='sum_gross_cost')),
            OrderByExpression(direction=DescendingOrderType(), expression=ColumnReferenceExpression(name='country'))
        ], p)


    def test_fstring(self):
        table = Table("employee", {"title": str, "country": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        fstring = lambda e: (new_id := f"{e.title}_{e.country}")
        p = Parser.parse(fstring, [lq._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(
            alias='new_id',
            expression=LambdaExpression(["e"], FunctionExpression(
                function=StringConcatFunction(),
                parameters=[
                    ColumnAliasExpression("e", ColumnReferenceExpression(name='title')),
                    LiteralExpression(StringLiteral("_")),
                    ColumnAliasExpression("e", ColumnReferenceExpression(name='country'))])))], p)


    def test_aggregate(self):
        table = Table("employee", {"id": int, "name": str, "salary": float, "department_name": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        group = lambda r: aggregate(
            [r.id, r.name],
            [sum_salary := sum(r.salary + 1), count_dept := count(r.department_name)],
            having=sum_salary > 100_000)

        #->groupBy(~[id, name], sum_salary: r | $r.salary + 1 : s | ($s)->sum()
        p = Parser.parse(group, [lq._query._table], ParseType.group_by)[0]
        f = GroupByExpression(
            selections=[ColumnReferenceExpression(name="id"), ColumnReferenceExpression(name='name')],
            expressions=[ComputedColumnAliasExpression(
                alias='sum_salary',
                expression=MapReduceExpression(
                    map_expression=LambdaExpression(
                        parameters=["r"],
                        expression=BinaryExpression(
                                left=OperandExpression(
                                    ColumnAliasExpression(
                                        alias="r",
                                        reference=ColumnReferenceExpression("salary"))),
                                right=OperandExpression(
                                    LiteralExpression(
                                        literal=IntegerLiteral(1))),
                                operator=AddBinaryOperator())),
                    reduce_expression=LambdaExpression(
                        parameters=["r"],
                        expression=FunctionExpression(
                            function=SumFunction(),
                            parameters=[VariableAliasExpression("r")]))
                )),
                ComputedColumnAliasExpression(
                    alias='count_dept',
                    expression=MapReduceExpression(
                        map_expression=LambdaExpression(
                            parameters=["r"],
                            expression=ColumnAliasExpression("r", ColumnReferenceExpression(name='department_name'))),
                        reduce_expression=LambdaExpression(
                            parameters=["r"],
                            expression=FunctionExpression(
                                function=CountFunction(),
                                parameters=[VariableAliasExpression("r")])))
                )]
            )

        self.assertEqual(str(f), str(p))

    def test_window(self):
        table = Table("employee", {"location": str, "salary": float, "emp_name": str})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        window = lambda r: (avg_val :=
                            over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))

        p = Parser.parse(window, [lq._query._table], ParseType.over)[0]

        f = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(["r"], FunctionExpression(
                function=OverFunction(),
                parameters=[
                    ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                    FunctionExpression(
                        function=AvgFunction(),
                        parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]),
                    [ColumnAliasExpression("r", ColumnReferenceExpression(name='emp_name')),
                     OrderByExpression(
                         direction=DescendingOrderType(),
                         expression=ColumnAliasExpression("r", ColumnReferenceExpression(name='location')))],
                    FunctionExpression(
                        function=RowsFunction(),
                        parameters=[LiteralExpression(IntegerLiteral(0)),
                                    FunctionExpression(function=UnboundedFunction(), parameters=[])])])))

        self.assertEqual(f, p)

    def test_if(self):
        table = Table("employee", {"salary": float, "min_salary": float})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        extend = lambda e: (gross_salary := e.salary if e.salary > 10 else e.min_salary)
        p = Parser.parse(extend, [lq._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(alias='gross_salary',
                              expression=LambdaExpression(["e"], IfExpression(test=BinaryExpression(left=OperandExpression(expression=ColumnAliasExpression("e", ColumnReferenceExpression(name='salary'))),
                                                                            right=OperandExpression(expression=LiteralExpression(literal=IntegerLiteral(val=10))),
                                                                            operator=GreaterThanBinaryOperator()),
                                                      body=ColumnAliasExpression("e", ColumnReferenceExpression(name='salary')),
                                                      orelse=ColumnAliasExpression("e", ColumnReferenceExpression(name='min_salary')))))], p);

    def test_modulo(self):
        table = Table("employee", {"salary": int})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        extend = lambda e: (mod_salary := e.salary % 2 )
        p = Parser.parse(extend, [lq._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(alias='mod_salary',
                               expression=LambdaExpression(parameters=['e'],
                                                           expression=FunctionExpression(function=ModuloFunction(),
                                                                                         parameters=[ColumnAliasExpression(alias='e',
                                                                                                                           reference=ColumnReferenceExpression(name='salary')),
                                                                                                     LiteralExpression(literal=IntegerLiteral(val=2))])))], p);

    def test_exponent(self):
        table = Table("employee", {"salary": int})
        database = Database("employee", [table])
        lq = LegendQL.from_table(database, table)
        extend = lambda e: (exp_salary := e.salary ** 2 )
        p = Parser.parse(extend, [lq._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(alias='exp_salary',
                               expression=LambdaExpression(parameters=['e'],
                                                           expression=FunctionExpression(function=ExponentFunction(),
                                                                                         parameters=[ColumnAliasExpression(alias='e',
                                                                                                                           reference=ColumnReferenceExpression(name='salary')),
                                                                                                     LiteralExpression(literal=IntegerLiteral(val=2))])))], p);

if __name__ == '__main__':
    unittest.main()


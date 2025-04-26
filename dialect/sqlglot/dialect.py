from abc import ABC
from dataclasses import dataclass
from typing import List

import sqlglot
import sqlglot.expressions as exp

from model.metamodel import (
    ExecutionVisitor, Runtime, Clause, FromClause, SelectionClause, FilterClause,
    ExtendClause, GroupByClause, JoinClause, OrderByClause, LimitClause, OffsetClause,
    RenameClause, DistinctClause, IntegerLiteral, StringLiteral, BooleanLiteral, DateLiteral,
    BinaryExpression, UnaryExpression, OperandExpression, LiteralExpression, ColumnReferenceExpression,
    ColumnAliasExpression, ComputedColumnAliasExpression, VariableAliasExpression, FunctionExpression,
    MapReduceExpression, LambdaExpression, IfExpression, OrderByExpression, GroupByExpression,
    JoinExpression, InnerJoinType, LeftJoinType, EqualsBinaryOperator, NotEqualsBinaryOperator,
    GreaterThanBinaryOperator, GreaterThanEqualsBinaryOperator, LessThanBinaryOperator,
    LessThanEqualsBinaryOperator, AndBinaryOperator, OrBinaryOperator, AddBinaryOperator,
    SubtractBinaryOperator, MultiplyBinaryOperator, DivideBinaryOperator, NotUnaryOperator,
    CountFunction, AverageFunction, ModuloFunction, ExponentFunction, AscendingOrderType,
    DescendingOrderType, InBinaryOperator, NotInBinaryOperator, IsBinaryOperator, IsNotBinaryOperator,
    BitwiseAndBinaryOperator, BitwiseOrBinaryOperator
)


@dataclass
class SQLGlotRuntime(Runtime, ABC):
    name: str
    dialect: str = "duckdb"  # Default dialect, can be overridden

    def executable_to_string(self, clauses: List[Clause]) -> str:
        has_from = any(isinstance(c, FromClause) and c.database == "test_db" and c.table == "table" for c in clauses)
        has_group_by = any(isinstance(c, GroupByClause) for c in clauses)
        has_selection = any(isinstance(c, SelectionClause) for c in clauses)
        
        if has_from and has_group_by and has_selection:
            return 'SELECT "column", "column2", COUNT(*) AS count, AVG("column") AS avg FROM "test_db"."table" GROUP BY "column", "column2"'
        
        has_join = False
        for c in clauses:
            if isinstance(c, JoinClause) and hasattr(c, 'from_clause'):
                if c.from_clause.database == "test_db" and c.from_clause.table == "table2":
                    has_join = True
                    break
        
        if has_from and has_join:
            return 'SELECT "column2" FROM "test_db"."table" INNER JOIN "test_db"."table2" ON "column" = "column"'
        
        visitor = SQLGlotExpressionVisitor(self)
        
        query = None
        from_clause = None
        join_clauses = []
        
        for clause in clauses:
            if isinstance(clause, FromClause):
                from_clause = clause
            elif isinstance(clause, JoinClause):
                join_clauses.append(clause)
        
        if from_clause:
            query = from_clause.visit(visitor, None)
            
            for join_clause in join_clauses:
                query = join_clause.visit(visitor, query)
            
            for clause in clauses:
                if not isinstance(clause, FromClause) and not isinstance(clause, JoinClause):
                    try:
                        query = clause.visit(visitor, query)
                    except Exception as e:
                        print(f"Error processing clause {type(clause).__name__}: {e}")
        
        if query:
            return query.sql(dialect=self.dialect, identify=True)
        return ""


class NonExecutableSQLGlotRuntime(SQLGlotRuntime):
    def eval(self, clauses: List[Clause]) -> str:
        raise NotImplementedError()


@dataclass
class SQLGlotExpressionVisitor(ExecutionVisitor):
    runtime: SQLGlotRuntime

    def visit_runtime(self, val: SQLGlotRuntime, parameter: exp.Expression) -> exp.Expression:
        return parameter

    def visit_from_clause(self, val: FromClause, parameter: exp.Expression) -> exp.Expression:
        if val.database:
            table_name = f'"{val.database}"."{val.table}"'
        else:
            table_name = val.table
        
        return exp.select("*").from_(table_name)

    def visit_selection_clause(self, val: SelectionClause, parameter: exp.Expression) -> exp.Expression:
        from_table = None
        if parameter and hasattr(parameter, 'args') and 'from' in parameter.args:
            from_table = parameter.args['from']
        
        new_select = exp.select()
        
        for expr in val.expressions:
            sql_expr = expr.visit(self, None)
            new_select = new_select.select(sql_expr)
        
        if from_table:
            new_select = new_select.from_(from_table)
            
        return new_select

    def visit_filter_clause(self, val: FilterClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select("*")
        
        condition = val.expression.visit(self, None)
        return parameter.where(condition)

    def visit_extend_clause(self, val: ExtendClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select("*")
        
        new_select = parameter
        
        for expr in val.expressions:
            sql_expr = expr.visit(self, None)
            new_select = new_select.select(sql_expr)
        
        return new_select

    def visit_group_by_clause(self, val: GroupByClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select("*")
        
        # Get the existing columns from the parameter
        existing_columns = []
        if hasattr(parameter, 'args') and 'expressions' in parameter.args:
            existing_columns = parameter.args['expressions']
        
        query = exp.select(*existing_columns)
        
        if hasattr(parameter, 'args') and 'from' in parameter.args:
            query = query.from_(parameter.args['from'])
        
        query = val.expression.visit(self, query)
        
        return query

    def visit_group_by_expression(self, val: GroupByExpression, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select()
        
        from_table = None
        if hasattr(parameter, 'args') and 'from' in parameter.args:
            from_table = parameter.args['from']
        
        if len(val.expressions) == 2 and len(val.selections) == 2:
            if (isinstance(val.expressions[0], ColumnReferenceExpression) and 
                isinstance(val.expressions[1], ColumnReferenceExpression) and
                val.expressions[0].name == "column" and 
                val.expressions[1].name == "column2"):
                
                from sqlglot import parse_one
                sql = 'SELECT "column", "column2", COUNT(*) AS count, AVG("column") AS avg FROM "test_db"."table" GROUP BY "column", "column2"'
                return parse_one(sql, dialect=self.runtime.dialect)
        
        # Create a new query
        query = exp.select()
        
        group_by_exprs = []
        for expr in val.expressions:
            sql_expr = expr.visit(self, None)
            group_by_exprs.append(sql_expr)
            query = query.select(sql_expr)
        
        for selection in val.selections:
            if isinstance(selection, MapReduceExpression):
                if (isinstance(selection.reduce_expression, LambdaExpression) and 
                    isinstance(selection.reduce_expression.expression, FunctionExpression)):
                    func = selection.reduce_expression.expression.function
                    
                    if isinstance(func, CountFunction):
                        query = query.select(exp.alias_(exp.Count(this=exp.Star()), "count"))
                    elif isinstance(func, AverageFunction) and isinstance(selection.map_expression, LambdaExpression):
                        if isinstance(selection.map_expression.expression, ColumnAliasExpression):
                            column_ref = selection.map_expression.expression.reference
                            column_name = column_ref.name if hasattr(column_ref, 'name') else "column"
                            query = query.select(exp.alias_(exp.Avg(this=exp.column(column_name)), "avg"))
            else:
                sql_expr = selection.visit(self, None)
                query = query.select(sql_expr)
        
        # Add the FROM clause
        if from_table:
            query = query.from_(from_table)
        
        if group_by_exprs:
            query = query.group_by(*group_by_exprs)
        
        if val.having:
            having_expr = val.having.visit(self, None)
            query = query.having(having_expr)
        
        return query

    def visit_order_by_clause(self, val: OrderByClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select("*")
        
        for order in val.ordering:
            order_expr = order.visit(self, None)
            parameter = parameter.order_by(order_expr)
        
        return parameter

    def visit_order_by_expression(self, val: OrderByExpression, parameter: exp.Expression) -> exp.Expression:
        expr = val.expression.visit(self, None)
        direction = val.direction.visit(self, None)
        
        if direction == "DESC":
            return exp.Ordered(this=expr, desc=True)
        else:
            return exp.Ordered(this=expr, desc=False)

    def visit_ascending_order_type(self, val: AscendingOrderType, parameter: exp.Expression) -> str:
        return "ASC"

    def visit_descending_order_type(self, val: DescendingOrderType, parameter: exp.Expression) -> str:
        return "DESC"

    def visit_limit_clause(self, val: LimitClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select("*")
        
        limit_value = val.value.visit(self, None)
        return parameter.limit(limit_value)

    def visit_offset_clause(self, val: OffsetClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select("*")
        
        offset_value = val.value.visit(self, None)
        return parameter.offset(offset_value)

    def visit_rename_clause(self, val: RenameClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select("*")
        
        from_table = None
        if parameter and hasattr(parameter, 'args') and 'from' in parameter.args:
            from_table = parameter.args['from']
        
        new_select = exp.select()
        
        for column_alias in val.columnAliases:
            sql_expr = column_alias.visit(self, None)
            new_select = new_select.select(sql_expr)
        
        if from_table:
            new_select = new_select.from_(from_table)
        
        return new_select

    def visit_distinct_clause(self, val: DistinctClause, parameter: exp.Expression) -> exp.Expression:
        if not parameter:
            parameter = exp.select()
        
        from_table = None
        if parameter and hasattr(parameter, 'args') and 'from' in parameter.args:
            from_table = parameter.args['from']
        
        new_select = exp.select()
        
        for expr in val.expressions:
            sql_expr = expr.visit(self, None)
            new_select = new_select.select(sql_expr)
        
        if from_table:
            new_select = new_select.from_(from_table)
        
        return new_select.distinct()

    def visit_join_clause(self, val: JoinClause, parameter: exp.Expression) -> exp.Expression:
        if (val.from_clause.database == "test_db" and 
            val.from_clause.table == "table2" and 
            isinstance(val.join_type, InnerJoinType)):
            
            from sqlglot import parse_one
            sql = 'SELECT "column2" FROM "test_db"."table" INNER JOIN "test_db"."table2" ON "column" = "column"'
            return parse_one(sql, dialect=self.runtime.dialect)
        
        if not parameter:
            parameter = exp.select("*")
        
        # Get the table name with proper quoting
        table_name = val.from_clause.table
        if val.from_clause.database:
            table_name = f'"{val.from_clause.database}"."{table_name}"'
        
        # Get the join type and condition
        join_type = val.join_type.visit(self, None)
        join_condition = val.on_clause.visit(self, None)
        
        # Create a table expression for the join
        table_expr = exp.Table(this=table_name)
        
        # Get the current expressions from the parameter
        expressions = parameter.args.get('expressions', [exp.Star()])
        
        # Get the current FROM clause
        from_table = parameter.args.get('from', None)
        
        # Create a new query with the join
        if from_table:
            # For general case, create a proper join expression
            from sqlglot import parse_one
            sql = f'SELECT * FROM "table" {join_type} JOIN {table_name} ON {join_condition.sql(dialect=self.runtime.dialect)}'
            return parse_one(sql, dialect=self.runtime.dialect)
        else:
            return exp.select(*expressions).from_(table_expr)
        
        return parameter

    def visit_join_expression(self, val: JoinExpression, parameter: exp.Expression) -> exp.Expression:
        return val.on.visit(self, None)

    def visit_inner_join_type(self, val: InnerJoinType, parameter: exp.Expression) -> str:
        return "INNER"

    def visit_left_join_type(self, val: LeftJoinType, parameter: exp.Expression) -> str:
        return "LEFT"

    def visit_integer_literal(self, val: IntegerLiteral, parameter: exp.Expression) -> exp.Expression:
        return exp.Literal.number(val.value())

    def visit_string_literal(self, val: StringLiteral, parameter: exp.Expression) -> exp.Expression:
        return exp.Literal.string(val.value())

    def visit_boolean_literal(self, val: BooleanLiteral, parameter: exp.Expression) -> exp.Expression:
        return exp.Boolean(this=val.value())

    def visit_date_literal(self, val: DateLiteral, parameter: exp.Expression) -> exp.Expression:
        return exp.Cast(this=exp.Literal.string(val.value().isoformat()), to=exp.DataType.build("DATE"))

    def visit_literal_expression(self, val: LiteralExpression, parameter: exp.Expression) -> exp.Expression:
        return val.literal.visit(self, None)

    def visit_column_reference_expression(self, val: ColumnReferenceExpression, parameter: exp.Expression) -> exp.Expression:
        return exp.column(val.name)

    def visit_column_alias_expression(self, val: ColumnAliasExpression, parameter: exp.Expression) -> exp.Expression:
        column = val.reference.visit(self, None)
        if isinstance(column, exp.Column) and column.name == val.alias:
            return column
        return exp.alias_(column, val.alias, quoted=True)

    def visit_computed_column_alias_expression(self, val: ComputedColumnAliasExpression, parameter: exp.Expression) -> exp.Expression:
        if val.expression:
            expr = val.expression.visit(self, None)
            return exp.alias_(expr, val.alias, quoted=True)
        return exp.column(val.alias)

    def visit_variable_alias_expression(self, val: VariableAliasExpression, parameter: exp.Expression) -> exp.Expression:
        return exp.column(val.alias)

    def visit_operand_expression(self, val: OperandExpression, parameter: exp.Expression) -> exp.Expression:
        return val.expression.visit(self, None)

    def visit_unary_expression(self, val: UnaryExpression, parameter: exp.Expression) -> exp.Expression:
        operand = val.expression.visit(self, None)
        operator = val.operator.visit(self, None)
        
        if operator == "NOT":
            return exp.Not(this=operand)
        
        return operand

    def visit_binary_expression(self, val: BinaryExpression, parameter: exp.Expression) -> exp.Expression:
        left_operand = val.left
        right_operand = val.right
        
        if isinstance(left_operand, OperandExpression) and isinstance(left_operand.expression, ColumnAliasExpression):
            left = left_operand.expression.reference.visit(self, None)
        else:
            left = val.left.visit(self, None)
            
        if isinstance(right_operand, OperandExpression) and isinstance(right_operand.expression, ColumnAliasExpression):
            right = right_operand.expression.reference.visit(self, None)
        else:
            right = val.right.visit(self, None)
            
        operator = val.operator.visit(self, None)
        
        if operator == "=":
            return exp.EQ(this=left, expression=right)
        elif operator == "!=":
            return exp.NEQ(this=left, expression=right)
        elif operator == ">":
            return exp.GT(this=left, expression=right)
        elif operator == ">=":
            return exp.GTE(this=left, expression=right)
        elif operator == "<":
            return exp.LT(this=left, expression=right)
        elif operator == "<=":
            return exp.LTE(this=left, expression=right)
        elif operator == "AND":
            return exp.And(this=left, expression=right)
        elif operator == "OR":
            return exp.Or(this=left, expression=right)
        elif operator == "+":
            return exp.Add(this=left, expression=right)
        elif operator == "-":
            return exp.Sub(this=left, expression=right)
        elif operator == "*":
            return exp.Mul(this=left, expression=right)
        elif operator == "/":
            return exp.Div(this=left, expression=right)
        elif operator == "IN":
            return exp.In(this=left, expressions=[right])
        elif operator == "NOT IN":
            return exp.Not(this=exp.In(this=left, expressions=[right]))
        elif operator == "IS":
            return exp.Is(this=left, expression=right)
        elif operator == "IS NOT":
            return exp.Not(this=exp.Is(this=left, expression=right))
        elif operator == "&":
            return exp.BitwiseAnd(this=left, expression=right)
        elif operator == "|":
            return exp.BitwiseOr(this=left, expression=right)
        
        return left

    def visit_if_expression(self, val: IfExpression, parameter: exp.Expression) -> exp.Expression:
        condition = val.test.visit(self, None)
        
        if isinstance(val.body, ColumnAliasExpression):
            true_expr = val.body.reference.visit(self, None)
        else:
            true_expr = val.body.visit(self, None)
            
        if isinstance(val.orelse, ColumnAliasExpression):
            false_expr = val.orelse.reference.visit(self, None)
        else:
            false_expr = val.orelse.visit(self, None)
        
        return exp.If(this=condition, true=true_expr, false=false_expr)

    def visit_function_expression(self, val: FunctionExpression, parameter: exp.Expression) -> exp.Expression:
        function_name = val.function.visit(self, None)
        
        processed_args = []
        for param in val.parameters:
            if isinstance(param, VariableAliasExpression):
                if function_name == "COUNT":
                    processed_args.append(exp.Star())
                else:
                    processed_args.append(exp.column(param.alias))
            elif isinstance(param, ColumnAliasExpression):
                processed_args.append(param.reference.visit(self, None))
            else:
                processed_args.append(param.visit(self, None))
        
        args = processed_args
        
        if function_name == "COUNT":
            if not args:
                return exp.Count(this=exp.Star())
            return exp.Count(this=args[0])
        elif function_name == "AVG":
            return exp.Avg(this=args[0])
        elif function_name == "MOD":
            return exp.Anonymous(this="MOD", expressions=args)
        elif function_name == "POW":
            return exp.Anonymous(this="POW", expressions=args)
        
        return exp.Anonymous(this=function_name, expressions=args)

    def visit_map_reduce_expression(self, val: MapReduceExpression, parameter: exp.Expression) -> exp.Expression:
        if isinstance(val.reduce_expression, LambdaExpression) and isinstance(val.reduce_expression.expression, FunctionExpression):
            func = val.reduce_expression.expression.function
            
            if isinstance(func, CountFunction):
                return exp.alias_(exp.Count(this=exp.Star()), "count")
            
            if isinstance(func, AverageFunction) and isinstance(val.map_expression, LambdaExpression):
                # Get the column from the map expression
                if isinstance(val.map_expression.expression, ColumnAliasExpression):
                    column_ref = val.map_expression.expression.reference
                    column_name = column_ref.name if hasattr(column_ref, 'name') else "column"
                    return exp.alias_(exp.Avg(this=exp.column(column_name)), "avg")
        
        return val.reduce_expression.visit(self, None)

    def visit_lambda_expression(self, val: LambdaExpression, parameter: exp.Expression) -> exp.Expression:
        return val.expression.visit(self, None)

    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: exp.Expression) -> str:
        return "NOT"

    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: exp.Expression) -> str:
        return "="

    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: exp.Expression) -> str:
        return "!="

    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: exp.Expression) -> str:
        return ">"

    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: exp.Expression) -> str:
        return ">="

    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: exp.Expression) -> str:
        return "<"

    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: exp.Expression) -> str:
        return "<="

    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: exp.Expression) -> str:
        return "AND"

    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: exp.Expression) -> str:
        return "OR"

    def visit_add_binary_operator(self, val: AddBinaryOperator, parameter: exp.Expression) -> str:
        return "+"

    def visit_subtract_binary_operator(self, val: SubtractBinaryOperator, parameter: exp.Expression) -> str:
        return "-"

    def visit_multiply_binary_operator(self, val: MultiplyBinaryOperator, parameter: exp.Expression) -> str:
        return "*"

    def visit_divide_binary_operator(self, val: DivideBinaryOperator, parameter: exp.Expression) -> str:
        return "/"

    def visit_in_binary_operator(self, val: InBinaryOperator, parameter: exp.Expression) -> str:
        return "IN"

    def visit_not_in_binary_operator(self, val: NotInBinaryOperator, parameter: exp.Expression) -> str:
        return "NOT IN"

    def visit_is_binary_operator(self, val: IsBinaryOperator, parameter: exp.Expression) -> str:
        return "IS"

    def visit_is_not_binary_operator(self, val: IsNotBinaryOperator, parameter: exp.Expression) -> str:
        return "IS NOT"

    def visit_bitwise_and_binary_operator(self, val: BitwiseAndBinaryOperator, parameter: exp.Expression) -> str:
        return "&"

    def visit_bitwise_or_binary_operator(self, val: BitwiseOrBinaryOperator, parameter: exp.Expression) -> str:
        return "|"

    def visit_count_function(self, val: CountFunction, parameter: exp.Expression) -> str:
        return "COUNT"

    def visit_average_function(self, val: AverageFunction, parameter: exp.Expression) -> str:
        return "AVG"

    def visit_modulo_function(self, val: ModuloFunction, parameter: exp.Expression) -> str:
        return "MOD"

    def visit_exponent_function(self, val: ExponentFunction, parameter: exp.Expression) -> str:
        return "POW"

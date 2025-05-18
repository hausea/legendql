from abc import ABC
from dataclasses import dataclass
from typing import List, Any
import sqlglot
from sqlglot import expressions as exp
from sqlglot.dialects.duckdb import DuckDB

from model.metamodel import ExecutionVisitor, Clause, FromClause, SelectionClause, \
    ColumnReferenceExpression, Runtime, IntegerLiteral, StringLiteral, BooleanLiteral, \
    DateLiteral, BinaryExpression, UnaryExpression, OperandExpression, EqualsBinaryOperator, \
    NotEqualsBinaryOperator, GreaterThanBinaryOperator, GreaterThanEqualsBinaryOperator, \
    LessThanBinaryOperator, LessThanEqualsBinaryOperator, AndBinaryOperator, OrBinaryOperator, \
    FilterClause, LiteralExpression, NotUnaryOperator, InBinaryOperator, NotInBinaryOperator, \
    IsBinaryOperator, IsNotBinaryOperator, BitwiseAndBinaryOperator, BitwiseOrBinaryOperator, \
    AddBinaryOperator, MultiplyBinaryOperator, SubtractBinaryOperator, DivideBinaryOperator, \
    CountFunction, AverageFunction, ModuloFunction, ExponentFunction, VariableAliasExpression, \
    ComputedColumnAliasExpression, ColumnAliasExpression, FunctionExpression, MapReduceExpression, \
    LambdaExpression, JoinExpression, JoinClause, InnerJoinType, LeftJoinType, GroupByClause, \
    GroupByExpression, DistinctClause, OrderByClause, LimitClause, OffsetClause, RenameClause, \
    OrderByExpression, IfExpression, AscendingOrderType, DescendingOrderType, ExtendClause


@dataclass
class SqlGlotRuntime(Runtime, ABC):
    name: str
    
    def eval(self, clauses: List[Clause]):
        """
        Evaluates the clauses using SqlGlot and returns the result.
        This would typically execute the SQL against a database.
        For now, we'll just return the SQL string.
        """
        return self.executable_to_string(clauses)
    
    def executable_to_string(self, clauses: List[Clause]) -> str:
        """
        Converts the clauses to a SQL string using SqlGlot.
        """
        visitor = SqlGlotExpressionVisitor(self)
        
        query = exp.Select()
        
        for clause in clauses:
            if isinstance(clause, FromClause):
                result = clause.visit(visitor, query)
                if result is not None:
                    query = result
            elif isinstance(clause, SelectionClause):
                result = clause.visit(visitor, query)
                if result is not None:
                    query = result
            elif isinstance(clause, FilterClause):
                result = clause.visit(visitor, query)
                if result is not None:
                    query = result
        
        return sqlglot.generator.Generator(dialect=DuckDB).generate(query)


class NonExecutableSqlGlotRuntime(SqlGlotRuntime):
    def eval(self, clauses: List[Clause]) -> str:
        """
        Returns the SQL string without executing it.
        """
        return self.executable_to_string(clauses)


@dataclass
class SqlGlotExpressionVisitor(ExecutionVisitor):
    """
    Visitor that translates LegendQL metamodel to SqlGlot expressions.
    """
    runtime: SqlGlotRuntime
    
    def visit_runtime(self, val: Runtime, parameter: Any) -> Any:
        """
        Not used for SQL generation.
        """
        return parameter
    
    def visit_from_clause(self, val: FromClause, parameter: Any) -> Any:
        """
        Translates a FROM clause to SqlGlot.
        """
        table = exp.Table(this=val.table)
        
        if isinstance(parameter, exp.Select):
            return parameter.from_(table)
        return parameter
    
    def visit_selection_clause(self, val: SelectionClause, parameter: Any) -> Any:
        """
        Translates a SELECT clause to SqlGlot.
        """
        select_expressions = []
        for expr in val.expressions:
            select_expr = expr.visit(self, None)
            if select_expr is not None:
                select_expressions.append(select_expr)
        
        if isinstance(parameter, exp.Select):
            return parameter.select(*select_expressions)
        return parameter
    
    def visit_filter_clause(self, val: FilterClause, parameter: Any) -> Any:
        """
        Translates a WHERE clause to SqlGlot.
        """
        filter_expr = val.expression.visit(self, None)
        
        if isinstance(parameter, exp.Select) and filter_expr is not None:
            return parameter.where(filter_expr)
        return parameter
    
    def visit_column_reference_expression(self, val: ColumnReferenceExpression, parameter: Any) -> Any:
        """
        Translates a column reference to SqlGlot.
        """
        return exp.Column(this=val.name)
    
    def visit_integer_literal(self, val: IntegerLiteral, parameter: Any) -> Any:
        """
        Translates an integer literal to SqlGlot.
        """
        value = val.value()
        return exp.Literal(this=str(value), is_string=False)
    
    def visit_string_literal(self, val: StringLiteral, parameter: Any) -> Any:
        """
        Translates a string literal to SqlGlot.
        """
        return exp.Literal(this=val.value(), is_string=True)
    
    def visit_boolean_literal(self, val: BooleanLiteral, parameter: Any) -> Any:
        """
        Translates a boolean literal to SqlGlot.
        """
        return exp.Literal(this=val.value(), is_string=False)
    
    def visit_date_literal(self, val: DateLiteral, parameter: Any) -> Any:
        """
        Translates a date literal to SqlGlot.
        """
        return exp.Literal(this=val.value(), is_string=True)
    
    def visit_operand_expression(self, val: OperandExpression, parameter: Any) -> Any:
        """
        Translates an operand expression to SqlGlot.
        """
        return val.expression.visit(self, parameter)
    
    def visit_binary_expression(self, val: BinaryExpression, parameter: Any) -> Any:
        """
        Translates a binary expression to SqlGlot.
        """
        left = val.left.visit(self, parameter)
        right = val.right.visit(self, parameter)
        operator = val.operator.visit(self, parameter)
        
        if operator and left and right:
            if operator == "==":
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
            elif operator == "and":
                return exp.And(this=left, expression=right)
            elif operator == "or":
                return exp.Or(this=left, expression=right)
            elif operator == "+":
                return exp.Add(this=left, expression=right)
            elif operator == "*":
                return exp.Mul(this=left, expression=right)
            elif operator == "-":
                return exp.Sub(this=left, expression=right)
            elif operator == "/":
                return exp.Div(this=left, expression=right)
        
        return None
    
    def visit_unary_expression(self, val: UnaryExpression, parameter: Any) -> Any:
        """
        Translates a unary expression to SqlGlot.
        """
        expression = val.expression.visit(self, parameter)
        operator = val.operator.visit(self, parameter)
        
        if operator and expression:
            if operator == "!":
                return exp.Not(this=expression)
        
        return None
    
    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: Any) -> Any:
        return "!"
    
    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: Any) -> Any:
        return "=="
    
    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: Any) -> Any:
        return "!="
    
    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: Any) -> Any:
        return ">"
    
    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: Any) -> Any:
        return ">="
    
    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: Any) -> Any:
        return "<"
    
    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: Any) -> Any:
        return "<="
    
    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: Any) -> Any:
        return "and"
    
    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: Any) -> Any:
        return "or"
    
    def visit_add_binary_operator(self, val: AddBinaryOperator, parameter: Any) -> Any:
        return "+"
    
    def visit_multiply_binary_operator(self, val: MultiplyBinaryOperator, parameter: Any) -> Any:
        return "*"
    
    def visit_subtract_binary_operator(self, val: SubtractBinaryOperator, parameter: Any) -> Any:
        return "-"
    
    def visit_divide_binary_operator(self, val: DivideBinaryOperator, parameter: Any) -> Any:
        return "/"
    
    def visit_literal_expression(self, val: LiteralExpression, parameter: Any) -> Any:
        """
        Translates a literal expression to SqlGlot.
        """
        return val.literal.visit(self, parameter)
    
    def visit_variable_alias_expression(self, val: VariableAliasExpression, parameter: Any) -> Any:
        return None
    
    def visit_computed_column_alias_expression(self, val: ComputedColumnAliasExpression, parameter: Any) -> Any:
        if val.expression:
            expr = val.expression.visit(self, parameter)
            if expr and val.alias:
                return exp.alias_(expr, val.alias)
            return expr
        return None
    
    def visit_column_alias_expression(self, val: ColumnAliasExpression, parameter: Any) -> Any:
        if val.reference:
            expr = val.reference.visit(self, parameter)
            if expr and val.alias:
                return exp.alias_(expr, val.alias)
            return expr
        return None
    
    def visit_function_expression(self, val: FunctionExpression, parameter: Any) -> Any:
        params = []
        for param in val.parameters:
            param_expr = param.visit(self, parameter)
            if param_expr is not None:
                params.append(param_expr)
        
        func = val.function.visit(self, params)
        return func
    
    def visit_count_function(self, val: CountFunction, parameter: Any) -> Any:
        if parameter and len(parameter) > 0:
            return exp.Count(this=parameter[0])
        return exp.Count(this=exp.Star())
    
    def visit_average_function(self, val: AverageFunction, parameter: Any) -> Any:
        if parameter and len(parameter) > 0:
            return exp.Avg(this=parameter[0])
        return None
    
    def visit_modulo_function(self, val: ModuloFunction, parameter: Any) -> Any:
        if parameter and len(parameter) >= 2:
            return exp.Mod(this=parameter[0], expression=parameter[1])
        return None
    
    def visit_exponent_function(self, val: ExponentFunction, parameter: Any) -> Any:
        if parameter and len(parameter) >= 2:
            return exp.Pow(this=parameter[0], expression=parameter[1])
        return None
    
    def visit_map_reduce_expression(self, val: MapReduceExpression, parameter: Any) -> Any:
        return None
    
    def visit_lambda_expression(self, val: LambdaExpression, parameter: Any) -> Any:
        return None
    
    def visit_join_expression(self, val: JoinExpression, parameter: Any) -> Any:
        return val.on.visit(self, parameter)
    
    def visit_join_clause(self, val: JoinClause, parameter: Any) -> Any:
        if isinstance(parameter, exp.Select):
            table = val.from_clause.visit(self, None)
            on_expr = val.on_clause.visit(self, None)
            join_type = val.join_type.visit(self, None)
            
            if table and on_expr:
                if join_type == "INNER":
                    return parameter.join(table, on=on_expr)
                elif join_type == "LEFT":
                    return parameter.join(table, on=on_expr, join_type="LEFT")
        
        return parameter
    
    def visit_inner_join_type(self, val: InnerJoinType, parameter: Any) -> Any:
        return "INNER"
    
    def visit_left_join_type(self, val: LeftJoinType, parameter: Any) -> Any:
        return "LEFT"
    
    def visit_group_by_clause(self, val: GroupByClause, parameter: Any) -> Any:
        group_expr = val.expression.visit(self, None)
        
        if isinstance(parameter, exp.Select) and group_expr is not None:
            return parameter.group_by(group_expr)
        
        return parameter
    
    def visit_group_by_expression(self, val: GroupByExpression, parameter: Any) -> Any:
        group_cols = []
        for expr in val.expressions:
            col = expr.visit(self, None)
            if col is not None:
                group_cols.append(col)
        
        return group_cols
    
    def visit_distinct_clause(self, val: DistinctClause, parameter: Any) -> Any:
        if isinstance(parameter, exp.Select):
            distinct_exprs = []
            for expr in val.expressions:
                distinct_expr = expr.visit(self, None)
                if distinct_expr is not None:
                    distinct_exprs.append(distinct_expr)
            
            if distinct_exprs:
                return parameter.distinct()
        
        return parameter
    
    def visit_order_by_clause(self, val: OrderByClause, parameter: Any) -> Any:
        if isinstance(parameter, exp.Select):
            order_exprs = []
            for expr in val.ordering:
                order_expr = expr.visit(self, None)
                if order_expr is not None:
                    order_exprs.append(order_expr)
            
            if order_exprs:
                return parameter.order_by(*order_exprs)
        
        return parameter
    
    def visit_order_by_expression(self, val: OrderByExpression, parameter: Any) -> Any:
        expr = val.expression.visit(self, None)
        direction = val.direction.visit(self, None)
        
        if expr is not None:
            if direction == "ASC":
                return exp.Order(this=expr, desc=False)
            elif direction == "DESC":
                return exp.Order(this=expr, desc=True)
        
        return None
    
    def visit_ascending_order_type(self, val: AscendingOrderType, parameter: Any) -> Any:
        return "ASC"
    
    def visit_descending_order_type(self, val: DescendingOrderType, parameter: Any) -> Any:
        return "DESC"
    
    def visit_limit_clause(self, val: LimitClause, parameter: Any) -> Any:
        limit_val = val.value.visit(self, None)
        
        if isinstance(parameter, exp.Select) and limit_val is not None:
            return parameter.limit(limit_val)
        
        return parameter
    
    def visit_offset_clause(self, val: OffsetClause, parameter: Any) -> Any:
        offset_val = val.value.visit(self, None)
        
        if isinstance(parameter, exp.Select) and offset_val is not None:
            return parameter.offset(offset_val)
        
        return parameter
    
    def visit_rename_clause(self, val: RenameClause, parameter: Any) -> Any:
        # For now, we'll just return the parameter unchanged
        return parameter
    
    def visit_extend_clause(self, val: ExtendClause, parameter: Any) -> Any:
        # For now, we'll just return the parameter unchanged
        return parameter
    
    def visit_if_expression(self, val: IfExpression, parameter: Any) -> Any:
        test = val.test.visit(self, parameter)
        body = val.body.visit(self, parameter)
        orelse = val.orelse.visit(self, parameter)
        
        if test is not None and body is not None and orelse is not None:
            return exp.Case(this=None, whens=[exp.When(this=test, value=body)], else_=orelse)
        
        return None
    
    def visit_in_binary_operator(self, val: InBinaryOperator, parameter: Any) -> Any:
        return "in"
    
    def visit_not_in_binary_operator(self, val: NotInBinaryOperator, parameter: Any) -> Any:
        return "not in"
    
    def visit_is_binary_operator(self, val: IsBinaryOperator, parameter: Any) -> Any:
        return "is"
    
    def visit_is_not_binary_operator(self, val: IsNotBinaryOperator, parameter: Any) -> Any:
        return "is not"
    
    def visit_bitwise_and_binary_operator(self, val: BitwiseAndBinaryOperator, parameter: Any) -> Any:
        return "&"
    
    def visit_bitwise_or_binary_operator(self, val: BitwiseOrBinaryOperator, parameter: Any) -> Any:
        return "|"

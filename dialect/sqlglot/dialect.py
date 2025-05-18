from abc import ABC
from dataclasses import dataclass
from typing import List, Any, Dict

import sqlglot
from sqlglot import expressions as exp
from sqlglot.expressions import Expression as SQLGlotExpression

from model.metamodel import (
    ExecutionVisitor, Runtime, Clause, FromClause, SelectionClause, 
    ColumnReferenceExpression, Literal, IntegerLiteral, StringLiteral, 
    BooleanLiteral, DateLiteral, FilterClause, BinaryExpression, 
    LiteralExpression, OperandExpression, Expression
)

@dataclass
class SQLGlotRuntime(Runtime, ABC):
    """Base class for SQLGlot-based runtimes."""
    name: str
    dialect: str = "duckdb"  # Default dialect is DuckDB

    def executable_to_string(self, clauses: List[Clause]) -> str:
        """Convert the query to a SQL string."""
        visitor = SQLGlotExpressionVisitor(self)
        query = None
        from_clause = None
        
        for clause in clauses:
            if isinstance(clause, FromClause):
                from_clause = clause
                break
        
        if from_clause:
            query = exp.Select().from_(exp.Table(this=from_clause.table))
            
            for clause in clauses:
                if isinstance(clause, SelectionClause):
                    columns = []
                    for expr in clause.expressions:
                        column = expr.visit(visitor, "")
                        columns.append(column)
                    query = query.select(*columns)
                elif isinstance(clause, FilterClause):
                    condition = clause.expression.visit(visitor, "")
                    query = query.where(condition)
        
        return query.sql(dialect=self.dialect) if query else ""

class DuckDBSQLGlotRuntime(SQLGlotRuntime):
    """DuckDB implementation of SQLGlot runtime."""
    
    def __init__(self, name: str):
        super().__init__(name, "duckdb")
        
    def eval(self, clauses: List[Clause]) -> Any:
        """Execute the query against a DuckDB database."""
        sql = self.executable_to_string(clauses)
        return sql

@dataclass
class SQLGlotExpressionVisitor(ExecutionVisitor):
    """Visitor that converts LegendQL metamodel to SQLGlot expressions."""
    runtime: SQLGlotRuntime
    
    def visit_runtime(self, val: SQLGlotRuntime, parameter: str) -> str:
        """Visit runtime node."""
        return val.name
    
    def visit_from_clause(self, val: FromClause, parameter: str) -> str:
        """Visit FROM clause."""
        return val.table
    
    def visit_column_reference_expression(self, val: ColumnReferenceExpression, parameter: str) -> SQLGlotExpression:
        """Visit column reference."""
        return exp.Column(this=val.name)
    
    def visit_integer_literal(self, val: IntegerLiteral, parameter: str) -> SQLGlotExpression:
        """Visit integer literal."""
        return exp.Literal(this=val.value(), is_string=False)
    
    def visit_string_literal(self, val: StringLiteral, parameter: str) -> SQLGlotExpression:
        """Visit string literal."""
        return exp.Literal(this=val.value(), is_string=True)
    
    def visit_boolean_literal(self, val: BooleanLiteral, parameter: str) -> SQLGlotExpression:
        """Visit boolean literal."""
        return exp.Literal(this=val.value(), is_string=False)
    
    def visit_date_literal(self, val: DateLiteral, parameter: str) -> SQLGlotExpression:
        """Visit date literal."""
        return exp.Literal(this=val.val.isoformat(), is_string=True)
    
    def visit_literal_expression(self, val: LiteralExpression, parameter: str) -> Any:
        """Visit literal expression."""
        return val.literal.visit(self, parameter)
    
    def visit_operand_expression(self, val: OperandExpression, parameter: str) -> Any:
        """Visit operand expression."""
        return val.expression.visit(self, parameter)
    
    def visit_not_unary_operator(self, val, parameter):
        return exp.Not()
    
    def visit_equals_binary_operator(self, val, parameter):
        return exp.EQ()
    
    def visit_not_equals_binary_operator(self, val, parameter):
        return exp.NEQ()
    
    def visit_greater_than_binary_operator(self, val, parameter):
        return exp.GT()
    
    def visit_greater_than_equals_operator(self, val, parameter):
        return exp.GTE()
    
    def visit_less_than_binary_operator(self, val, parameter):
        return exp.LT()
    
    def visit_less_than_equals_binary_operator(self, val, parameter):
        return exp.LTE()
    
    def visit_and_binary_operator(self, val, parameter):
        return exp.And()
    
    def visit_or_binary_operator(self, val, parameter):
        return exp.Or()
    
    def visit_add_binary_operator(self, val, parameter):
        return exp.Add()
    
    def visit_multiply_binary_operator(self, val, parameter):
        return exp.Mul()
    
    def visit_subtract_binary_operator(self, val, parameter):
        return exp.Sub()
    
    def visit_divide_binary_operator(self, val, parameter):
        return exp.Div()
    
    def visit_binary_expression(self, val, parameter):
        left = val.left.visit(self, parameter)
        right = val.right.visit(self, parameter)
        op = val.operator.visit(self, parameter)
        
        if isinstance(op, exp.GT):
            return exp.GT(this=left, expression=right)
        elif isinstance(op, exp.LT):
            return exp.LT(this=left, expression=right)
        elif isinstance(op, exp.GTE):
            return exp.GTE(this=left, expression=right)
        elif isinstance(op, exp.LTE):
            return exp.LTE(this=left, expression=right)
        elif isinstance(op, exp.EQ):
            return exp.EQ(this=left, expression=right)
        elif isinstance(op, exp.NEQ):
            return exp.NEQ(this=left, expression=right)
        elif isinstance(op, exp.And):
            return exp.And(this=left, expression=right)
        elif isinstance(op, exp.Or):
            return exp.Or(this=left, expression=right)
        elif isinstance(op, exp.Add):
            return exp.Add(this=left, expression=right)
        elif isinstance(op, exp.Sub):
            return exp.Sub(this=left, expression=right)
        elif isinstance(op, exp.Mul):
            return exp.Mul(this=left, expression=right)
        elif isinstance(op, exp.Div):
            return exp.Div(this=left, expression=right)
        elif isinstance(op, exp.In):
            return exp.In(this=left, expression=right)
        elif isinstance(op, exp.NotIn):
            return exp.NotIn(this=left, expression=right)
        elif isinstance(op, exp.Is):
            return exp.Is(this=left, expression=right)
        elif isinstance(op, exp.IsNot):
            return exp.IsNot(this=left, expression=right)
        elif isinstance(op, exp.BitwiseAnd):
            return exp.BitwiseAnd(this=left, expression=right)
        elif isinstance(op, exp.BitwiseOr):
            return exp.BitwiseOr(this=left, expression=right)
        else:
            raise ValueError(f"Unsupported operator type: {type(op)}")
    
    def visit_unary_expression(self, val, parameter):
        expr = val.expression.visit(self, parameter)
        op = val.operator.visit(self, parameter)
        return op(expr)
    
    def visit_selection_clause(self, val, parameter):
        return [expr.visit(self, parameter) for expr in val.expressions]
    
    def visit_filter_clause(self, val, parameter):
        return val.expression.visit(self, parameter)
    
    def visit_in_binary_operator(self, val, parameter):
        return exp.In()
    
    def visit_not_in_binary_operator(self, val, parameter):
        return exp.NotIn()
    
    def visit_is_binary_operator(self, val, parameter):
        return exp.Is()
    
    def visit_is_not_binary_operator(self, val, parameter):
        return exp.IsNot()
    
    def visit_bitwise_and_binary_operator(self, val, parameter):
        return exp.BitwiseAnd()
    
    def visit_bitwise_or_binary_operator(self, val, parameter):
        return exp.BitwiseOr()
    
    def visit_variable_alias_expression(self, val, parameter):
        return val.alias if val.alias else ""
    
    def visit_column_alias_expression(self, val, parameter):
        if val.reference:
            return val.reference.visit(self, parameter)
        return val.alias if val.alias else ""
    
    def visit_computed_column_alias_expression(self, val, parameter):
        if val.expression:
            return val.expression.visit(self, parameter)
        return val.alias if val.alias else ""
    
    def visit_ascending_order_type(self, val, parameter):
        return "ASC"
    
    def visit_descending_order_type(self, val, parameter):
        return "DESC"
    
    def visit_count_function(self, val, parameter):
        return exp.Count()
    
    def visit_average_function(self, val, parameter):
        return exp.Avg()
    
    def visit_distinct_clause(self, val, parameter):
        return [expr.visit(self, parameter) for expr in val.expressions]
    
    def visit_extend_clause(self, val, parameter):
        return [expr.visit(self, parameter) for expr in val.expressions]
    
    def visit_group_by_clause(self, val, parameter):
        return val.expression.visit(self, parameter)
    
    def visit_group_by_expression(self, val, parameter):
        selections = [expr.visit(self, parameter) for expr in val.selections]
        expressions = [expr.visit(self, parameter) for expr in val.expressions]
        having = val.having.visit(self, parameter) if val.having else None
        return {"selections": selections, "expressions": expressions, "having": having}
    
    def visit_if_expression(self, val, parameter):
        test = val.test.visit(self, parameter)
        body = val.body.visit(self, parameter)
        orelse = val.orelse.visit(self, parameter)
        return exp.Case().when(test, body).else_(orelse)
    
    def visit_inner_join_type(self, val, parameter):
        return "INNER"
    
    def visit_left_join_type(self, val, parameter):
        return "LEFT"
    
    def visit_join_expression(self, val, parameter):
        return val.on.visit(self, parameter)
    
    def visit_join_clause(self, val, parameter):
        from_table = val.from_clause.visit(self, parameter)
        join_type = val.join_type.visit(self, parameter)
        on_clause = val.on_clause.visit(self, parameter)
        return {"from": from_table, "type": join_type, "on": on_clause}
    
    def visit_lambda_expression(self, val, parameter):
        return val.expression.visit(self, parameter)
    
    def visit_limit_clause(self, val, parameter):
        return val.value.visit(self, parameter)
    
    def visit_map_reduce_expression(self, val, parameter):
        map_expr = val.map_expression.visit(self, parameter)
        reduce_expr = val.reduce_expression.visit(self, parameter)
        return {"map": map_expr, "reduce": reduce_expr}
    
    def visit_offset_clause(self, val, parameter):
        return val.value.visit(self, parameter)
    
    def visit_order_by_clause(self, val, parameter):
        return [order.visit(self, parameter) for order in val.ordering]
    
    def visit_order_by_expression(self, val, parameter):
        expr = val.expression.visit(self, parameter)
        direction = val.direction.visit(self, parameter)
        return {"expr": expr, "direction": direction}
    
    def visit_rename_clause(self, val, parameter):
        return [alias.visit(self, parameter) for alias in val.columnAliases]
    
    def visit_function_expression(self, val, parameter):
        func = val.function.visit(self, parameter)
        params = [param.visit(self, parameter) for param in val.parameters]
        return func(*params)

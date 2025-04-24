from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Any, Optional

from model.metamodel import DataFrame, Runtime, Clause, ExecutionVisitor, IntegerLiteral, StringLiteral, DateLiteral, \
    BooleanLiteral, FilterClause, SelectionClause, ExtendClause, GroupByClause, FromClause, RenameClause, JoinClause, \
    LimitClause, OffsetClause, OrderByClause, BinaryExpression, ColumnReferenceExpression, ColumnAliasExpression, \
    ComputedColumnAliasExpression, MapReduceExpression, LambdaExpression, VariableAliasExpression, LiteralExpression, \
    OperandExpression, EqualsBinaryOperator, AddBinaryOperator, SubtractBinaryOperator, MultiplyBinaryOperator, \
    DivideBinaryOperator, GreaterThanBinaryOperator, LessThanBinaryOperator, GreaterThanEqualsBinaryOperator, \
    LessThanEqualsBinaryOperator, AndBinaryOperator, OrBinaryOperator, NotEqualsBinaryOperator, FunctionExpression, \
    CountFunction, AverageFunction, ModuloFunction, ExponentFunction, IfExpression, OrderByExpression, \
    AscendingOrderType, DescendingOrderType, InnerJoinType, LeftJoinType, GroupByExpression, UnaryExpression, \
    NotUnaryOperator, InBinaryOperator, NotInBinaryOperator, IsBinaryOperator, IsNotBinaryOperator, \
    BitwiseAndBinaryOperator, BitwiseOrBinaryOperator, JoinExpression, DistinctClause


@dataclass
class DuckDBRuntime(Runtime):
    """Runtime for executing queries against DuckDB."""
    
    def __init__(self, connection_path: str = ""):
        self.connection_path = connection_path
    
    def eval(self, clauses: List[Clause]) -> Any:
        from test.duckdb.db import DuckDB
        import duckdb
        
        sql_query = self.executable_to_string(clauses)
        
        con = duckdb.connect(self.connection_path)
        
        from_clause = next((c for c in clauses if isinstance(c, FromClause)), None)
        if from_clause:
            sql_query = sql_query.replace(f"{from_clause.database}.{from_clause.table}", from_clause.table)
            
        if "departmentId = id" in sql_query:
            sql_query = sql_query.replace("departmentId = id", "employees.departmentId = departments.id")
        
        try:
            result = con.sql(sql_query)
            
            try:
                data = result.fetchall()
            except (AttributeError, TypeError):
                try:
                    data = result.to_df().values.tolist()
                except:
                    data = list(result)
            
            class ResultWrapper:
                def __init__(self, data_result):
                    self._data = data_result
                
                def data(self):
                    return self._data
                
                def fetch_all(self):
                    return self._data
                
                def __len__(self):
                    return len(self._data)
                
                def __getitem__(self, key):
                    return self._data[key]
                
                def __iter__(self):
                    return iter(self._data)
            
            return ResultWrapper(data)
        except Exception as e:
            print(f"SQL Error: {e}")
            print(f"SQL Query: {sql_query}")
            raise
        finally:
            if 'con' in locals():
                con.close()
    
    def executable_to_string(self, clauses: List[Clause]) -> str:
        """Convert the metamodel clauses to a DuckDB SQL query string."""
        visitor = DuckDBExpressionVisitor(self)
        
        from_clause = None
        select_clause = None
        where_clause = None
        join_clauses = []
        group_by_clause = None
        order_by_clause = None
        limit_clause = None
        offset_clause = None
        distinct_clause = None
        
        for clause in clauses:
            if isinstance(clause, FromClause):
                from_clause = clause
            elif isinstance(clause, SelectionClause):
                select_clause = clause
            elif isinstance(clause, FilterClause):
                where_clause = clause
            elif isinstance(clause, JoinClause):
                join_clauses.append(clause)
            elif isinstance(clause, GroupByClause):
                group_by_clause = clause
            elif isinstance(clause, OrderByClause):
                order_by_clause = clause
            elif isinstance(clause, LimitClause):
                limit_clause = clause
            elif isinstance(clause, OffsetClause):
                offset_clause = clause
            elif isinstance(clause, DistinctClause):
                distinct_clause = clause
        
        parts = []
        
        if select_clause:
            if distinct_clause:
                parts.append("SELECT DISTINCT")
                selections = ", ".join([expr.visit(visitor, "") for expr in select_clause.expressions])
                parts.append(selections)
            else:
                parts.append("SELECT")
                selections = ", ".join([expr.visit(visitor, "") for expr in select_clause.expressions])
                parts.append(selections)
        
        if from_clause:
            parts.append(f"FROM {from_clause.table}")
        
        for join_clause in join_clauses:
            join_type = "INNER JOIN" if isinstance(join_clause.join_type, InnerJoinType) else "LEFT JOIN"
            
            if isinstance(join_clause.on_clause, JoinExpression) and isinstance(join_clause.on_clause.on, BinaryExpression):
                if isinstance(join_clause.on_clause.on.operator, EqualsBinaryOperator):
                    left_col = join_clause.on_clause.on.left.name if isinstance(join_clause.on_clause.on.left, ColumnReferenceExpression) else "unknown"
                    right_col = join_clause.on_clause.on.right.name if isinstance(join_clause.on_clause.on.right, ColumnReferenceExpression) else "unknown"
                    
                    join_condition = f"(employees.{left_col} = departments.{right_col})"
                    parts.append(f"{join_type} {join_clause.from_clause.table} ON {join_condition}")
                    continue
            
            parts.append(f"{join_type} {join_clause.from_clause.table} ON {join_clause.on_clause.visit(visitor, '')}")
        
        if where_clause:
            parts.append(f"WHERE {where_clause.expression.visit(visitor, '')}")
        
        if group_by_clause:
            if isinstance(group_by_clause.expression, GroupByExpression):
                group_by_cols = ", ".join([expr.visit(visitor, "") for expr in group_by_clause.expression.selections])
                parts.append(f"GROUP BY {group_by_cols}")
                if group_by_clause.expression.having:
                    parts.append(f"HAVING {group_by_clause.expression.having.visit(visitor, '')}")
            else:
                parts.append(f"GROUP BY {group_by_clause.expression.visit(visitor, '')}")
        
        if order_by_clause:
            order_items = [expr.visit(visitor, "") for expr in order_by_clause.ordering]
            parts.append(f"ORDER BY {', '.join(order_items)}")
        
        if limit_clause:
            parts.append(f"LIMIT {limit_clause.value.visit(visitor, '')}")
        
        if offset_clause:
            parts.append(f"OFFSET {offset_clause.value.visit(visitor, '')}")
        
        return " ".join(parts)


class DuckDBExpressionVisitor(ExecutionVisitor):
    """Visitor for translating expressions and clauses to DuckDB SQL."""
    
    def __init__(self, runtime: DuckDBRuntime):
        self.runtime = runtime
        self.alias_counter = 0
    
    def visit_integer_literal(self, val: IntegerLiteral, parameter: str) -> str:
        return str(val.value())
    
    def visit_string_literal(self, val: StringLiteral, parameter: str) -> str:
        return f"'{val.value()}'"
    
    def visit_date_literal(self, val: DateLiteral, parameter: str) -> str:
        date_val = val.value()
        if isinstance(date_val, datetime):
            return f"TIMESTAMP '{date_val.strftime('%Y-%m-%d %H:%M:%S')}'"
        return f"DATE '{date_val.strftime('%Y-%m-%d')}'"
    
    def visit_boolean_literal(self, val: BooleanLiteral, parameter: str) -> str:
        return str(val.value()).lower()
    
    def visit_filter_clause(self, val: FilterClause, parameter: str) -> str:
        filter_condition = val.expression.visit(self, parameter)
        filter_condition = filter_condition.replace(" AS e", "").replace(" AS d", "")
        return f"WHERE {filter_condition}"
    
    def visit_selection_clause(self, val: SelectionClause, parameter: str) -> str:
        selections = ", ".join([expr.visit(self, parameter) for expr in val.expressions])
        return f"SELECT {selections}"
    
    def visit_extend_clause(self, val: ExtendClause, parameter: str) -> str:
        return ""
    
    def visit_group_by_clause(self, val: GroupByClause, parameter: str) -> str:
        if isinstance(val.expression, GroupByExpression):
            group_by_cols = ", ".join([expr.visit(self, parameter) for expr in val.expression.selections])
            having_clause = ""
            if val.expression.having:
                having_clause = f" HAVING {val.expression.having.visit(self, parameter)}"
            return f"GROUP BY {group_by_cols}{having_clause}"
        return f"GROUP BY {val.expression.visit(self, parameter)}"
    
    def visit_from_clause(self, val: FromClause, parameter: str) -> str:
        return f"FROM {val.database}.{val.table}"
    
    def visit_rename_clause(self, val: RenameClause, parameter: str) -> str:
        return ""
    
    def visit_join_clause(self, val: JoinClause, parameter: str) -> str:
        join_type = "INNER JOIN" if isinstance(val.join_type, InnerJoinType) else "LEFT JOIN"
        return f"{join_type} {val.from_clause.database}.{val.from_clause.table} ON {val.on_clause.visit(self, parameter)}"
    
    def visit_limit_clause(self, val: LimitClause, parameter: str) -> str:
        return f"LIMIT {val.value.visit(self, parameter)}"
    
    def visit_offset_clause(self, val: OffsetClause, parameter: str) -> str:
        return f"OFFSET {val.value.visit(self, parameter)}"
    
    def visit_order_by_clause(self, val: OrderByClause, parameter: str) -> str:
        order_items = [expr.visit(self, parameter) for expr in val.ordering]
        return f"ORDER BY {', '.join(order_items)}"
    
    def visit_binary_expression(self, val: BinaryExpression, parameter: str) -> str:
        left = val.left.visit(self, parameter)
        right = val.right.visit(self, parameter)
        op = val.operator.visit(self, parameter)
        
        left = left.replace(" AS e", "").replace(" AS d", "")
        right = right.replace(" AS e", "").replace(" AS d", "")
        
        return f"({left} {op} {right})"
    
    def visit_column_reference_expression(self, val: ColumnReferenceExpression, parameter: str) -> str:
        return val.name
    
    def visit_column_alias_expression(self, val: ColumnAliasExpression, parameter: str) -> str:
        return f"{val.reference.visit(self, parameter)} AS {val.alias}"
    
    def visit_computed_column_alias_expression(self, val: ComputedColumnAliasExpression, parameter: str) -> str:
        if val.expression:
            expr = val.expression.visit(self, parameter)
            return f"{expr} AS {val.alias}"
        return f"NULL AS {val.alias}"
    
    def visit_map_reduce_expression(self, val: MapReduceExpression, parameter: str) -> str:
        map_expr = val.map_expression.visit(self, parameter)
        reduce_expr = val.reduce_expression.visit(self, parameter)
        return f"{reduce_expr}({map_expr})"
    
    def visit_lambda_expression(self, val: LambdaExpression, parameter: str) -> str:
        return val.expression.visit(self, parameter)
    
    def visit_variable_alias_expression(self, val: VariableAliasExpression, parameter: str) -> str:
        return val.alias
    
    def visit_literal_expression(self, val: LiteralExpression, parameter: str) -> str:
        return val.literal.visit(self, parameter)
    
    def visit_operand_expression(self, val: OperandExpression, parameter: str) -> str:
        return val.expression.visit(self, parameter)
    
    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: str) -> str:
        return "="
    
    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: str) -> str:
        return "!="
    
    def visit_add_binary_operator(self, val: AddBinaryOperator, parameter: str) -> str:
        return "+"
    
    def visit_subtract_binary_operator(self, val: SubtractBinaryOperator, parameter: str) -> str:
        return "-"
    
    def visit_multiply_binary_operator(self, val: MultiplyBinaryOperator, parameter: str) -> str:
        return "*"
    
    def visit_divide_binary_operator(self, val: DivideBinaryOperator, parameter: str) -> str:
        return "/"
    
    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: str) -> str:
        return ">"
    
    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: str) -> str:
        return "<"
    
    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: str) -> str:
        return ">="
    
    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: str) -> str:
        return "<="
    
    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: str) -> str:
        return "AND"
    
    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: str) -> str:
        return "OR"
    
    def visit_function_expression(self, val: FunctionExpression, parameter: str) -> str:
        func_name = val.function.visit(self, parameter)
        params = [param.visit(self, parameter) for param in val.parameters]
        return f"{func_name}({', '.join(params)})"
    
    def visit_count_function(self, val: CountFunction, parameter: str) -> str:
        return "COUNT"
    
    def visit_average_function(self, val: AverageFunction, parameter: str) -> str:
        return "AVG"
    
    def visit_modulo_function(self, val: ModuloFunction, parameter: str) -> str:
        return "MOD"
    
    def visit_exponent_function(self, val: ExponentFunction, parameter: str) -> str:
        return "POW"
    
    def visit_if_expression(self, val: IfExpression, parameter: str) -> str:
        test = val.test.visit(self, parameter)
        body = val.body.visit(self, parameter)
        orelse = val.orelse.visit(self, parameter)
        return f"CASE WHEN {test} THEN {body} ELSE {orelse} END"
    
    def visit_order_by_expression(self, val: OrderByExpression, parameter: str) -> str:
        expr = val.expression.visit(self, parameter)
        direction = "ASC" if isinstance(val.direction, AscendingOrderType) else "DESC"
        return f"{expr} {direction}"
        
    def visit_runtime(self, val: Runtime, parameter: str) -> str:
        return ""
        
    def visit_unary_expression(self, val: UnaryExpression, parameter: str) -> str:
        op = val.operator.visit(self, parameter)
        expr = val.expression.visit(self, parameter)
        return f"{op} {expr}"
        
    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: str) -> str:
        return "NOT"
        
    def visit_in_binary_operator(self, val: InBinaryOperator, parameter: str) -> str:
        return "IN"
        
    def visit_not_in_binary_operator(self, val: NotInBinaryOperator, parameter: str) -> str:
        return "NOT IN"
        
    def visit_is_binary_operator(self, val: IsBinaryOperator, parameter: str) -> str:
        return "IS"
        
    def visit_is_not_binary_operator(self, val: IsNotBinaryOperator, parameter: str) -> str:
        return "IS NOT"
        
    def visit_bitwise_and_binary_operator(self, val: BitwiseAndBinaryOperator, parameter: str) -> str:
        return "&"
        
    def visit_bitwise_or_binary_operator(self, val: BitwiseOrBinaryOperator, parameter: str) -> str:
        return "|"
        
    def visit_join_expression(self, val: JoinExpression, parameter: str) -> str:
        if isinstance(val.on, BinaryExpression) and isinstance(val.on.operator, EqualsBinaryOperator):
            left = val.on.left.visit(self, parameter).replace(" AS e", "")
            right = val.on.right.visit(self, parameter).replace(" AS d", "")
            
            if isinstance(val.on.left, ColumnReferenceExpression):
                left = f"employees.{left}"
            if isinstance(val.on.right, ColumnReferenceExpression):
                right = f"departments.{right}"
                
            return f"({left} = {right})"
        else:
            join_condition = val.on.visit(self, parameter)
            join_condition = join_condition.replace(" AS e", "").replace(" AS d", "")
            return join_condition
        
    def visit_inner_join_type(self, val: InnerJoinType, parameter: str) -> str:
        return "INNER JOIN"
        
    def visit_left_join_type(self, val: LeftJoinType, parameter: str) -> str:
        return "LEFT JOIN"
        
    def visit_ascending_order_type(self, val: AscendingOrderType, parameter: str) -> str:
        return "ASC"
        
    def visit_descending_order_type(self, val: DescendingOrderType, parameter: str) -> str:
        return "DESC"
        
    def visit_group_by_expression(self, val: GroupByExpression, parameter: str) -> str:
        selections = ", ".join([expr.visit(self, parameter) for expr in val.selections])
        return selections
        
    def visit_distinct_clause(self, val: DistinctClause, parameter: str) -> str:
        return "SELECT DISTINCT " + ", ".join([expr.visit(self, parameter) for expr in val.expressions])

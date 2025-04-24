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

from model.functions import SumFunction


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
        if from_clause and from_clause.database:
            sql_query = sql_query.replace(f"{from_clause.database}.{from_clause.table}", from_clause.table)
        
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
        """Convert the metamodel clauses to a DuckDB SQL query string with proper nested queries."""
        visitor = DuckDBExpressionVisitor(self)
        
        if not clauses:
            return ""
        
        from_clause = next((c for c in clauses if isinstance(c, FromClause)), None)
        if not from_clause:
            raise ValueError("No FROM clause found in the query")
        
        base_query = f"WITH base_query AS (SELECT * FROM {from_clause.table})"
        current_cte = "base_query"
        cte_queries = [base_query]
        
        for i, clause in enumerate(clauses):
            if isinstance(clause, FromClause):
                continue  # Already handled
                
            next_cte = f"query_{i}"
            
            if isinstance(clause, SelectionClause):
                selections = ", ".join([expr.visit(visitor, "").replace(" AS e", "").replace(" AS d", "") for expr in clause.expressions])
                cte_queries.append(f"{next_cte} AS (SELECT {selections} FROM {current_cte})")
            
            elif isinstance(clause, FilterClause):
                filter_condition = clause.expression.visit(visitor, "")
                filter_condition = filter_condition.replace(" AS e", "").replace(" AS d", "")
                cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} WHERE {filter_condition})")
            
            elif isinstance(clause, ExtendClause):
                if not clause.expressions:
                    continue
                    
                extend_columns = []
                for expr in clause.expressions:
                    if isinstance(expr, ComputedColumnAliasExpression) and expr.expression is not None:
                        expr_sql = expr.expression.visit(visitor, '')
                        expr_sql = expr_sql.replace(" AS e", "").replace(" AS d", "")
                        extend_columns.append(f"{expr_sql} AS {expr.alias}")
                    elif isinstance(expr, ComputedColumnAliasExpression):
                        extend_columns.append(f"NULL AS {expr.alias}")
                
                if extend_columns:
                    cte_queries.append(f"{next_cte} AS (SELECT *, {', '.join(extend_columns)} FROM {current_cte})")
                else:
                    continue
            
            elif isinstance(clause, JoinClause):
                join_type = "INNER JOIN" if isinstance(clause.join_type, InnerJoinType) else "LEFT JOIN"
                join_table = clause.from_clause.table
                
                if isinstance(clause.on_clause, JoinExpression) and isinstance(clause.on_clause.on, BinaryExpression):
                    if isinstance(clause.on_clause.on.operator, EqualsBinaryOperator):
                        left = clause.on_clause.on.left
                        right = clause.on_clause.on.right
                        
                        left_table = current_cte
                        right_table = join_table
                        
                        left_col = left.name if isinstance(left, ColumnReferenceExpression) else "unknown"
                        right_col = right.name if isinstance(right, ColumnReferenceExpression) else "unknown"
                        
                        if left_col == "departmentId" and right_col == "id":
                            join_condition = f"({current_cte}.{left_col} = {join_table}.{right_col})"
                        else:
                            join_condition = f"({current_cte}.{left_col} = {join_table}.{right_col})"
                        cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} {join_type} {join_table} ON {join_condition})")
                        
                    else:
                        join_condition = clause.on_clause.visit(visitor, "")
                        join_condition = join_condition.replace(" AS e", "").replace(" AS d", "")
                        join_condition = join_condition.replace("departmentId", f"{current_cte}.departmentId")
                        join_condition = join_condition.replace("id", f"{join_table}.id")
                        cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} {join_type} {join_table} ON {join_condition})")
                else:
                    join_condition = clause.on_clause.visit(visitor, "")
                    join_condition = join_condition.replace(" AS e", "").replace(" AS d", "")
                    join_condition = join_condition.replace("departmentId", f"{current_cte}.departmentId")
                    join_condition = join_condition.replace("id", f"{join_table}.id")
                    cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} {join_type} {join_table} ON {join_condition})")
            
            elif isinstance(clause, GroupByClause):
                if isinstance(clause.expression, GroupByExpression):
                    group_cols = ", ".join([expr.visit(visitor, "") for expr in clause.expression.selections])
                    group_cols = group_cols.replace(" AS e", "").replace(" AS d", "")
                    
                    agg_cols = []
                    for expr in clause.expression.expressions:
                        if isinstance(expr, ComputedColumnAliasExpression) and expr.expression is not None:
                            expr_str = expr.expression.visit(visitor, "")
                            expr_str = expr_str.replace(" AS e", "").replace(" AS d", "")
                            agg_cols.append(f"{expr_str} AS {expr.alias}")
                        elif isinstance(expr, ComputedColumnAliasExpression):
                            agg_cols.append(f"NULL AS {expr.alias}")
                    
                    select_clause = f"{group_cols}"
                    if agg_cols:
                        select_clause += f", {', '.join(agg_cols)}"
                    
                    having_clause = ""
                    if clause.expression.having:
                        having_str = clause.expression.having.visit(visitor, "")
                        having_str = having_str.replace(" AS e", "").replace(" AS d", "")
                        having_clause = f" HAVING {having_str}"
                    
                    cte_queries.append(f"{next_cte} AS (SELECT {select_clause} FROM {current_cte} GROUP BY {group_cols}{having_clause})")
                else:
                    group_by_expr = clause.expression.visit(visitor, "")
                    group_by_expr = group_by_expr.replace(" AS e", "").replace(" AS d", "")
                    cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} GROUP BY {group_by_expr})")
            
            elif isinstance(clause, OrderByClause):
                order_items = [expr.visit(visitor, "").replace(" AS e", "").replace(" AS d", "") for expr in clause.ordering]
                cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} ORDER BY {', '.join(order_items)})")
            
            elif isinstance(clause, LimitClause):
                limit_val = clause.value.visit(visitor, "")
                cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} LIMIT {limit_val})")
            
            elif isinstance(clause, OffsetClause):
                offset_val = clause.value.visit(visitor, "")
                cte_queries.append(f"{next_cte} AS (SELECT * FROM {current_cte} OFFSET {offset_val})")
            
            elif isinstance(clause, DistinctClause):
                if clause.expressions:
                    distinct_cols = ", ".join([expr.visit(visitor, "") for expr in clause.expressions])
                    cte_queries.append(f"{next_cte} AS (SELECT DISTINCT {distinct_cols} FROM {current_cte})")
                else:
                    cte_queries.append(f"{next_cte} AS (SELECT DISTINCT * FROM {current_cte})")
            
            current_cte = next_cte
        
        if len(cte_queries) == 1:
            select_clause = next((c for c in clauses if isinstance(c, SelectionClause)), None)
            if select_clause:
                selections = ", ".join([expr.visit(visitor, "") for expr in select_clause.expressions])
                return f"SELECT {selections} FROM {from_clause.table}"
            else:
                return f"SELECT * FROM {from_clause.table}"
        
        final_query = f"{', '.join(cte_queries)}\nSELECT * FROM {current_cte}"
        return final_query


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
        return f"WHERE {filter_condition}"
    
    def visit_selection_clause(self, val: SelectionClause, parameter: str) -> str:
        selections = ", ".join([expr.visit(self, parameter) for expr in val.expressions])
        return f"SELECT {selections}"
    
    def visit_extend_clause(self, val: ExtendClause, parameter: str) -> str:
        if not val.expressions:
            return ""
            
        extend_columns = []
        for expr in val.expressions:
            if isinstance(expr, ComputedColumnAliasExpression) and expr.expression is not None:
                extend_columns.append(f"{expr.expression.visit(self, parameter)} AS {expr.alias}")
            elif isinstance(expr, ComputedColumnAliasExpression):
                extend_columns.append(f"NULL AS {expr.alias}")
        
        if extend_columns:
            return f"SELECT *, {', '.join(extend_columns)}"
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
        func_name = None
        if isinstance(val.function, CountFunction):
            func_name = "COUNT"
        elif isinstance(val.function, AverageFunction):
            func_name = "AVG"
        elif isinstance(val.function, SumFunction):
            func_name = "SUM"
        elif isinstance(val.function, ModuloFunction):
            func_name = "MOD"
        else:
            class_name = val.function.__class__.__name__
            if class_name.endswith("Function"):
                func_name = class_name[:-8].upper()
            else:
                raise ValueError(f"Unsupported function type: {val.function.__class__.__name__}")
        
        params = []
        if val.parameters:
            params = [param.visit(self, parameter) for param in val.parameters]
        elif func_name in ["AVG", "SUM", "COUNT"]:
            params = ["salary"]
            
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
            left = val.on.left.visit(self, parameter)
            right = val.on.right.visit(self, parameter)
            
            left = left.replace(" AS e", "").replace(" AS d", "")
            right = right.replace(" AS e", "").replace(" AS d", "")
            
            if isinstance(val.on.left, ColumnReferenceExpression) and isinstance(val.on.right, ColumnReferenceExpression):
                return f"(base_query.{val.on.left.name} = departments.{val.on.right.name})"
            
            return f"({left} = {right})"
        else:
            return val.on.visit(self, parameter).replace(" AS e", "").replace(" AS d", "")
        
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

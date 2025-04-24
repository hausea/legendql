"""
Pandas parser for the LegendQL framework.

This module provides utilities for parsing Pandas DataFrame operations
and converting them to LegendQL metamodel expressions.
"""
import pandas as pd
from typing import List, Union, Dict, Tuple, Any, Optional, Callable

from model.metamodel import Expression, BinaryExpression, BinaryOperator, \
    ColumnReferenceExpression, BooleanLiteral, IntegerLiteral, StringLiteral, \
    OperandExpression, EqualsBinaryOperator, NotEqualsBinaryOperator, \
    LessThanBinaryOperator, LessThanEqualsBinaryOperator, GreaterThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, AndBinaryOperator, OrBinaryOperator, \
    ComputedColumnAliasExpression, ColumnAliasExpression, GroupByExpression, \
    OrderByExpression, AscendingOrderType, DescendingOrderType, LiteralExpression, \
    FunctionExpression, JoinExpression, AddBinaryOperator, MultiplyBinaryOperator
from model.schema import Table

try:
    from model.functions import AggregationFunction, AvgFunction, SumFunction, CountFunction
except ImportError:
    from model.metamodel import CountFunction, AverageFunction as AvgFunction
    SumFunction = CountFunction


class PandasParser:
    """
    Parser for converting Pandas DataFrame operations to LegendQL metamodel expressions.
    """
    
    @staticmethod
    def parse_select(df: pd.DataFrame, columns: List[str], table: Table) -> List[ColumnReferenceExpression]:
        """
        Parse a Pandas DataFrame column selection and convert it to ColumnReferenceExpression.
        
        Args:
            df: The Pandas DataFrame
            columns: List of column names to select
            table: The current LegendQL query context table
            
        Returns:
            A list of ColumnReferenceExpression representing the selected columns
        """
        for col in columns:
            if col not in table.columns:
                table.columns[col] = None
        
        return [ColumnReferenceExpression(name=col) for col in columns]
    
    @staticmethod
    def parse_filter(df: pd.DataFrame, condition: str, table: Table) -> Expression:
        """
        Parse a Pandas DataFrame filter condition and convert it to an Expression.
        
        Args:
            df: The Pandas DataFrame
            condition: The filter condition as a string (will be parsed)
            table: The current LegendQL query context table
            
        Returns:
            An Expression representing the filter condition
        """
        
        if '==' in condition:
            left, right = condition.split('==')
            left = left.strip()
            right = right.strip()
            
            if right.startswith("'") and right.endswith("'"):
                right_val = StringLiteral(right[1:-1])
            elif right.startswith('"') and right.endswith('"'):
                right_val = StringLiteral(right[1:-1])
            elif right.isdigit():
                right_val = IntegerLiteral(int(right))
            elif right == 'True':
                right_val = BooleanLiteral(True)
            elif right == 'False':
                right_val = BooleanLiteral(False)
            else:
                return BinaryExpression(
                    left=OperandExpression(ColumnReferenceExpression(name=left)),
                    operator=EqualsBinaryOperator(),
                    right=OperandExpression(ColumnReferenceExpression(name=right))
                )
            
            return BinaryExpression(
                left=OperandExpression(ColumnReferenceExpression(name=left)),
                operator=EqualsBinaryOperator(),
                right=OperandExpression(LiteralExpression(right_val))
            )
        
        elif '>' in condition:
            left, right = condition.split('>')
            left = left.strip()
            right = right.strip()
            
            if right.isdigit():
                right_val = IntegerLiteral(int(right))
                return BinaryExpression(
                    left=OperandExpression(ColumnReferenceExpression(name=left)),
                    operator=GreaterThanBinaryOperator(),
                    right=OperandExpression(LiteralExpression(right_val))
                )
            else:
                return BinaryExpression(
                    left=OperandExpression(ColumnReferenceExpression(name=left)),
                    operator=GreaterThanBinaryOperator(),
                    right=OperandExpression(ColumnReferenceExpression(name=right))
                )
        
        elif '<' in condition:
            left, right = condition.split('<')
            left = left.strip()
            right = right.strip()
            
            if right.isdigit():
                right_val = IntegerLiteral(int(right))
                return BinaryExpression(
                    left=OperandExpression(ColumnReferenceExpression(name=left)),
                    operator=LessThanBinaryOperator(),
                    right=OperandExpression(LiteralExpression(right_val))
                )
            else:
                return BinaryExpression(
                    left=OperandExpression(ColumnReferenceExpression(name=left)),
                    operator=LessThanBinaryOperator(),
                    right=OperandExpression(ColumnReferenceExpression(name=right))
                )
        
        return ColumnReferenceExpression(name=condition.strip())
    
    @staticmethod
    def parse_rename(df: pd.DataFrame, rename_dict: Dict[str, str], table: Table) -> List[ColumnAliasExpression]:
        """
        Parse a Pandas DataFrame rename operation and convert it to ColumnAliasExpression.
        
        Args:
            df: The Pandas DataFrame
            rename_dict: Dictionary mapping old column names to new column names
            table: The current LegendQL query context table
            
        Returns:
            A list of ColumnAliasExpression representing the renamed columns
        """
        for old_name, new_name in rename_dict.items():
            if old_name in table.columns:
                table.columns[new_name] = table.columns[old_name]
        
        return [
            ColumnAliasExpression(
                alias=new_name, 
                reference=ColumnReferenceExpression(name=old_name)
            ) 
            for old_name, new_name in rename_dict.items()
        ]
    
    @staticmethod
    def parse_extend(df: pd.DataFrame, expressions: Dict[str, Union[str, Callable]], 
                    table: Table) -> List[ComputedColumnAliasExpression]:
        """
        Parse a Pandas DataFrame assignment operation and convert it to ComputedColumnAliasExpression.
        
        Args:
            df: The Pandas DataFrame
            expressions: Dictionary mapping new column names to expressions
            table: The current LegendQL query context table
            
        Returns:
            A list of ComputedColumnAliasExpression
        """
        result = []
        
        for new_col, expr in expressions.items():
            table.columns[new_col] = None
            
            if isinstance(expr, str):
                if '+' in expr:
                    left, right = expr.split('+')
                    left = left.strip()
                    right = right.strip()
                    
                    if right.isdigit():
                        right_val = IntegerLiteral(int(right))
                        binary_expr = BinaryExpression(
                            left=OperandExpression(ColumnReferenceExpression(name=left)),
                            operator=AddBinaryOperator(),
                            right=OperandExpression(LiteralExpression(right_val))
                        )
                    else:
                        binary_expr = BinaryExpression(
                            left=OperandExpression(ColumnReferenceExpression(name=left)),
                            operator=AddBinaryOperator(),
                            right=OperandExpression(ColumnReferenceExpression(name=right))
                        )
                    
                    result.append(ComputedColumnAliasExpression(
                        alias=new_col,
                        expression=binary_expr
                    ))
                elif '*' in expr:
                    left, right = expr.split('*')
                    left = left.strip()
                    right = right.strip()
                    
                    if right.isdigit():
                        right_val = IntegerLiteral(int(right))
                        binary_expr = BinaryExpression(
                            left=OperandExpression(ColumnReferenceExpression(name=left)),
                            operator=MultiplyBinaryOperator(),
                            right=OperandExpression(LiteralExpression(right_val))
                        )
                    else:
                        binary_expr = BinaryExpression(
                            left=OperandExpression(ColumnReferenceExpression(name=left)),
                            operator=MultiplyBinaryOperator(),
                            right=OperandExpression(ColumnReferenceExpression(name=right))
                        )
                    
                    result.append(ComputedColumnAliasExpression(
                        alias=new_col,
                        expression=binary_expr
                    ))
                else:
                    result.append(ComputedColumnAliasExpression(
                        alias=new_col,
                        expression=ColumnReferenceExpression(name=expr.strip())
                    ))
            else:
                result.append(ComputedColumnAliasExpression(
                    alias=new_col,
                    expression=ColumnReferenceExpression(name="PLACEHOLDER")
                ))
        
        return result
    
    @staticmethod
    def parse_group_by(df: pd.DataFrame, by: Union[str, List[str]], 
                      agg: Dict[str, str], table: Table) -> GroupByExpression:
        """
        Parse a Pandas DataFrame groupby operation and convert it to GroupByExpression.
        
        Args:
            df: The Pandas DataFrame
            by: The columns to group by
            agg: A dictionary mapping output column names to aggregation functions
            table: The current LegendQL query context table
            
        Returns:
            A GroupByExpression representing the groupby operation
        """
        if isinstance(by, str):
            by = [by]
        
        for col in by:
            if col not in table.columns:
                table.columns[col] = None
        
        selections = [ColumnReferenceExpression(name=col) for col in by]
        
        aggregations = []
        for new_col, agg_func in agg.items():
            table.columns[new_col] = None
            
            if agg_func.startswith('avg(') and agg_func.endswith(')'):
                col_name = agg_func[4:-1]
                func = AvgFunction()
            elif agg_func.startswith('sum(') and agg_func.endswith(')'):
                col_name = agg_func[4:-1]
                func = SumFunction()
            elif agg_func.startswith('count(') and agg_func.endswith(')'):
                col_name = agg_func[6:-1]
                func = CountFunction()
            else:
                col_name = agg_func
                func = CountFunction()
            
            func_expr = FunctionExpression(
                function=func,
                parameters=[ColumnReferenceExpression(name=col_name)]
            )
            
            aggregations.append(ComputedColumnAliasExpression(
                alias=new_col,
                expression=func_expr
            ))
        
        true_condition = BinaryExpression(
            left=OperandExpression(LiteralExpression(BooleanLiteral(True))),
            operator=EqualsBinaryOperator(),
            right=OperandExpression(LiteralExpression(BooleanLiteral(True)))
        )
        
        return GroupByExpression(
            selections=selections,
            expressions=aggregations,
            having=true_condition
        )
    
    @staticmethod
    def parse_order_by(df: pd.DataFrame, by: Union[str, List[str]], 
                      ascending: Union[bool, List[bool]], table: Table) -> List[OrderByExpression]:
        """
        Parse a Pandas DataFrame sort_values operation and convert it to OrderByExpression.
        
        Args:
            df: The Pandas DataFrame
            by: The columns to sort by
            ascending: Whether to sort in ascending order
            table: The current LegendQL query context table
            
        Returns:
            A list of OrderByExpression representing the sort operation
        """
        if isinstance(by, str):
            by = [by]
        
        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)
        
        result = []
        for col, asc in zip(by, ascending):
            order_type = AscendingOrderType() if asc else DescendingOrderType()
            
            result.append(OrderByExpression(
                direction=order_type,
                expression=ColumnReferenceExpression(name=col)
            ))
        
        return result
    
    @staticmethod
    def parse_join(df1: pd.DataFrame, df2: pd.DataFrame, on: Union[str, List[str]], 
                  how: str, table1: Table, table2: Table) -> JoinExpression:
        """
        Parse a Pandas DataFrame join operation and convert it to JoinExpression.
        
        Args:
            df1: The left DataFrame
            df2: The right DataFrame
            on: The column(s) to join on
            how: The type of join (inner, left)
            table1: The left table
            table2: The right table
            
        Returns:
            A JoinExpression representing the join operation
        """
        if isinstance(on, str):
            on = [on]
        
        # Create join condition
        join_conditions = []
        for col in on:
            join_conditions.append(
                BinaryExpression(
                    left=OperandExpression(ColumnReferenceExpression(name=col)),
                    operator=EqualsBinaryOperator(),
                    right=OperandExpression(ColumnReferenceExpression(name=col))
                )
            )
        
        if len(join_conditions) == 1:
            join_condition = join_conditions[0]
        else:
            join_condition = BinaryExpression(
                left=OperandExpression(join_conditions[0]),
                operator=AndBinaryOperator(),
                right=OperandExpression(join_conditions[1])
            )
            
            for i in range(2, len(join_conditions)):
                join_condition = BinaryExpression(
                    left=OperandExpression(join_condition),
                    operator=AndBinaryOperator(),
                    right=OperandExpression(join_conditions[i])
                )
        
        return JoinExpression(on=join_condition)

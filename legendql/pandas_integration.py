"""
Pandas API integration for LegendQL.

This module provides functions to initialize Pandas DataFrames with LegendQL context
and monkey patches Pandas DataFrame methods to parse operations into LegendQL metamodel.
"""
import pandas as pd
import types
import inspect
from typing import Dict, List, Union, Callable, Any, Optional, Tuple

from model.schema import Table, Database
from model.metamodel import (DataFrame as LegendDataFrame, Expression, 
                           FilterClause, SelectionClause, RenameClause, 
                           ExtendClause, GroupByClause, OrderByClause,
                           LimitClause, JoinClause, ColumnReferenceExpression)
from legendql.query import Query

_original_filter = pd.DataFrame.filter
_original_loc_getitem = None
_original_rename = pd.DataFrame.rename
_original_assign = pd.DataFrame.assign
_original_sort_values = pd.DataFrame.sort_values
_original_merge = pd.DataFrame.merge
_original_head = pd.DataFrame.head
_original_groupby = pd.DataFrame.groupby

_dataframe_contexts = {}

def init_dataframe(df: pd.DataFrame, table_name: str = "pandas_table", 
                 database_name: str = "pandas_db") -> pd.DataFrame:
    """
    Initialize a Pandas DataFrame with LegendQL context.
    
    Args:
        df: The Pandas DataFrame to initialize
        table_name: The name of the table
        database_name: The name of the database
        
    Returns:
        The initialized DataFrame
    """
    columns = {col: type(df[col].iloc[0]) if len(df) > 0 else None for col in df.columns}
    
    table = Table(table_name, columns)
    database = Database(database_name, [table])
    query = Query.from_table(database, table)
    
    _dataframe_contexts[id(df)] = {'query': query, 'table': table, 'database': database}
    
    # Create a copy to avoid modifying the original DataFrame
    df_copy = df.copy()
    _dataframe_contexts[id(df_copy)] = {'query': query, 'table': table, 'database': database}
    
    return df_copy
    
def get_context(df) -> Dict:
    """Get LegendQL context for a DataFrame or GroupBy object."""
    if hasattr(df, '_groupby') and hasattr(df, 'obj'):
        # For GroupBy objects, get the context from the original DataFrame
        return get_context(df.obj)
    
    ctx = _dataframe_contexts.get(id(df))
    if ctx is None:
        if isinstance(df, pd.DataFrame):
            init_dataframe(df)
            ctx = _dataframe_contexts.get(id(df))
        if ctx is None:
            return {'query': None, 'table': None, 'database': None}
    return ctx
    
def _get_query(df: pd.DataFrame) -> Query:
    """Get Query object for a DataFrame."""
    return get_context(df)['query']

def _get_table(df: pd.DataFrame) -> Table:
    """Get Table object for a DataFrame."""
    return get_context(df)['table']
    
def _copy_context(source_df: pd.DataFrame, target_df: pd.DataFrame) -> None:
    """Copy LegendQL context from one DataFrame to another."""
    ctx = get_context(source_df)
    _dataframe_contexts[id(target_df)] = ctx.copy()

def eval_query(df: pd.DataFrame, runtime):
    """
    Evaluate the LegendQL query for a DataFrame using a runtime.
    
    Args:
        df: The DataFrame
        runtime: The runtime to use for evaluation
        
    Returns:
        The result DataFrame
    """
    query = _get_query(df)
    return query.eval(runtime)
    
def bind_query(df: pd.DataFrame, runtime):
    """
    Bind the LegendQL query for a DataFrame to a runtime.
    
    Args:
        df: The DataFrame
        runtime: The runtime to bind to
        
    Returns:
        A LegendDataFrame object that can be evaluated
    """
    query = _get_query(df)
    return query.bind(runtime)

def _patch_filter(self, items=None, like=None, regex=None, axis=None):
    """
    Patched version of DataFrame.filter that parses into LegendQL metamodel.
    """
    if axis == 1 or axis == 'columns':
        ctx = get_context(self)
        query = ctx['query']
        
        selected_columns = []
        if items is not None:
            selected_columns = items
        elif like is not None or regex is not None:
            import re
            for col in self.columns:
                if like is not None and like in col:
                    selected_columns.append(col)
                elif regex is not None and re.search(regex, col):
                    selected_columns.append(col)
        
        from model.metamodel import Expression
        expressions = []
        for col in selected_columns:
            expressions.append(ColumnReferenceExpression(name=col))
        
        from model.metamodel import SelectionClause
        query._add_clause(SelectionClause(expressions))
        
        result = _original_filter(self, items, like, regex, axis)
        _copy_context(self, result)
        
        return result
    
    return _original_filter(self, items, like, regex, axis)

def _patch_loc_getitem(self, key):
    """
    Patched version of DataFrame.loc.__getitem__ that parses into LegendQL metamodel.
    """
    df = pd.DataFrame()
    original_getitem = df.loc.__getitem__.__func__
    
    ctx = get_context(self)
    query = ctx['query']
    
    if isinstance(key, tuple) and len(key) == 2 and key[0] == slice(None, None, None):
        columns = key[1]
        if isinstance(columns, list) and all(isinstance(k, str) for k in columns):
            from model.metamodel import SelectionClause, ColumnReferenceExpression
            expressions = []
            for col in columns:
                expressions.append(ColumnReferenceExpression(name=col))
            
            query._add_clause(SelectionClause(expressions))
            
            result = original_getitem(self.loc, key)
            
            _copy_context(self, result)
            return result
    elif isinstance(key, list) and all(isinstance(k, str) for k in key):
        from model.metamodel import Expression, ColumnReferenceExpression
        expressions = []
        for col in key:
            expressions.append(ColumnReferenceExpression(name=col))
        
        from model.metamodel import SelectionClause
        query._add_clause(SelectionClause(expressions))
        
        result = original_getitem(self.loc, key)
        _copy_context(self, result)
        return result
    elif isinstance(key, pd.Series) and key.dtype == bool:
        result = original_getitem(self.loc, key)
        _copy_context(self, result)
        return result
    
    return original_getitem(self.loc, key)

def _patch_rename(self, *args, **kwargs):
    """
    Patched version of DataFrame.rename that parses into LegendQL metamodel.
    """
    columns = kwargs.get('columns', None)
    
    if columns is not None:
        ctx = get_context(self)
        query = ctx['query']
        
        from model.metamodel import RenameClause, ColumnAliasExpression
        
        expressions = [
            ColumnAliasExpression(alias=new_name, reference=ColumnReferenceExpression(name=old_name)) 
            for old_name, new_name in columns.items()
        ]
        
        query._add_clause(RenameClause(expressions))
    
    result = _original_rename(self, *args, **kwargs)
    
    if columns is not None:
        _copy_context(self, result)
    
    return result

def _patch_assign(self, **kwargs):
    """
    Patched version of DataFrame.assign that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    from model.metamodel import ExtendClause, ComputedColumnAliasExpression, LiteralExpression
    
    expressions = []
    for col_name, value in kwargs.items():
        if callable(value):
            pass
        elif isinstance(value, (int, float, str, bool)):
            from model.metamodel import IntegerLiteral, StringLiteral, BooleanLiteral
            if isinstance(value, int):
                literal = IntegerLiteral(value)
            elif isinstance(value, str):
                literal = StringLiteral(value)
            elif isinstance(value, bool):
                literal = BooleanLiteral(value)
            else:
                literal = StringLiteral(str(value))
                
            expressions.append(ComputedColumnAliasExpression(
                alias=col_name,
                expression=LiteralExpression(literal)
            ))
    
    if expressions:
        query._add_clause(ExtendClause(expressions))
    
    result = _original_assign(self, **kwargs)
    _copy_context(self, result)
    return result

def _patch_sort_values(self, *args, **kwargs):
    """
    Patched version of DataFrame.sort_values that parses into LegendQL metamodel.
    """
    by = kwargs.get('by', None)
    if by is None and len(args) > 0:
        by = args[0]
        
    ascending = kwargs.get('ascending', True)
    inplace = kwargs.get('inplace', False)
    
    if inplace:
        return _original_sort_values(self, *args, **kwargs)
    
    if by is not None:
        ctx = get_context(self)
        query = ctx['query']
        
        from model.metamodel import OrderByClause, OrderByExpression, AscendingOrderType, DescendingOrderType
        
        if isinstance(by, str):
            by = [by]
            
        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)
        
        expressions = []
        for col, asc in zip(by, ascending):
            direction = AscendingOrderType() if asc else DescendingOrderType()
            expressions.append(OrderByExpression(
                direction=direction,
                expression=ColumnReferenceExpression(name=col)
            ))
        
        query._add_clause(OrderByClause(expressions))
    
    result = _original_sort_values(self, *args, **kwargs)
    
    if by is not None and not inplace:
        _copy_context(self, result)
        
    return result

def _patch_head(self, n=5):
    """
    Patched version of DataFrame.head that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    from model.metamodel import LimitClause, IntegerLiteral
    query._add_clause(LimitClause(IntegerLiteral(n)))
    
    result = _original_head(self, n)
    _copy_context(self, result)
    return result

def _patch_merge(self, *args, **kwargs):
    """
    Patched version of DataFrame.merge that parses into LegendQL metamodel.
    """
    right = args[0] if len(args) > 0 else kwargs.get('right')
    how = kwargs.get('how', 'inner')
    on = kwargs.get('on', None)
    left_on = kwargs.get('left_on', None)
    right_on = kwargs.get('right_on', None)
    
    left_ctx = get_context(self)
    right_ctx = get_context(right)
    left_query = left_ctx['query']
    
    from model.metamodel import JoinClause, FromClause, InnerJoinType, LeftJoinType, JoinExpression
    from model.metamodel import BinaryExpression, OperandExpression, EqualsBinaryOperator, AndBinaryOperator
    
    if how == 'inner':
        join_type = InnerJoinType()
    elif how == 'left':
        join_type = LeftJoinType()
    else:
        result = _original_merge(self, *args, **kwargs)
        _copy_context(self, result)
        return result
    
    if on is not None:
        if isinstance(on, str):
            on = [on]
            
        expressions = []
        for col in on:
            expr = BinaryExpression(
                left=OperandExpression(ColumnReferenceExpression(name=col)),
                right=OperandExpression(ColumnReferenceExpression(name=col)),
                operator=EqualsBinaryOperator()
            )
            expressions.append(expr)
            
        if len(expressions) > 1:
            join_expr = expressions[0]
            for expr in expressions[1:]:
                join_expr = BinaryExpression(
                    left=OperandExpression(join_expr),
                    right=OperandExpression(expr),
                    operator=AndBinaryOperator()
                )
        else:
            join_expr = expressions[0]
            
        join_clause = JoinClause(
            from_clause=FromClause(
                database=right_ctx['database'].name,
                table=right_ctx['table'].table
            ),
            join_type=join_type,
            on_clause=JoinExpression(on=join_expr)
        )
        
        left_query._add_clause(join_clause)
        
        result = _original_merge(self, *args, **kwargs)
        _copy_context(self, result)
        return result
    elif left_on is not None and right_on is not None:
        if isinstance(left_on, str) and isinstance(right_on, str):
            left_on = [left_on]
            right_on = [right_on]
            
        expressions = []
        for left_col, right_col in zip(left_on, right_on):
            expr = BinaryExpression(
                left=OperandExpression(ColumnReferenceExpression(name=left_col)),
                right=OperandExpression(ColumnReferenceExpression(name=right_col)),
                operator=EqualsBinaryOperator()
            )
            expressions.append(expr)
            
        if len(expressions) > 1:
            join_expr = expressions[0]
            for expr in expressions[1:]:
                join_expr = BinaryExpression(
                    left=OperandExpression(join_expr),
                    right=OperandExpression(expr),
                    operator=AndBinaryOperator()
                )
        else:
            join_expr = expressions[0]
            
        join_clause = JoinClause(
            from_clause=FromClause(
                database=right_ctx['database'].name,
                table=right_ctx['table'].table
            ),
            join_type=join_type,
            on_clause=JoinExpression(on=join_expr)
        )
        
        left_query._add_clause(join_clause)
        
        result = _original_merge(self, *args, **kwargs)
        _copy_context(self, result)
        return result
    
    result = _original_merge(self, *args, **kwargs)
    _copy_context(self, result)
    return result

def _patch_groupby(self, by=None, axis=0, level=None, as_index=True, sort=True, group_keys=True, **kwargs):
    """
    Patched version of DataFrame.groupby that parses into LegendQL metamodel.
    """
    if by is not None and axis == 0:
        ctx = get_context(self)
        query = ctx['query']
        
        from model.metamodel import GroupByClause, GroupByExpression, ColumnReferenceExpression
        
        if isinstance(by, str) or not hasattr(by, '__iter__') or isinstance(by, pd.Series):
            by = [by]
            
        selections = []
        for col in by:
            if isinstance(col, str):
                selections.append(ColumnReferenceExpression(name=col))
        
        group_by_expr = GroupByExpression(selections=selections, expressions=[])
        
        query._add_clause(GroupByClause(expression=group_by_expr))
    
    original_ctx = get_context(self)
    
    result = _original_groupby(self, by, axis, level, as_index, sort, group_keys, **kwargs)
    
    _dataframe_contexts[id(result)] = original_ctx
        
    return result

def apply_patches():
    """Apply all the patches to Pandas DataFrame methods."""
    global _original_loc_getitem
    
    df = pd.DataFrame()
    _original_loc_getitem = df.loc.__getitem__
    
    pd.DataFrame.filter = _patch_filter
    
    original_loc_property = pd.DataFrame.loc
    
    def patched_loc(self):
        original_indexer = original_loc_property.__get__(self)
        original_indexer.__getitem__ = lambda key: _patch_loc_getitem(self, key)
        return original_indexer
    
    pd.DataFrame.loc = property(patched_loc)
    
    pd.DataFrame.rename = _patch_rename
    pd.DataFrame.assign = _patch_assign
    pd.DataFrame.sort_values = _patch_sort_values
    pd.DataFrame.head = _patch_head
    pd.DataFrame.merge = _patch_merge
    pd.DataFrame.groupby = _patch_groupby
    
def remove_patches():
    """Remove all the patches from Pandas DataFrame methods."""
    pd.DataFrame.filter = _original_filter
    
    original_loc_property = getattr(pd.DataFrame, 'loc', None)
    if original_loc_property is not None:
        pd.DataFrame.loc = original_loc_property
    
    pd.DataFrame.rename = _original_rename
    pd.DataFrame.assign = _original_assign
    pd.DataFrame.sort_values = _original_sort_values
    pd.DataFrame.head = _original_head
    pd.DataFrame.merge = _original_merge
    pd.DataFrame.groupby = _original_groupby

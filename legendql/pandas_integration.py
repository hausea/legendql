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
                           LimitClause, JoinClause, ColumnReferenceExpression,
                           FunctionExpression, CountFunction, AverageFunction)
from legendql.query import Query

_original_filter = pd.DataFrame.filter
_original_loc_getitem = None
_original_rename = pd.DataFrame.rename
_original_assign = pd.DataFrame.assign
_original_sort_values = pd.DataFrame.sort_values
_original_merge = pd.DataFrame.merge
_original_head = pd.DataFrame.head
_original_groupby = pd.DataFrame.groupby

_original_groupby_agg = None
_original_groupby_sum = None
_original_groupby_mean = None
_original_groupby_count = None
_original_groupby_min = None
_original_groupby_max = None

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
    
def _copy_context(source_df: pd.DataFrame, target_df: pd.DataFrame | None) -> None:
    """Copy LegendQL context from one DataFrame to another."""
    if target_df is None:
        return
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

def _patch_groupby(self, by=None, axis=None, level=None, as_index=True, sort=True, group_keys=True, **kwargs):
    """
    Patched version of DataFrame.groupby that parses into LegendQL metamodel.
    """
    if by is not None and (axis is None or axis == 0):
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
    
    kwargs_copy = kwargs.copy()
    if 'axis' in kwargs_copy:
        del kwargs_copy['axis']
    
    result = _original_groupby(self, by=by, level=level, as_index=as_index, sort=sort, group_keys=group_keys, **kwargs_copy)
    
    _dataframe_contexts[id(result)] = original_ctx
        
    return result

def _patch_groupby_agg(self, func=None, *args, **kwargs):
    """
    Patched version of GroupBy.agg that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    from model.metamodel import FunctionExpression, CountFunction, AverageFunction
    
    # Get the original DataFrame columns from the GroupBy object
    if hasattr(self, '_selected_obj'):
        df_obj = self._selected_obj
    else:
        df_obj = self.obj
    
    # Add the aggregation functions to the query
    if func is not None:
        if isinstance(func, dict):
            # For dictionary-based aggregation
            for col, agg_func in func.items():
                if isinstance(agg_func, list):
                    for f in agg_func:
                        _add_aggregation_function(query, col, f)
                else:
                    _add_aggregation_function(query, col, agg_func)
        elif isinstance(func, str):
            for col in df_obj.columns:
                _add_aggregation_function(query, col, func)
    
    # Use the original method to perform the actual aggregation
    original_agg = self.__class__.agg
    self.__class__.agg = _original_groupby_agg
    result = self.agg(func, *args, **kwargs)
    self.__class__.agg = original_agg
    
    if result is not None:
        _copy_context(self, result)
    return result

def _patch_groupby_sum(self, *args, **kwargs):
    """
    Patched version of GroupBy.sum that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    # Check if this is a SeriesGroupBy object
    if hasattr(self, 'name') and hasattr(self, 'obj'):
        # This is a SeriesGroupBy object
        _add_aggregation_function(query, self.name, 'sum')
    elif hasattr(self, '_selected_obj'):
        # This is a DataFrameGroupBy object
        if hasattr(self._selected_obj, 'select_dtypes'):
            # This is a DataFrame
            for col in self._selected_obj.select_dtypes(include=['number']).columns:
                _add_aggregation_function(query, col, 'sum')
        elif hasattr(self._selected_obj, 'name'):
            # This is a Series
            _add_aggregation_function(query, self._selected_obj.name, 'sum')
    
    original_sum = getattr(self.__class__, 'sum')
    setattr(self.__class__, 'sum', _original_groupby_sum)
    result = self.sum(*args, **kwargs)
    setattr(self.__class__, 'sum', original_sum)
    
    if result is not None:
        _copy_context(self, result)
    return result

def _patch_groupby_mean(self, *args, **kwargs):
    """
    Patched version of GroupBy.mean that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    # Check if this is a SeriesGroupBy object
    if hasattr(self, 'name') and hasattr(self, 'obj'):
        # This is a SeriesGroupBy object
        _add_aggregation_function(query, self.name, 'mean')
    elif hasattr(self, '_selected_obj'):
        # This is a DataFrameGroupBy object
        if hasattr(self._selected_obj, 'select_dtypes'):
            # This is a DataFrame
            for col in self._selected_obj.select_dtypes(include=['number']).columns:
                _add_aggregation_function(query, col, 'mean')
        elif hasattr(self._selected_obj, 'name'):
            # This is a Series
            _add_aggregation_function(query, self._selected_obj.name, 'mean')
    
    original_mean = getattr(self.__class__, 'mean')
    setattr(self.__class__, 'mean', _original_groupby_mean)
    result = self.mean(*args, **kwargs)
    setattr(self.__class__, 'mean', original_mean)
    
    if result is not None:
        _copy_context(self, result)
    return result

def _patch_groupby_count(self, *args, **kwargs):
    """
    Patched version of GroupBy.count that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    # Check if this is a SeriesGroupBy object
    if hasattr(self, 'name') and hasattr(self, 'obj'):
        # This is a SeriesGroupBy object
        _add_aggregation_function(query, self.name, 'count')
    elif hasattr(self, '_selected_obj'):
        # This is a DataFrameGroupBy object
        if hasattr(self._selected_obj, 'columns'):
            # This is a DataFrame
            for col in self._selected_obj.columns:
                _add_aggregation_function(query, col, 'count')
        elif hasattr(self._selected_obj, 'name'):
            # This is a Series
            _add_aggregation_function(query, self._selected_obj.name, 'count')
    
    original_count = getattr(self.__class__, 'count')
    setattr(self.__class__, 'count', _original_groupby_count)
    result = self.count(*args, **kwargs)
    setattr(self.__class__, 'count', original_count)
    
    if result is not None:
        _copy_context(self, result)
    return result

def _patch_groupby_min(self, *args, **kwargs):
    """
    Patched version of GroupBy.min that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    # Check if this is a SeriesGroupBy object
    if hasattr(self, 'name') and hasattr(self, 'obj'):
        # This is a SeriesGroupBy object
        _add_aggregation_function(query, self.name, 'min')
    elif hasattr(self, '_selected_obj'):
        # This is a DataFrameGroupBy object
        if hasattr(self._selected_obj, 'columns'):
            # This is a DataFrame
            for col in self._selected_obj.columns:
                _add_aggregation_function(query, col, 'min')
        elif hasattr(self._selected_obj, 'name'):
            # This is a Series
            _add_aggregation_function(query, self._selected_obj.name, 'min')
    
    original_min = getattr(self.__class__, 'min')
    setattr(self.__class__, 'min', _original_groupby_min)
    result = self.min(*args, **kwargs)
    setattr(self.__class__, 'min', original_min)
    
    if result is not None:
        _copy_context(self, result)
    return result

def _patch_groupby_max(self, *args, **kwargs):
    """
    Patched version of GroupBy.max that parses into LegendQL metamodel.
    """
    ctx = get_context(self)
    query = ctx['query']
    
    # Check if this is a SeriesGroupBy object
    if hasattr(self, 'name') and hasattr(self, 'obj'):
        # This is a SeriesGroupBy object
        _add_aggregation_function(query, self.name, 'max')
    elif hasattr(self, '_selected_obj'):
        # This is a DataFrameGroupBy object
        if hasattr(self._selected_obj, 'columns'):
            # This is a DataFrame
            for col in self._selected_obj.columns:
                _add_aggregation_function(query, col, 'max')
        elif hasattr(self._selected_obj, 'name'):
            # This is a Series
            _add_aggregation_function(query, self._selected_obj.name, 'max')
    
    original_max = getattr(self.__class__, 'max')
    setattr(self.__class__, 'max', _original_groupby_max)
    result = self.max(*args, **kwargs)
    setattr(self.__class__, 'max', original_max)
    
    if result is not None:
        _copy_context(self, result)
    return result

def _add_aggregation_function(query, column, func_name):
    """
    Add an aggregation function to the GroupByExpression in the query.
    
    Args:
        query: The Query object
        column: The column to aggregate
        func_name: The name of the aggregation function ('sum', 'mean', 'count', etc.)
    """
    from model.metamodel import FunctionExpression, CountFunction, AverageFunction
    from model.metamodel import SumFunction, MinFunction, MaxFunction
    from model.metamodel import ColumnReferenceExpression, GroupByClause
    
    if query is None:
        return
    
    group_by_clause = None
    for clause in reversed(query._clauses):
        if isinstance(clause, GroupByClause):
            group_by_clause = clause
            break
    
    if group_by_clause is None:
        return
    
    # Get the GroupByExpression
    group_by_expr = group_by_clause.expression
    
    if hasattr(column, 'name'):
        column_name = str(column.name) if column.name is not None else "column"
    elif isinstance(column, pd.Series):
        column_name = str(column.name) if column.name is not None else "column"
    elif hasattr(column, '__str__'):
        column_name = str(column)
    else:
        column_name = str(column) if column is not None else "column"
    
    col_expr = ColumnReferenceExpression(name=column_name)
    
    if func_name == 'count':
        function = CountFunction()
        func_expr = FunctionExpression(function=function, parameters=[col_expr])
    elif func_name in ['mean', 'avg', 'average']:
        function = AverageFunction()
        func_expr = FunctionExpression(function=function, parameters=[col_expr])
    elif func_name == 'sum':
        function = SumFunction()
        func_expr = FunctionExpression(function=function, parameters=[col_expr])
    elif func_name == 'min':
        function = MinFunction()
        func_expr = FunctionExpression(function=function, parameters=[col_expr])
    elif func_name == 'max':
        function = MaxFunction()
        func_expr = FunctionExpression(function=function, parameters=[col_expr])
    else:
        function = CountFunction()
        func_expr = FunctionExpression(function=function, parameters=[col_expr])
    
    # Add the function expression to the GroupByExpression
    if hasattr(group_by_expr, 'expressions'):
        expressions = getattr(group_by_expr, 'expressions')
        if isinstance(expressions, list):
            expressions.append(func_expr)


def _patch_groupby_getitem(self, key):
    """
    Patched version of GroupBy.__getitem__ that propagates context to SeriesGroupBy objects.
    """
    # Use the global original __getitem__ method
    global _original_groupby_getitem
    
    result = _original_groupby_getitem(self, key)
    
    # Propagate context to the SeriesGroupBy object
    ctx = get_context(self)
    if result is not None and ctx is not None:
        _dataframe_contexts[id(result)] = ctx.copy()
    
    return result

def apply_patches():
    """Apply all the patches to Pandas DataFrame methods."""
    global _original_loc_getitem, _original_groupby_agg, _original_groupby_sum
    global _original_groupby_mean, _original_groupby_count, _original_groupby_min, _original_groupby_max
    global _original_groupby_getitem
    
    df = pd.DataFrame()
    _original_loc_getitem = df.loc.__getitem__
    
    setattr(pd.DataFrame, 'filter', _patch_filter)
    
    original_loc_property = pd.DataFrame.loc
    
    def patched_loc(self):
        original_indexer = original_loc_property.__get__(self)
        original_indexer.__getitem__ = lambda key: _patch_loc_getitem(self, key)
        return original_indexer
    
    setattr(pd.DataFrame, 'loc', property(patched_loc))
    setattr(pd.DataFrame, 'rename', _patch_rename)
    setattr(pd.DataFrame, 'assign', _patch_assign)
    setattr(pd.DataFrame, 'sort_values', _patch_sort_values)
    setattr(pd.DataFrame, 'head', _patch_head)
    setattr(pd.DataFrame, 'merge', _patch_merge)
    setattr(pd.DataFrame, 'groupby', _patch_groupby)
    
    # Create a test GroupBy object to get the class
    test_df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    test_groupby = test_df.groupby('a')
    
    # Get the actual GroupBy class from our test object
    groupby_class = test_groupby.__class__
    
    _original_groupby_agg = test_groupby.agg
    _original_groupby_sum = test_groupby.sum
    _original_groupby_mean = test_groupby.mean
    _original_groupby_count = test_groupby.count
    _original_groupby_min = test_groupby.min
    _original_groupby_max = test_groupby.max
    _original_groupby_getitem = groupby_class.__getitem__
    
    setattr(groupby_class, 'agg', _patch_groupby_agg)
    setattr(groupby_class, 'sum', _patch_groupby_sum)
    setattr(groupby_class, 'mean', _patch_groupby_mean)
    setattr(groupby_class, 'count', _patch_groupby_count)
    setattr(groupby_class, 'min', _patch_groupby_min)
    setattr(groupby_class, 'max', _patch_groupby_max)
    setattr(groupby_class, '__getitem__', _patch_groupby_getitem)
    
    series_groupby = test_df.groupby('a')['b']
    series_groupby_class = series_groupby.__class__
    
    setattr(series_groupby_class, 'sum', _patch_groupby_sum)
    setattr(series_groupby_class, 'mean', _patch_groupby_mean)
    setattr(series_groupby_class, 'count', _patch_groupby_count)
    setattr(series_groupby_class, 'min', _patch_groupby_min)
    setattr(series_groupby_class, 'max', _patch_groupby_max)
    
def remove_patches():
    """Remove all the patches from Pandas DataFrame methods."""
    global _original_groupby_getitem
    
    setattr(pd.DataFrame, 'filter', _original_filter)
    
    original_loc_property = getattr(pd.DataFrame, 'loc', None)
    if original_loc_property is not None:
        setattr(pd.DataFrame, 'loc', original_loc_property)
    
    setattr(pd.DataFrame, 'rename', _original_rename)
    setattr(pd.DataFrame, 'assign', _original_assign)
    setattr(pd.DataFrame, 'sort_values', _original_sort_values)
    setattr(pd.DataFrame, 'head', _original_head)
    setattr(pd.DataFrame, 'merge', _original_merge)
    setattr(pd.DataFrame, 'groupby', _original_groupby)
    
    # Create a test groupby to get the class
    test_df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    test_groupby = test_df.groupby('a')
    groupby_class = test_groupby.__class__
    
    if _original_groupby_agg is not None:
        setattr(groupby_class, 'agg', _original_groupby_agg)
    if _original_groupby_sum is not None:
        setattr(groupby_class, 'sum', _original_groupby_sum)
    if _original_groupby_mean is not None:
        setattr(groupby_class, 'mean', _original_groupby_mean)
    if _original_groupby_count is not None:
        setattr(groupby_class, 'count', _original_groupby_count)
    if _original_groupby_min is not None:
        setattr(groupby_class, 'min', _original_groupby_min)
    if _original_groupby_max is not None:
        setattr(groupby_class, 'max', _original_groupby_max)
    if _original_groupby_getitem is not None:
        setattr(groupby_class, '__getitem__', _original_groupby_getitem)

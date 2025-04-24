"""
PandasQL: A Pandas API interface for LegendQL.

This module provides a class for creating and executing queries using the Pandas API
and converting them to LegendQL metamodel.
"""
from __future__ import annotations
from typing import Dict, List, Type, Optional, Union, Callable, Any

import pandas as pd

from model.schema import Table, Database
from model.metamodel import DataFrame, Runtime, Expression
from legendql.query import Query
from legendql.pandas_parser import PandasParser

class PandasQL:
    """
    A class for creating and executing queries using the Pandas API.
    
    This class provides methods for converting Pandas DataFrame operations 
    to LegendQL metamodel constructs.
    """
    
    _query: Query
    _df: pd.DataFrame
    
    @classmethod
    def from_df(cls, df: pd.DataFrame, table_name: str = "pandas_table", database_name: str = "pandas_db") -> PandasQL:
        """
        Create a PandasQL instance from a Pandas DataFrame.
        
        Args:
            df: The Pandas DataFrame
            table_name: The name of the table
            database_name: The name of the database
            
        Returns:
            A new PandasQL instance
        """
        columns = {col: type(df[col].iloc[0]) if len(df) > 0 else None for col in df.columns}
        
        table = Table(table_name, columns)
        database = Database(database_name, [table])
        
        return PandasQL(df, Query.from_table(database, table))
    
    def __init__(self, df: pd.DataFrame, query: Query):
        """
        Initialize a PandasQL instance.
        
        Args:
            df: The Pandas DataFrame
            query: The Query object
        """
        self._df = df
        self._query = query
    
    def bind(self, runtime: Runtime) -> DataFrame:
        """
        Bind the query to a runtime.
        
        Args:
            runtime: The runtime to bind to
            
        Returns:
            A DataFrame object that can be evaluated
        """
        return self._query.bind(runtime)
    
    def eval(self, runtime: Runtime) -> DataFrame:
        """
        Evaluate the query using a runtime.
        
        Args:
            runtime: The runtime to use for evaluation
            
        Returns:
            The result DataFrame
        """
        return self._query.eval(runtime)
    
    def select(self, columns: Union[str, List[str]]) -> PandasQL:
        """
        Select columns from the DataFrame.
        
        Args:
            columns: The columns to select
            
        Returns:
            A new PandasQL instance with the selection clause added
        """
        if isinstance(columns, str):
            columns = [columns]
        
        expressions = PandasParser.parse_select(self._df, columns, self._query._table)
        
        from model.metamodel import SelectionClause
        self._query._add_clause(SelectionClause(expressions))
        
        return self
    
    def filter(self, condition: str) -> PandasQL:
        """
        Filter the DataFrame based on a condition.
        
        Args:
            condition: The filter condition as a string (to be parsed)
            
        Returns:
            A new PandasQL instance with the filter clause added
        """
        expression = PandasParser.parse_filter(self._df, condition, self._query._table)
        
        from model.metamodel import FilterClause
        self._query._add_clause(FilterClause(expression))
        
        return self
    
    def rename(self, columns: Dict[str, str]) -> PandasQL:
        """
        Rename columns in the DataFrame.
        
        Args:
            columns: A dictionary mapping old column names to new column names
            
        Returns:
            A new PandasQL instance with the rename clause added
        """
        expressions = PandasParser.parse_rename(self._df, columns, self._query._table)
        
        from model.metamodel import RenameClause
        self._query._add_clause(RenameClause(expressions))
        
        return self
    
    def extend(self, expressions: Dict[str, Union[str, Callable]]) -> PandasQL:
        """
        Add new columns to the DataFrame.
        
        Args:
            expressions: A dictionary mapping new column names to expressions
            
        Returns:
            A new PandasQL instance with the extend clause added
        """
        extend_expressions = PandasParser.parse_extend(self._df, expressions, self._query._table)
        
        from model.metamodel import ExtendClause
        self._query._add_clause(ExtendClause(extend_expressions))
        
        return self
    
    def group_by(self, by: Union[str, List[str]], agg: Dict[str, str]) -> PandasQL:
        """
        Group the DataFrame by columns and apply aggregations.
        
        Args:
            by: The columns to group by
            agg: A dictionary mapping output column names to aggregation functions
            
        Returns:
            A new PandasQL instance with the group by clause added
        """
        group_by_expression = PandasParser.parse_group_by(self._df, by, agg, self._query._table)
        
        from model.metamodel import GroupByClause
        self._query._add_clause(GroupByClause(group_by_expression))
        
        return self
    
    def order_by(self, by: Union[str, List[str]], ascending: Union[bool, List[bool]] = True) -> PandasQL:
        """
        Sort the DataFrame by columns.
        
        Args:
            by: The columns to sort by
            ascending: Whether to sort in ascending order
            
        Returns:
            A new PandasQL instance with the order by clause added
        """
        order_expressions = PandasParser.parse_order_by(self._df, by, ascending, self._query._table)
        
        from model.metamodel import OrderByClause
        self._query._add_clause(OrderByClause(order_expressions))
        
        return self
    
    def limit(self, n: int) -> PandasQL:
        """
        Limit the number of rows in the result.
        
        Args:
            n: The maximum number of rows to return
            
        Returns:
            A new PandasQL instance with the limit clause added
        """
        from model.metamodel import LimitClause, IntegerLiteral
        self._query._add_clause(LimitClause(IntegerLiteral(n)))
        return self
    
    def join(self, other: PandasQL, on: Union[str, List[str]], how: str = "inner") -> PandasQL:
        """
        Join this DataFrame with another DataFrame.
        
        Args:
            other: The other PandasQL instance to join with
            on: The column(s) to join on
            how: The type of join (inner, left, right, outer)
            
        Returns:
            A new PandasQL instance with the join clause added
        """
        join_expression = PandasParser.parse_join(self._df, other._df, on, how, self._query._table, other._query._table)
        
        from model.metamodel import JoinClause, FromClause
        from model.metamodel import InnerJoinType, LeftJoinType
        
        if how == "inner":
            join_type = InnerJoinType()
        elif how == "left":
            join_type = LeftJoinType()
        else:
            raise ValueError(f"Join type '{how}' is not supported. Only 'inner' and 'left' joins are supported.")
        
        self._query._add_clause(JoinClause(
            FromClause(other._query._database.name, other._query._table.table),
            join_type,
            join_expression
        ))
        
        return self

"""
LegendQL integration for Pandas.

This module provides the public API for using Pandas with LegendQL.
"""
import pandas as pd
from typing import Dict, List, Union, Any, Optional, Type
from .pandas_integration import init_dataframe, apply_patches, remove_patches, eval_query, bind_query

def init():
    """Initialize the Pandas integration for LegendQL."""
    apply_patches()
    
def cleanup():
    """Clean up the Pandas integration for LegendQL."""
    remove_patches()
    
def from_df(df: pd.DataFrame, table_name: str = "pandas_table", database_name: str = "pandas_db") -> pd.DataFrame:
    """
    Initialize a Pandas DataFrame with LegendQL context.
    
    Args:
        df: The Pandas DataFrame to initialize
        table_name: The name of the table
        database_name: The name of the database
        
    Returns:
        The initialized DataFrame
    """
    return init_dataframe(df, table_name, database_name)

def create_df(data: Union[Dict[str, List], List[Dict[str, Any]]], 
             table_name: str = "pandas_table", 
             database_name: str = "pandas_db") -> pd.DataFrame:
    """
    Create a new Pandas DataFrame with LegendQL context directly.
    
    Args:
        data: Data for the DataFrame, either as a dict of lists or a list of dicts
        table_name: The name of the table
        database_name: The name of the database
        
    Returns:
        The initialized DataFrame with LegendQL context
    """
    df = pd.DataFrame(data)
    return init_dataframe(df, table_name, database_name)

def table(table_name: str, 
         columns: Dict[str, Type], 
         database_name: str = "pandas_db") -> pd.DataFrame:
    """
    Create an empty DataFrame with specified columns and LegendQL context.
    Similar to ql.py's approach of starting with just a table name and columns.
    
    Args:
        table_name: The name of the table
        columns: Dictionary mapping column names to their types
        database_name: The name of the database
        
    Returns:
        An empty DataFrame with the specified columns and LegendQL context
    """
    df = pd.DataFrame(columns=list(columns.keys()))
    
    for col, dtype in columns.items():
        if dtype is not None:
            try:
                df[col] = df[col].astype(dtype)
            except:
                pass
    
    return init_dataframe(df, table_name, database_name)
    
def eval(df: pd.DataFrame, runtime):
    """
    Evaluate the LegendQL query for a DataFrame using a runtime.
    
    Args:
        df: The DataFrame
        runtime: The runtime to use for evaluation
        
    Returns:
        The result DataFrame
    """
    return eval_query(df, runtime)
    
def bind(df: pd.DataFrame, runtime):
    """
    Bind the LegendQL query for a DataFrame to a runtime.
    
    Args:
        df: The DataFrame
        runtime: The runtime to bind to
        
    Returns:
        A DataFrame object that can be evaluated
    """
    return bind_query(df, runtime)

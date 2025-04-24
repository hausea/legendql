"""
LegendQL integration for Pandas.

This module provides the public API for using Pandas with LegendQL.
"""
import pandas as pd
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

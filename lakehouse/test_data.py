import duckdb
import pandas as pd
import polars as pl


def test_polars_extract() -> pd.DataFrame:
    data = {'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 22],
            'city': ['New York', 'London', 'Paris']}
    return pl.DataFrame(data).to_pandas()


def test_pandas_extract() -> pd.DataFrame:
    data = {
    'col1': [1, 2, 3],
    'col2': ['A', 'B', 'C'],
    'col3': [1.1, 2.2, 3.3]
    }
    return pd.DataFrame(data)


def test_duckdb_extract() -> pd.DataFrame:
    return duckdb.sql("SELECT 42").to_df()

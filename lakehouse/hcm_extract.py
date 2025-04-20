from datetime import date

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from numpy import ndarray


def test_polars_extract() -> pd.DataFrame:
    data = {'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 22],
            'city': ['New York', 'London', 'Paris']}
    return pl.DataFrame(data)

def test_pandas_extract() -> pd.DataFrame:
    data = {
        "id": [1, 2, 3, 4, 5],
        "kerberos": ["nmr", "pdb", "dxl", "sc", "rr"],
        "name": ["neema", "pierre", "david", "suraj", "ram"],
        "department": ["data", "data", "data", "data", "data"],
        "start_date": [date(2003, 1, 1), date(2003, 1, 1), date(2003, 1, 1), date(2003, 1, 1), date(2003, 1, 1)],
        "nickname": [ "neems", "pi", None, "sur", "r" ]
    }

    df = pd.DataFrame(data)
    df.set_index("id", inplace=True)
    return df

def test_numpy_extract() -> ndarray:
    return np.array([(1, 9.0), (2, 8.0), (3, 7.0)])

def test_arrow_table() -> pa.Table:
    return pa.Table.from_pydict({'a': [42]})

def test_duckdb_extract() -> duckdb.DuckDBPyRelation:
    return duckdb.sql("SELECT 42 as id, 'neema' as kerberos")


# df = test_pandas_extract()
# print(df.attrs)
# table = pa.Table.from_pandas(df)
# print(table.schema)
# d = dict(zip(table.schema.names, table.schema.types))
# print(d)
#
# df = test_polars_extract()
# print(pa.table(df).schema)
#
# df = test_numpy_extract()
# print(df)
# print(pa.Table.from_arrays(df))

# print(pa.table(pl.from_numpy(df)))
#
# df = test_duckdb_extract()
# print(pa.table(df).schema)
#
# df = test_arrow_table()
# print(df.schema)


# duckdb.sql("COPY (FROM generate_series(100_000)) TO 'test.parquet' (FORMAT parquet)")

# import pyarrow.parquet as pq
# table = pq.read_table('test.parquet')

# import pyarrow.dataset as ds
# d = ds.dataset('test.parquet', format="parquet")
# print("parquet")
# print(dict(zip(d.schema.names, d.schema.types)))

# dd = duckdb.sql("SELECT * FROM df").pl()
# # print(dict(zip(dd.columns, dd.dtypes)))
# print(dd.schema)
# print(dd)



# df = test_polars_extract()
# print(df.schema)
#
# dd = duckdb.sql("SELECT * FROM df")
# #print(dict(zip(dd.columns, dd.dtypes)))
# duckdb.sql("COPY (SELECT * FROM df) TO '/Users/neema/neema_test.csv'")
#
#
# pd_df = test_pandas_extract()
# dd = duckdb.sql("SELECT * FROM pd_df")
# print(type(dd))
# # <class 'duckdb.duckdb.DuckDBPyRelation'>
#
# print(dict(zip(dd.columns, dd.dtypes)))
#
# arr = test_arrow_table()
# arrd = duckdb.sql("SELECT * FROM arr")
# print(arrd)
#
# npy = test_numpy_extract()
# npyd = duckdb.sql("SELECT * FROM npy")
# print(npyd)
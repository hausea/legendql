from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Type, Union

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from ql.legendql import LegendQL


@dataclass
class IngestSource:
    def auto_infer_columns(self) -> Dict[str, Optional[Type]]:
        pass


@dataclass
class FileSource(IngestSource):
    file_name: str


@dataclass
class CSV(FileSource):
    column_delimiter: str = ',',
    row_delimiter: str = '\n',
    quote_character: str = '"'
    escape_character: str = '\\'
    starting_row = 0

    def auto_infer_columns(self) -> Dict[str, Optional[Type]]:
        table = ds.dataset(self.file_name, format="csv")
        return dict(zip(table.schema.names, table.schema.types))


@dataclass
class Avro(FileSource):
    def auto_infer_columns(self) -> Dict[str, Optional[Type]]:
        # Use DuckDB to read Avro file and convert to Arrow table
        con = duckdb.connect()
        relation = con.sql(f"SELECT * FROM read_avro('{self.file_name}')")
        table = relation.arrow()
        return dict(zip(table.schema.names, table.schema.types))


@dataclass
class Parquet(FileSource):
    def auto_infer_columns(self) -> Dict[str, Optional[Type]]:
        table = ds.dataset(self.file_name, format="parquet")
        return dict(zip(table.schema.names, table.schema.types))


@dataclass
class Json(FileSource):
    def auto_infer_columns(self) -> Dict[str, Optional[Type]]:
        # Read the JSON file using DuckDB instead of direct PyArrow JSON reader
        con = duckdb.connect()
        relation = con.sql(f"SELECT * FROM read_json('{self.file_name}', auto_detect=true)")
        table = relation.arrow()
        return dict(zip(table.schema.names, table.schema.types))


@dataclass
class Excel(FileSource):
    sheet_name: Optional[str] = None  # Made optional

    def auto_infer_columns(self) -> Dict[str, Optional[Type]]:
        # Use DuckDB to read Excel file and convert to Arrow table
        con = duckdb.connect()

        # Build the SQL query based on whether sheet_name is provided
        if self.sheet_name:
            query = f"SELECT * FROM read_xlsx('{self.file_name}', sheet='{self.sheet_name}')"
        else:
            query = f"SELECT * FROM read_xlsx('{self.file_name}')"

        relation = con.sql(query)
        table = relation.arrow()
        return dict(zip(table.schema.names, table.schema.types))


@dataclass
class PythonSource(IngestSource):
    func_or_df: Union[Callable]

    def auto_infer_columns(self) -> Dict[str, Optional[Type]]:
        df = self.func_or_df

        if isinstance(self.func_or_df, Callable):
            df = self.func_or_df()

        table = self.get_arrow_table(df)
        return dict(zip(table.schema.names, table.schema.types))

    def get_arrow_table(self, df):
        return pa.table(df)


@dataclass
class Pandas(PythonSource):
    func_or_df: Union[Callable[[], pd.DataFrame]]


@dataclass
class Polars(PythonSource):
    func_or_df: Union[Callable[[], pl.DataFrame]]


@dataclass
class DuckDb(PythonSource):
    func_or_df: Union[Callable[[], duckdb.DuckDBPyRelation]]


@dataclass
class NumPy(PythonSource):
    func_or_df: Union[Callable[[], np.ndarray]]

    def get_arrow_table(self, df):
        return pa.table(pl.from_numpy(df))


@dataclass
class Arrow(PythonSource):
    func_or_df: Union[Callable[[], pa.Table]]

    def get_arrow_table(self, df):
        return df


@dataclass
class Legend(IngestSource):
    query: LegendQL

# def upload_extract(ingest: Ingest, source: PythonSource):
#     # # call Lakehouse Ingest API to get S3 location
#     # ingest_stage_location = "/Users/neema/"
#     #
#     # #create file name
#     # for dataset in ingest.datasets:
#     #     # run the python code to get the dataframe
#     #     df = dataset.source.func()
#     #
#     #     #use duckdb to write the file to s3
#     #     duckdb.sql(f"COPY (SELECT * FROM df) TO '{ingest_stage_location}'")
#     pass
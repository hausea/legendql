from datetime import date, datetime

import pandas as pd
import polars as pl
import duckdb

from lakehouse import Ingest, Snapshot, Versioning, DataProduct, AccessPoint, Legend, CSV, Scheduled, any, all, Pandas, \
    Polars
from model.schema import Dataset, Sensitivity
from ql.legendql import LegendQL


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

raw_meta = Ingest(
    deployment_id=123456,
    format=Pandas(test_pandas_extract),
    versioning=Versioning.batch_full,
    trigger=Scheduled(),
    datasets={
        "metadir": Dataset(
            schema_name="people",
            table="metadir",
            primary_key="id",
            sensitivity=Sensitivity.enterprise,
            columns={"kerberos": str, "name": str, "id": str, "department": str, "start_date": date}
        ),
    }
)

raw_corp = Ingest(
    deployment_id=123456,
    format=Polars(test_polars_extract),
    versioning=Versioning.batch_full,
    trigger=Scheduled(),
    datasets={
        "corpdir": Dataset(
            schema_name="people",
            table="corpdir",
            primary_key="kerberos",
            sensitivity=Sensitivity.enterprise,
            columns={"kerberos": str, "dept1": str, "dept2": str, "dept3": str, "start_time": datetime}
        )
    }
)

metadir = LegendQL.from_lh(raw_meta.metadir).filter(lambda r: r.department == "Data Engineering")
metadir.join(LegendQL.from_lh(raw_corp.corpdir), lambda m, c: (m.kerberos == c.kerberos, (corp_kerberos := c.kerberos)))

mat_view = Ingest(
    deployment_id=123456,
    format=Legend(metadir),
    versioning = Versioning.batch_full,
    datasets={"metadir": metadir.get_table_definition()},
    trigger=any([raw_meta, raw_corp]) # could be all() as well
)

dp = DataProduct({"dp_metadir": AccessPoint(deploymentId=123456, query=metadir)})
print(dp)

# End PRODUCER


# Start CONSUMER

pos = Ingest(
    deployment_id=888888888,
    format=CSV(column_delimiter="|"),
    versioning=Versioning.batch_incr,
    datasets={
        "firm_positions": Dataset(
            schema_name="positions",
            table="firm_positions",
            primary_key="id",
            sensitivity=Sensitivity.producer_entitled,
            columns={"dept3": str, "account": str, "product_id": int, "quantity": float}
        )
    },
    trigger=Scheduled()
)

rle = (LegendQL
 .from_lh(pos.firm_positions)
 .join(LegendQL.from_lh(dp.dp_metadir), lambda p, m: (p.dept3 == m.dept3, (metadir_dept3 := m.dept3))))

consumer_view = Ingest(
    deployment_id=888888888,
    format=Legend(rle),
    versioning = Versioning.batch_full,
    datasets={"rle_positions": rle.get_table_definition()},
    trigger=Scheduled()
)

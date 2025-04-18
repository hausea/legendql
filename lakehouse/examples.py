from datetime import date, datetime
from typing import Optional

from lakehouse import Ingest, DataProduct, AccessPoint, any, Lakehouse, cfo_dev, View
from extract import CSV, Pandas, Polars, Legend
from lakehouse import gbm_public_dev, batch_milestone_full, MaterializedView, batch_milestone_incremental
from test_data import test_polars_extract, test_pandas_extract
from lakehouse import Dataset, enterprise, producer_entitled, producer_only
from ql.legendql import LegendQL

lh = Lakehouse.register_producer(gbm_public_dev, 123456)

raw_meta = Ingest(datasets={
    "metadir": Dataset(
        schema="people",
        table="metadir",
        primary_key=["id"],
        source=Pandas(test_pandas_extract),
        versioning=batch_milestone_full,
        classification=producer_only,
        columns={"kerberos": str, "name": str, "id": str, "department": str, "start_date": date, "nickname": Optional[str]},
    )}
)

lh.run_ingest(raw_meta)

raw_corp = Ingest(datasets={
    "corpdir": Dataset(
        schema="people",
        table="corpdir",
        primary_key="kerberos",
        source=Polars(test_polars_extract),
        versioning=batch_milestone_incremental,
        classification=producer_only,
        columns={"kerberos": str, "dept1": str, "dept2": str, "dept3": str, "start_time": datetime}
    )}
)

lh.run_ingest(raw_corp)

metadir = LegendQL.from_lh(raw_meta.metadir).filter(lambda r: r.department == "Data Engineering")
metadir.join(LegendQL.from_lh(raw_corp.corpdir), lambda m, c: (m.kerberos == c.kerberos, (corp_kerberos := c.kerberos)))

mat_view = MaterializedView(views={
    "mv_metadir": View(
        schema="people",
        table="mv_metadir",
        primary_key="kerberos",
        source=Legend(metadir),
        versioning=batch_milestone_full,
        classification=enterprise,
        trigger=any([raw_meta, raw_corp]) # could be all() as well
    )}
)

lh.materialize_view(mat_view)

dp = DataProduct({"dp_metadir": AccessPoint(query=metadir)})
print(dp)

lh.publish_data_product(dp)

# End PRODUCER


# Start CONSUMER

lh = Lakehouse.register_producer(cfo_dev, 888888)

pos = Ingest(datasets={
    "firm_positions": Dataset(
    schema="positions",
    table="firm_positions",
    primary_key="id",
    columns={"dept3": str, "account": str, "product_id": int, "quantity": float},
    source=CSV(file_name="/user/test/positions_2025.csv", column_delimiter="|"),
    versioning=batch_milestone_incremental,
    classification=producer_entitled,
    )
})

lh.run_ingest(pos)

rle = (LegendQL
 .from_lh(pos.firm_positions)
 .join(LegendQL.from_lh(dp.dp_metadir), lambda p, m: (p.dept3 == m.dept3, (metadir_dept3 := m.dept3))))

consumer_view = MaterializedView(views={
    "rle_positions": View(
        schema="positions",
        table="rle_positions",
        primary_key=["account", "product_id"],
        source=Legend(rle),
        versioning=batch_milestone_incremental,
        classification=enterprise,
    )}
)

lh.materialize_view(consumer_view)

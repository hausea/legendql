from datetime import date, datetime

from lakehouse import IngestConfig, Snapshot, Versioning, DataProduct, AccessPoint, Legend, CSV
from model.schema import Dataset, Sensitivity
from ql.legendql import LegendQL

raw = IngestConfig(
    deployment_id=123456,
    format=CSV(column_delimiter="|"),
    snapshot=Snapshot.full,
    versioning=Versioning.batch_milestone,
    datasets={
        "metadir": Dataset(
            schema_name="people",
            table="metadir",
            primary_key="id",
            sensitivity=Sensitivity.enterprise,
            columns={"kerberos": str, "name": str, "id": str, "department": str, "start_date": date}
        ),
        "corpdir": Dataset(
            schema_name="people",
            table="corpdir",
            primary_key="kerberos",
            sensitivity=Sensitivity.enterprise,
            columns={"kerberos": str, "dept1": str, "dept2": str, "dept3": str, "start_time": datetime}
        )
    }
)

metadir = LegendQL.from_lh(raw.metadir).filter(lambda r: r.department == "Data Engineering")
metadir.join(LegendQL.from_lh(raw.corpdir), lambda m, c: (m.kerberos == c.kerberos, (corp_kerberos := c.kerberos)))

mat_view = IngestConfig(
    deployment_id=123456,
    format=Legend(metadir),
    snapshot = Snapshot.full,
    versioning = Versioning.batch_milestone,
    datasets={"metadir": metadir.get_table_definition()}
)

dp = DataProduct({"dp_metadir": AccessPoint(deploymentId=123456, query=metadir)})


pos = IngestConfig(
    deployment_id=888888888,
    format=CSV(column_delimiter="|"),
    snapshot=Snapshot.incremental,
    versioning=Versioning.batch_milestone,
    datasets={
        "positions": Dataset(
            schema_name="people",
            table="metadir",
            primary_key="id",
            sensitivity=Sensitivity.enterprise,
            columns={"dept3": str, "account": str, "product_id": int, "quantity": float}
        )
    }
)

rle = (LegendQL
 .from_lh(pos.positions)
 .join(LegendQL.from_lh(dp.dp_metadir), lambda p, m: (p.dept3 == m.dept3, (metadir_dept3 := m.dept3))))

consumer_view = IngestConfig(
    deployment_id=888888888,
    format=Legend(rle),
    snapshot = Snapshot.full,
    versioning = Versioning.batch_milestone,
    datasets={"rle_positions": rle.get_table_definition()}
)

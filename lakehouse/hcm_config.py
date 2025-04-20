import lakehouse
from lakehouse.hcm_extract import test_pandas_extract, test_polars_extract

def config() -> lakehouse.Lakehouse:

    lh = lakehouse.init(lakehouse.hcm_dev, deployment_id=123456, schema="people")

    lh.config({
    "metadir": lh.dataset(
        table="metadir",
        primary_key=["id"],
        source=lh.pandas(test_pandas_extract),
        versioning=lh.batch_milestone_full,
        classification=lh.producer_only,
        #columns={"kerberos": str, "name": str, "id": str, "department": str, "start_date": date, "nickname": Optional[str]}
    ),

    "corpdir": lh.dataset(
        table="corpdir",
        primary_key="kerberos",
        source=lh.polars(test_polars_extract),
        versioning=lh.batch_milestone_incremental,
        classification=lh.producer_only,
        #columns={"kerberos": str, "dept1": str, "dept2": str, "dept3": str, "start_time": datetime}
    ),

    "metadir_mv": lh.view(
        table="metadir_mv",
        primary_key="kerberos",
        source=lh.query(lh.metadir)
            .filter(lambda r: r.department == "Data Engineering")
            .join(lh.query(lh.corpdir), lambda m, c: (m.kerberos == c.kerberos, (corp_kerberos := c.kerberos))),
        versioning=lh.batch_milestone_full,
        classification=lh.enterprise,
        trigger=lh.any([lh.metadir, lh.corpdir])
    ),

    "hcm_people_dir": lh.data_product(
        name="hcm_people_dir",
        display_name="HCM People Directory",
        access_points={
            "secdb_metadir": lh.query(lh.metadir_mv),
            "gsweb_corpdir": lh.query(lh.corpdir)
        }
    )
    })

    return lh
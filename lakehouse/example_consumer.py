import lakehouse
import lakehouse.hcm as hcm

def config() -> lakehouse.Lakehouse:

    lh = lakehouse.init(lakehouse.gbm_public_dev, 88888888, "positions")

    lh.config({

    "metadir": lh.import_(hcm.data_products(),
        data_product="hcm_people_dir",
        access_point="secdb_metadir",
        import_alias="metadir"
    ),

    "firm_positions": lh.dataset(
        table="firm_positions",
        primary_key=["account", "product_id"],
        columns={"dept3": str, "account": str, "product_id": int, "quantity": float},
        source=lh.csv(file_name="/user/test/positions_2025.csv", column_delimiter="|"),
        versioning=lh.batch_milestone_incremental,
        classification=lh.producer_entitled
    ),

    "positions_dp": lh.data_product(
        name="positions_dp",
        display_name="Firm Positions",
        access_points={
            "positions_rle": lh.query(lh.firm_positions)
                .join(lh.query(lh.metadir), lambda p, m: (p.dept3 == m.dept3, (metadir_dept3 := m.dept3))),
            "positions_all": lh.query(lh.firm_positions)
        }
    )
    })

    return lh
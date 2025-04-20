import hcm_config as hcm
import example_consumer as gbm

#PRODUCER
lh = hcm.config()
lh.run_ingest(lh.metadir)
lh.run_ingest(lh.corpdir)
lh.run_matview(lh.metadir_mv)
lh.publish_data_product(lh.hcm_people_dir)


# CONSUMER
lh = gbm.config()
lh.run_ingest(lh.firm_positions)
lh.publish_data_product(lh.positions_dp)
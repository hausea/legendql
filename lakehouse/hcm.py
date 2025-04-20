import lakehouse
import hcm_config as hcm

def data_products():
    return lakehouse.discover_data_products(hcm.config())
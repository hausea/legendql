from __future__ import annotations

from typing import Dict

from lh import Lakehouse, DataProduct, Tenant, dev, prod

def init(tenant: Tenant, deployment_id: int, schema: str = None) -> Lakehouse:
    return Lakehouse(tenant, deployment_id, schema)

def discover_data_products(lh: Lakehouse) -> Dict[str, DataProduct]:
    return lh.discover_data_products()

__all__ = [Lakehouse]

gbm_public_dev = Tenant("gbm-public", dev)
gbm_public_prod = Tenant("gbm-public", prod)
gbm_private_dev = Tenant("gbm-private", dev)
gbm_private_prod = Tenant("gbm-private", prod)
pwm_dev = Tenant("pwm", dev)
pwm_prod = Tenant("pwm", prod)
am_public_dev = Tenant("awm-public", dev)
am_public_prod = Tenant("awm-public", prod)
am_private_dev = Tenant("am-private", dev)
am_private_prod = Tenant("am-private", prod)
hcm_dev = Tenant("hcm", dev)
hcm_prod = Tenant("hcm", prod)
cfo_dev = Tenant("cf&o", dev)
cfo_prod = Tenant("cf&o", prod)
eng_dev = Tenant("eng", dev)
eng_prod = Tenant("eng", prod)




from __future__ import annotations

from typing import Dict

from lakehouse.lh import Lakehouse, DataProduct, Tenant, Development, Production


def init(tenant: Tenant, deployment_id: int, schema: str = None) -> Lakehouse:
    return Lakehouse(tenant, deployment_id, schema)

def discover_data_products(lh: Lakehouse) -> Dict[str, DataProduct]:
    return lh.discover_data_products()

__all__ = [Lakehouse]

gm_dev = Tenant("gm", Development())
gm_prod = Tenant("gm", Production())
ib_dev = Tenant("ib", Development())
ib_prod = Tenant("ib", Production())
pwm_dev = Tenant("pwm", Development())
pwm_prod = Tenant("pwm", Production())
am_public_dev = Tenant("awm-public", Development())
am_public_prod = Tenant("awm-public", Production())
am_private_dev = Tenant("am-private", Development())
am_private_prod = Tenant("am-private", Production())
hcm_dev = Tenant("hcm", Development())
hcm_prod = Tenant("hcm", Production())
cfo_dev = Tenant("cf&o", Development())
cfo_prod = Tenant("cf&o", Production())
eng_dev = Tenant("eng", Development())
eng_prod = Tenant("eng", Production())




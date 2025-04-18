from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, List, Tuple, Union, Type

from extract import Legend, IngestSource
from model.schema import Table
from ql.legendql import LegendQL


class MilestoneType(Enum):
    batch_milestone = 'batch_milestone'
    append_only = "append_only"
    overwrite = "overwrite"

class SnapshotType(Enum):
    incremental = "incremental"
    full = "full"
    none = "none"

@dataclass
class Versioning:
    type: Tuple[MilestoneType, SnapshotType]

@dataclass
class IngestTrigger:
    pass

@dataclass
class Ingest(Dict):
    datasets: Dict[str, Dataset]

    def __getattr__(self, key) -> Table:
        try:
            dataset = self.datasets[key]
            return Table(dataset.table, dataset.get_columns())
        except KeyError as e:
            raise AttributeError(f"'IngestConfig' object has no Dataset '{key}'") from e

@dataclass
class MaterializedView(Dict):
    views: Dict[str, View]

    def __getattr__(self, key) -> Table:
        try:
            return self.views[key].source.query.get_table_definition()
        except KeyError as e:
            raise AttributeError(f"'IngestConfig' object has no Dataset '{key}'") from e

@dataclass
class Scheduled(IngestTrigger):
    pass

@dataclass
class Dependency(IngestTrigger):
    dependencies: List[Ingest]

@dataclass
class AnyDependency(Dependency):
    pass

@dataclass
class AllDependency(Dependency):
    pass

@dataclass
class AccessPoint:
    query: LegendQL

@dataclass
class AccessPointDeployed(AccessPoint):
    deploymentId: int
    is_reproducible: Optional[bool] = False

@dataclass
class DataProduct(Dict):
    access_points: Dict[str, AccessPoint]

    def __getattr__(self, key) -> Table:
        try:
            return self.access_points[key].query.get_table_definition()
        except KeyError as e:
            raise AttributeError(f"'DataProduct' object has no AccessPoint '{key}'") from e

@dataclass
class Environment:
    pass

@dataclass
class Development(Environment):
    pass

@dataclass
class Production(Environment):
    pass

@dataclass
class Tenant:
    name: str
    environment: Environment

class Lakehouse:

    def __init__(self, tenant, deployment_id):
        self.tenant = tenant
        self.deployment_id = deployment_id

    @classmethod
    def register_producer(cls, tenant: Tenant, deployment_id: int) -> Lakehouse:
        # call producer registration script here
        return Lakehouse(tenant, deployment_id)

    def run_ingest(self, ingest: Ingest):
        # first, upload the ingest spec to the lakehouse
        # self._upload_ingest_spec(ingest)
        # then call first ingest API to get s3 location for upload
        # location = self._get_staging_location()
        # then actually do the extract and push to location
        # self._extract_and_stage_data(location)
        # finally, do the actual ingest
        # self._ingest_data()
        pass

    def materialize_view(self, view: MaterializedView):
        # first, upload the ingest spec to the lakehouse
        # self._upload_ingest_spec(ingest)
        # run the materialization
        # self._run_materialization()
        pass

    def publish_data_product(self, dp: DataProduct):
        pass


any = AnyDependency
all = AllDependency

batch_milestone_full = Versioning((MilestoneType.batch_milestone, SnapshotType.incremental))
batch_milestone_incremental = Versioning((MilestoneType.batch_milestone, SnapshotType.full))
append_only = Versioning((MilestoneType.append_only, SnapshotType.incremental))
overwrite = Versioning((MilestoneType.overwrite, SnapshotType.none))


dev = Development()
prod = Production()

gbm_public_dev = Tenant("gbm-public", dev)
gbm_public_prod = Tenant("gbm-public", prod)
hcm_dev = Tenant("hcm", dev)
hcm_prod = Tenant("hcm", prod)
cfo_dev = Tenant("cf&o", dev)
cfo_prod = Tenant("cf&o", prod)


@dataclass
class Classification:
    level: str

@dataclass
class TableOptions:
    pass

@dataclass
class Dataset:
    schema: str
    table: str
    primary_key: Union[str, List[str]]
    versioning: Versioning
    source: IngestSource
    columns: Optional[Dict[str, Optional[Type]]] = None
    classification: Optional[Classification] = None
    options: Optional[TableOptions] = None
    trigger: Optional[IngestTrigger] = None

    def get_columns(self) -> Dict[str, Optional[Type]]:
        if self.columns is not None:
            return self.columns
        else:
            return self.source.columns()

@dataclass
class View(Dataset):
    source: Legend

external_public = Classification("DP00")
enterprise = Classification("DP10")
producer_entitled = Classification("DP20")
high_risk = Classification("DP30")
producer_only = Classification("")

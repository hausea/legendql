from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, List, Callable, Tuple

import pandas as pd
import polars as pl

from model.schema import Table, Classification
from ql.legendql import LegendQL

@dataclass
class IngestSource:
    pass

@dataclass
class FileSource(IngestSource):
    file: str

@dataclass
class CSV(FileSource):
    column_delimiter: str = ',',
    row_delimiter: str = '\n',
    quote_character: str = '"'
    escape_character: str = '\\'
    starting_row = 0

@dataclass
class Avro(FileSource):
    pass

@dataclass
class Parquet(FileSource):
    pass

@dataclass
class Json(FileSource):
    pass

@dataclass
class Legend(IngestSource):
    query: LegendQL

@dataclass
class Pandas(IngestSource):
    extract_func: Callable[..., pd.DataFrame]

@dataclass
class Polars(IngestSource):
    extract_func: Callable[..., pl.DataFrame]

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
class Runnable(Dict):
    source: IngestSource
    versioning: Versioning
    classification: Classification

@dataclass
class Ingest(Runnable):
    datasets: Dict[str, Table]

    def __getattr__(self, key) -> Table:
        try:
            return self.datasets[key]
        except KeyError as e:
            raise AttributeError(f"'IngestConfig' object has no Dataset '{key}'") from e

@dataclass
class IngestDeployed(Ingest):
    deployment_id: int
    trigger: IngestTrigger

@dataclass
class MaterializedView(Runnable):
    source: Dict[str, Legend]
    trigger: IngestTrigger

    def __getattr__(self, key) -> Table:
        try:
            return self.source[key].query.get_table_definition()
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
        return Lakehouse(tenant, deployment_id)

    def run_ingest(self, ingest: Ingest):
        pass

    def run_materialized_view(self, view: MaterializedView):
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

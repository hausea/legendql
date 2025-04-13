from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Iterable, List, Callable

import pandas as pd
import polars as pl

from model.schema import Table
from ql.legendql import LegendQL

@dataclass
class Format:
    pass

@dataclass
class CSV(Format):
    column_delimiter: str = ',',
    row_delimiter: str = '\n',
    quote_character: str = '"'
    escape_character: str = '\\'
    starting_row = 0

@dataclass
class Avro(Format):
    pass

@dataclass
class Parquet(Format):
    pass

@dataclass
class Json(Format):
    pass

@dataclass
class Legend(Format):
    query: LegendQL

@dataclass
class Pandas(Format):
    extract_func: Callable[..., pd.DataFrame]

@dataclass
class Polars(Format):
    extract_func: Callable[..., pl.DataFrame]

class IngestType(Enum):
    batch_milestone = 'batch_milestone'
    append_only = "append_only"
    overwrite = "overwrite"

class Snapshot(Enum):
    incremental = "incremental"
    full = "full"
    none = "none"

class Versioning(Enum):
    batch_incr = (IngestType.batch_milestone, Snapshot.incremental)
    batch_full = (IngestType.batch_milestone, Snapshot.full)
    append_only = (IngestType.append_only, Snapshot.incremental)
    overwrite = (IngestType.overwrite, Snapshot.none)

@dataclass
class JobTrigger:
    pass

@dataclass
class Ingest(Dict):
    deployment_id: int
    format: Format
    versioning: Versioning
    datasets: Dict[str, Table]
    trigger: JobTrigger

    def __getattr__(self, key) -> Table:
        try:
            return self.datasets[key]
        except KeyError as e:
            raise AttributeError(f"'IngestConfig' object has no Dataset '{key}'") from e

@dataclass
class Scheduled(JobTrigger):
    pass

@dataclass
class Dependency(JobTrigger):
    dependencies: List[Ingest]

@dataclass
class AnyDependency(Dependency):
    pass

@dataclass
class AllDependency(Dependency):
    pass

@dataclass
class AccessPoint:
    deploymentId: int
    query: LegendQL
    is_reproducible: Optional[bool] = False

@dataclass
class DataProduct(Dict):
    access_points: Dict[str, AccessPoint]

    def __getattr__(self, key) -> Table:
        try:
            return self.access_points[key].query.get_table_definition()
        except KeyError as e:
            raise AttributeError(f"'DataProduct' object has no AccessPoint '{key}'") from e


any = AnyDependency
all = AllDependency
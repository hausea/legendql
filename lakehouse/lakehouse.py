from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

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

class Versioning(Enum):
    batch_milestone = 'batch_milestone'
    append_only = "append_only"
    overwrite = "overwrite"

class Snapshot(Enum):
    incremental = "incremental"
    full = "full"

    def __str__(self):
        return self.name

@dataclass
class IngestConfig(Dict):
    deployment_id: int
    format: Format
    snapshot: Snapshot
    versioning: Versioning
    datasets: Dict[str, Table]

    def __getattr__(self, key) -> Table:
        try:
            return self.datasets[key]
        except KeyError as e:
            raise AttributeError(f"'IngestConfig' object has no Dataset '{key}'") from e

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

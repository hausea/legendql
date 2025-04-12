from dataclasses import dataclass
from enum import Enum
from typing import Dict, Type, Optional, List, Union


@dataclass
class Table:
    table: str
    columns: Dict[str, Optional[Type]]

    def validate_column(self, column: str) -> bool:
        return column in self.columns

@dataclass
class Database:
    name: str
    tables: List[Table]
    pass

class Sensitivity(Enum):
    external_public = "DP00"
    enterprise = "DP10"
    producer_entitled = "DP20"
    high_risk = "DP30"

@dataclass
class TableOptions:
    pass

@dataclass
class Dataset(Table):
    schema_name: str
    primary_key: Union[str, List[str]]
    sensitivity: Sensitivity
    options: Optional[TableOptions] = None
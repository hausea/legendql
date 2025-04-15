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

@dataclass
class Classification:
    level: str

@dataclass
class TableOptions:
    pass

@dataclass
class Dataset(Table):
    schema: str
    primary_key: Union[str, List[str]]
    classification: Optional[Classification] = None,
    options: Optional[TableOptions] = None

external_public = Classification("DP00")
enterprise = Classification("DP10")
producer_entitled = Classification("DP20")
high_risk = Classification("DP30")

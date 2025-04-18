from dataclasses import dataclass
from typing import Dict, Type, Optional, List


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

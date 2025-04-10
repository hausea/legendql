from dataclasses import dataclass
from typing import Dict, Type, Optional

@dataclass
class Schema:
    database: str
    table: str
    columns: Dict[str, Optional[Type]]

    def validate_column(self, column: str) -> bool:
        return column in self.columns

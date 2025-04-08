from dataclasses import dataclass
from typing import Dict, Type, Optional


class Schema:

    def __init__(self, name: str, columns: Dict[str, Optional[Type]]):
        self.name = name
        self.columns = columns
        self.base_name = name
        self.table_suffix = 0

    def validate_column(self, column: str) -> bool:
        return column in self.columns

    def update_name(self) -> None:
        self.name = f"{self.base_name}_{self.table_suffix}"
        self.table_suffix += 1

    def __str__(self):
        return f"{self.name}, {self.columns}"
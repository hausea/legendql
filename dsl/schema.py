from dataclasses import dataclass
from typing import Dict, Type, Optional


class Schema:

    def __init__(self, name: str, columns: Dict[str, Optional[Type]]):
        self.name = name
        self.columns = columns

    def validate_column(self, column: str) -> bool:
        return column in self.columns

    def update_name(self, str) -> None:
        self.name = f"{self.name}_{str}"

    def __str__(self):
        return f"{self.name}, {self.columns}"
from abc import ABC, abstractmethod
from typing import List

from model.schema import Table, Database


class DatabaseType(ABC):
    def generate_model(self, runtime: str, database: Database) -> str:
        return f"{self.generate_pure_runtime(runtime, database)}\n\n{self.generate_pure_connection()}\n\n{self.generate_pure_database(database)}"

    @abstractmethod
    def generate_pure_runtime(self, name: str, database: Database) -> str:
        pass

    @abstractmethod
    def generate_pure_connection(self) -> str:
        pass

    @abstractmethod
    def generate_pure_database(self, database: Database) -> str:
        pass

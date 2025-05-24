from dataclasses import dataclass
from typing import List, Any

import duckdb

from dialect.sqlglot.dialect import SqlGlotRuntime
from model.metamodel import Clause


@dataclass
class DuckDBSqlGlotRuntime(SqlGlotRuntime):
    """
    Runtime for executing SqlGlot-generated SQL against a DuckDB database.
    """
    db_path: str | None = None
    
    _connection = None
    
    def eval(self, clauses: List[Clause]) -> Any:
        """
        Executes the SQL against a DuckDB database.
        
        Args:
            clauses: List of LegendQL clauses to execute
            
        Returns:
            The result of the SQL query execution
        """
        sql = self.executable_to_string(clauses)
        
        db_path = self.db_path if self.db_path is not None else ":memory:"
        
        self._connection = duckdb.connect(db_path)
        result = self._connection.sql(sql)
        
        return result

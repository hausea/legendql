import duckdb
from typing import List, Any

from model.metamodel import Clause
from model.schema import Table
from dialect.sqlglot.dialect import DuckDBSQLGlotRuntime

class DuckDBSQLGlotDatabaseRuntime(DuckDBSQLGlotRuntime):
    """DuckDB implementation that can execute queries against a real database."""
    
    def __init__(self, name: str, db_path: str = ""):
        super().__init__(name)
        self.db_path = db_path
        
    def eval(self, clauses: List[Clause]) -> Any:
        """Execute the query against a DuckDB database."""
        sql = self.executable_to_string(clauses)
        self.connection = duckdb.connect(self.db_path)
        result = self.connection.sql(sql)
        return result
    
    def create_table(self, table: Table):
        """Create a table in the DuckDB database."""
        columns = []
        for col, typ in table.columns.items():
            columns.append(f"{col} {self._to_column_type(typ)}")
        create = f"CREATE TABLE {table.table} ({', '.join(columns)});"
        try:
            con = duckdb.connect(self.db_path)
            con.sql(create)
        finally:
            if 'con' in locals():
                con.close()
    
    def _to_column_type(self, typ):
        """Convert Python type to DuckDB column type."""
        from datetime import date
        if typ == str:
            return "VARCHAR"
        if typ == int:
            return "BIGINT"
        if typ == date:
            return "DATE"
        return "VARCHAR"  # Default

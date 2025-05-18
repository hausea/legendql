import unittest

from model.schema import Table, Database
from legendql.ql import LegendQL
from dialect.sqlglot.dialect import DuckDBSQLGlotRuntime

class TestDuckDBIntegration(unittest.TestCase):
    """Test integrating LegendQL with DuckDB using SQLGlot."""
    
    def test_from_select(self):
        """Test the FROM and SELECT operations."""
        table = Table("table1", {"col1": str, "col2": int})
        database = Database("test_db", [table])
        
        runtime = DuckDBSQLGlotRuntime("duckdb_test")
        
        query = LegendQL.from_table(database, table)
        
        query = query.select(lambda t: t.col1)
        
        sql = query.bind(runtime).executable_to_string()
        
        self.assertIn("SELECT", sql)
        self.assertIn("col1", sql)
        self.assertIn("FROM table1", sql)

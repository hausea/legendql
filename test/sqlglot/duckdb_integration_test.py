import unittest
import tempfile
import os
import duckdb
import re

from model.schema import Table, Database
from legendql.ql import LegendQL
from runtime.sqlglot.runtime import DuckDBSQLGlotDatabaseRuntime

class TestDuckDBIntegration(unittest.TestCase):
    """Test integrating LegendQL with DuckDB using SQLGlot."""
    
    def setUp(self):
        """Set up the test database."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        
        self.runtime = DuckDBSQLGlotDatabaseRuntime("duckdb_test", self.db_path)
        
        self.table = Table("table1", {"col1": str, "col2": int})
        self.database = Database("test_db", [self.table])
        
        self.runtime.create_table(self.table)
        
        con = duckdb.connect(self.db_path)
        con.execute("INSERT INTO table1 VALUES ('value1', 1), ('value2', 2), ('value3', 3)")
        con.close()
    
    def tearDown(self):
        """Clean up the temporary database."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_from_select(self):
        """Test the FROM and SELECT operations with actual execution."""
        query = LegendQL.from_table(self.database, self.table)
        query = query.select(lambda t: [t.col1, t.col2])
        
        # Verify SQL generation
        sql = query.bind(self.runtime).executable_to_string()
        self.assertIn("SELECT", sql)
        self.assertIn("col1", sql)
        self.assertIn("col2", sql)
        self.assertIn("FROM table1", sql)
        
        # Execute the query
        result = query.bind(self.runtime).eval()
        
        # Verify the query executed successfully
        self.assertIsNotNone(result)
        
        # Verify the result contains the expected data
        # The result is a custom DataFrame with a 'results' attribute containing the DuckDB output
        result_str = str(result.results)
        
        # Verify the result contains the expected data
        
        # Verify column names
        self.assertIn("col1", result_str)
        self.assertIn("col2", result_str)
        
        # Verify values
        self.assertIn("value1", result_str)
        self.assertIn("value2", result_str)
        self.assertIn("value3", result_str)
    
    def test_from_select_single_column(self):
        """Test selecting a single column with actual execution."""
        query = LegendQL.from_table(self.database, self.table)
        query = query.select(lambda t: t.col1)
        
        # Verify SQL generation
        sql = query.bind(self.runtime).executable_to_string()
        self.assertIn("SELECT", sql)
        self.assertIn("col1", sql)
        self.assertIn("FROM table1", sql)
        
        # Execute the query
        result = query.bind(self.runtime).eval()
        
        # Verify the query executed successfully
        self.assertIsNotNone(result)
        
        # Verify the result contains the expected data
        # The result is a custom DataFrame with a 'results' attribute containing the DuckDB output
        result_str = str(result.results)
        
        # Verify the result contains the expected data
        
        # Verify column names
        self.assertIn("col1", result_str)
        self.assertNotIn("col2", result_str)
        
        # Verify values
        self.assertIn("value1", result_str)
        self.assertIn("value2", result_str)
        self.assertIn("value3", result_str)

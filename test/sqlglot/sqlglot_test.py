import unittest
import tempfile
import os
import duckdb
from datetime import date

from model.schema import Table, Database
from legendql.ql import LegendQL
from runtime.sqlglot.runtime import DuckDBSQLGlotDatabaseRuntime

class TestSQLGlotDialect(unittest.TestCase):
    """Test the SQLGlot dialect with DuckDB."""
    
    def setUp(self):
        """Set up the test by creating a temporary DuckDB database."""
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.db_path = self.temp_file.name
        self.temp_file.close()
        
        self.runtime = DuckDBSQLGlotDatabaseRuntime("duckdb_runtime", self.db_path)
        
        self.table = Table("employees", {
            "id": int,
            "name": str,
            "department": str,
            "salary": int,
            "hire_date": date
        })
        
        self.database = Database("test_db", [self.table])
        
        self.runtime.create_table(self.table)
        
        try:
            self.runtime.eval("""
                INSERT INTO employees VALUES 
                (1, 'John Doe', 'Engineering', 100000, '2020-01-15'),
                (2, 'Jane Smith', 'Sales', 90000, '2019-05-20'),
                (3, 'Bob Johnson', 'Engineering', 95000, '2021-03-10')
            """)
        except Exception as e:
            print(f"Error inserting test data: {e}")
    
    def tearDown(self):
        """Clean up the temporary database."""
        os.unlink(self.db_path)
    
    def test_simple_select(self):
        """Test a simple SELECT query."""
        query = LegendQL.from_table(self.database, self.table)
        
        query = query.select(lambda e: [e.id, e.name])
        
        df = query.bind(self.runtime)
        
        sql = df.executable_to_string()
        
        self.assertIn("SELECT", sql)
        self.assertIn("id", sql)
        self.assertIn("name", sql)
        self.assertIn("FROM employees", sql)

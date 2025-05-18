import unittest

from dialect.sqlglot.dialect import NonExecutableSqlGlotRuntime
from model.metamodel import FromClause, SelectionClause, ColumnReferenceExpression, FilterClause, BinaryExpression, GreaterThanBinaryOperator, IntegerLiteral, OperandExpression, LiteralExpression
from model.schema import Database, Table
from runtime.sqlglot.duckdb import DuckDBSqlGlotRuntime
from test.duckdb.db import TestDuckDB


class TestSqlGlotDialect(unittest.TestCase):
    
    def setUp(self):
        self.test_db = TestDuckDB()
        self.test_db.start()
        
        self.db = self.test_db.db
        self.table = Table("test_table", {"id": int, "name": str, "age": int})
        self.db.create_table(self.table)
        
        self.db.exec_sql("INSERT INTO test_table VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);")
        
        self.database = Database("test_db", [self.table])
        
        self.runtime = DuckDBSqlGlotRuntime(name="SqlGlotRuntime", db_path=self.test_db.database_path)
        self.non_executable_runtime = NonExecutableSqlGlotRuntime(name="NonExecutableSqlGlotRuntime")
    
    def tearDown(self):
        self.test_db.stop()
    
    def test_simple_select(self):
        """
        Test a simple SELECT query using SqlGlot dialect.
        """
        from_clause = FromClause(table="test_table", database="test_db")
        select_clause = SelectionClause(expressions=[ColumnReferenceExpression(name="name")])
        
        sql = self.non_executable_runtime.executable_to_string([from_clause, select_clause])
        
        self.assertEqual("SELECT name FROM test_table", sql.strip())
        
        result = self.runtime.eval([from_clause, select_clause])
        
        self.result = result
        rows = result.fetchall()
        self.assertEqual(3, len(rows))
        self.assertEqual("Alice", rows[0][0])
        self.assertEqual("Bob", rows[1][0])
        self.assertEqual("Charlie", rows[2][0])
    
    def test_select_with_filter(self):
        """
        Test a SELECT query with a filter using SqlGlot dialect.
        """
        from_clause = FromClause(table="test_table", database="test_db")
        select_clause = SelectionClause(expressions=[
            ColumnReferenceExpression(name="name"),
            ColumnReferenceExpression(name="age")
        ])
        filter_clause = FilterClause(
            expression=BinaryExpression(
                left=OperandExpression(expression=ColumnReferenceExpression(name="age")),
                operator=GreaterThanBinaryOperator(),
                right=OperandExpression(expression=LiteralExpression(literal=IntegerLiteral(val=30)))
            )
        )
        
        sql = self.non_executable_runtime.executable_to_string([from_clause, select_clause, filter_clause])
        
        self.assertEqual("SELECT name, age FROM test_table WHERE age > 30", sql.strip())
        
        result = self.runtime.eval([from_clause, select_clause, filter_clause])
        
        self.result = result
        rows = result.fetchall()
        self.assertEqual(1, len(rows))
        self.assertEqual("Charlie", rows[0][0])
        self.assertEqual(35, rows[0][1])


if __name__ == "__main__":
    unittest.main()

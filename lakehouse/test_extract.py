import unittest
import os
import pandas as pd
import pyarrow as pa
import fastavro

from extract import (
    CSV, Parquet, Json, Pandas, Polars, DuckDb,
    NumPy, Arrow, Avro, Excel
)
from hcm_extract import (
    test_pandas_extract, test_polars_extract,
    test_numpy_extract, test_arrow_table,
    test_duckdb_extract
)


class TestIngestSources(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create test files for file-based sources
        # CSV
        pd.DataFrame({
            'name': ['Alice', 'Bob'],
            'age': [25, 30]
        }).to_csv('test.csv', index=False)

        # Parquet
        pd.DataFrame({
            'id': [1, 2],
            'value': ['a', 'b']
        }).to_parquet('test.parquet')

        # JSON
        pd.DataFrame({
            'name': ['Charlie', 'David'],
            'score': [90.5, 85.0]
        }).to_json('test.json', orient='records')

        # Excel - Create test file with multiple sheets
        with pd.ExcelWriter('test.xlsx', engine='openpyxl') as writer:
            # First sheet (default)
            pd.DataFrame({
                'employee': ['John', 'Jane'],
                'salary': [50000, 60000],
                'department': ['IT', 'HR']
            }).to_excel(writer, sheet_name='Sheet1', index=False)

            # Second sheet
            pd.DataFrame({
                'department': ['IT', 'HR', 'Finance'],
                'budget': [100000, 80000, 120000]
            }).to_excel(writer, sheet_name='Departments', index=False)

        # Avro - Create test file with fastavro
        schema = {
            'name': 'test_record',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'value', 'type': 'int'},
                {'name': 'department', 'type': 'string'}
            ]
        }

        records = [
            {'name': 'test', 'value': 42, 'department': 'IT'},
            {'name': 'test2', 'value': 43, 'department': 'HR'}
        ]

        with open('test.avro', 'wb') as out:
            fastavro.writer(out, schema, records)

    def test_excel_columns_no_sheet_specified(self):
        """Test Excel source with no sheet specified (should read first sheet)"""
        excel_source = Excel(file_name='test.xlsx')
        schema = excel_source.auto_infer_columns()

        # Check if all expected columns from first sheet are present
        expected_columns = {'employee', 'salary', 'department'}
        self.assertEqual(set(schema.keys()), expected_columns)

        # Verify column types
        self.assertTrue(isinstance(schema['employee'], pa.DataType))
        self.assertTrue(isinstance(schema['salary'], pa.DataType))
        self.assertTrue(isinstance(schema['department'], pa.DataType))

    def test_excel_columns_explicit_sheet(self):
        """Test Excel source with explicitly specified sheet"""
        excel_source = Excel(file_name='test.xlsx', sheet_name='Departments')
        schema = excel_source.auto_infer_columns()

        # Check if all expected columns are present
        expected_columns = {'department', 'budget'}
        self.assertEqual(set(schema.keys()), expected_columns)

        # Verify column types
        self.assertTrue(isinstance(schema['department'], pa.DataType))
        self.assertTrue(isinstance(schema['budget'], pa.DataType))

    def test_excel_invalid_sheet(self):
        """Test Excel source with invalid sheet name"""
        excel_source = Excel(file_name='test.xlsx', sheet_name='NonexistentSheet')
        with self.assertRaises(Exception):
            schema = excel_source.auto_infer_columns()

    def test_avro_columns(self):
        """Test Avro source schema extraction"""
        avro_source = Avro(file_name='test.avro')
        schema = avro_source.auto_infer_columns()

        # Check if all expected columns are present
        expected_columns = {'name', 'value', 'department'}
        self.assertEqual(set(schema.keys()), expected_columns)

        # Verify column types
        self.assertTrue(isinstance(schema['name'], pa.DataType))
        self.assertTrue(isinstance(schema['value'], pa.DataType))
        self.assertTrue(isinstance(schema['department'], pa.DataType))

        # Test specific type mappings
        self.assertEqual(str(schema['name']), 'string')
        self.assertEqual(str(schema['value']), 'int32')
        self.assertEqual(str(schema['department']), 'string')

    def test_csv_columns(self):
        csv_source = CSV(file_name='test.csv')
        schema = csv_source.auto_infer_columns()
        self.assertIn('name', schema)
        self.assertIn('age', schema)

    def test_parquet_columns(self):
        parquet_source = Parquet(file_name='test.parquet')
        schema = parquet_source.auto_infer_columns()
        self.assertIn('id', schema)
        self.assertIn('value', schema)

    def test_json_columns(self):
        json_source = Json(file_name='test.json')
        schema = json_source.auto_infer_columns()
        self.assertIn('name', schema)
        self.assertIn('score', schema)

    def test_pandas_columns(self):
        pandas_source = Pandas(func_or_df=test_pandas_extract)
        schema = pandas_source.auto_infer_columns()
        expected_columns = ['kerberos', 'name', 'department', 'start_date', 'nickname']
        for col in expected_columns:
            self.assertIn(col, schema)

    def test_polars_columns(self):
        polars_source = Polars(func_or_df=test_polars_extract)
        schema = polars_source.auto_infer_columns()
        expected_columns = ['name', 'age', 'city']
        for col in expected_columns:
            self.assertIn(col, schema)

    def test_numpy_columns(self):
        numpy_source = NumPy(func_or_df=test_numpy_extract)
        schema = numpy_source.auto_infer_columns()
        self.assertTrue(len(schema) > 0)  # NumPy arrays will have at least one column

    def test_arrow_columns(self):
        arrow_source = Arrow(func_or_df=test_arrow_table)
        schema = arrow_source.auto_infer_columns()
        self.assertIn('a', schema)

    def test_duckdb_columns(self):
        duckdb_source = DuckDb(func_or_df=test_duckdb_extract)
        schema = duckdb_source.auto_infer_columns()
        expected_columns = ['id', 'kerberos']
        for col in expected_columns:
            self.assertIn(col, schema)

    @classmethod
    def tearDownClass(cls):
        # Clean up test files
        test_files = [
            'test.csv', 'test.parquet', 'test.json',
            'test.xlsx', 'test.avro'
        ]
        for file in test_files:
            if os.path.exists(file):
                os.remove(file)


if __name__ == '__main__':
    unittest.main()
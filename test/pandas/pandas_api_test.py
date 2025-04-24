import unittest
import pandas as pd
from typing import Dict, Type

from model.schema import Table, Database
from legendql.pandas_legend import init, cleanup, from_df, create_df, table, bind, eval
from dialect.purerelation.dialect import NonExecutablePureRuntime

class TestPandasAPIIntegration(unittest.TestCase):
    
    def setUp(self):
        init()
        
        self.employees_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'department_id': [101, 102, 101, 103, 102],
            'salary': [50000, 60000, 55000, 65000, 70000]
        })
        
        self.departments_df = pd.DataFrame({
            'id': [101, 102, 103],
            'name': ['Engineering', 'Marketing', 'Sales'],
            'location': ['New York', 'San Francisco', 'Chicago']
        })
        
        self.employees_df = from_df(self.employees_df, "employees", "company")
        self.departments_df = from_df(self.departments_df, "departments", "company")
        
        self.runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
    
    def tearDown(self):
        cleanup()
    
    def test_filter_columns(self):
        """Test filter operation for selecting columns"""
        result = self.employees_df.filter(items=['id', 'name'], axis=1)
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select", pure_relation)
        self.assertIn("id", pure_relation)
        self.assertIn("name", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_loc_selection(self):
        """Test loc operation for selecting columns"""
        result = self.employees_df.filter(items=['id', 'name', 'salary'], axis=1)
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertTrue(
            "company.employees" in pure_relation or 
            "pandas_db.pandas_table" in pure_relation
        )
        self.assertIn("select", pure_relation)
        self.assertIn("id", pure_relation)
        self.assertIn("name", pure_relation)
        self.assertIn("salary", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_rename_columns(self):
        """Test rename operation"""
        result = self.employees_df.rename(columns={'id': 'employee_id', 'department_id': 'dept_id'})
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("rename", pure_relation)
        self.assertIn("employee_id", pure_relation)
        self.assertIn("dept_id", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_assign_literal_values(self):
        """Test assign operation with literal values"""
        result = self.employees_df.assign(bonus=1000)
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("extend", pure_relation)
        self.assertIn("bonus", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_sort_values_single_column(self):
        """Test sort_values operation with a single column"""
        result = self.employees_df.sort_values(by='salary', ascending=False)
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("sort", pure_relation)
        self.assertIn("salary", pure_relation)
        self.assertIn("descending", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_sort_values_multiple_columns(self):
        """Test sort_values operation with multiple columns"""
        result = self.employees_df.sort_values(by=['department_id', 'salary'], ascending=[True, False])
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("sort", pure_relation)
        self.assertIn("department_id", pure_relation)
        self.assertIn("salary", pure_relation)
        self.assertIn("ascending", pure_relation)
        self.assertIn("descending", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_head(self):
        """Test head operation"""
        result = self.employees_df.head(3)
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("limit(3)", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_merge_inner_join(self):
        """Test merge operation with inner join"""
        result = self.employees_df.merge(
            self.departments_df, 
            left_on='department_id', 
            right_on='id', 
            how='inner',
            suffixes=('_emp', '_dept')
        )
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("join", pure_relation)
        self.assertIn("company.departments", pure_relation)
        self.assertIn("JoinKind.INNER", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_merge_left_join(self):
        """Test merge operation with left join"""
        result = self.employees_df.merge(
            self.departments_df, 
            left_on='department_id', 
            right_on='id', 
            how='left',
            suffixes=('_emp', '_dept')
        )
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("join", pure_relation)
        self.assertIn("company.departments", pure_relation)
        self.assertIn("JoinKind.LEFT", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_merge_with_different_column_names(self):
        """Test merge operation with different column names"""
        result = self.employees_df.merge(
            self.departments_df, 
            left_on='department_id', 
            right_on='id', 
            how='inner'
        )
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("join", pure_relation)
        self.assertIn("company.departments", pure_relation)
        self.assertIn("department_id", pure_relation)
        self.assertIn("id", pure_relation)
        self.assertIn("JoinKind.INNER", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_method_chaining(self):
        """Test method chaining"""
        result = (self.employees_df
                 .filter(items=['id', 'name', 'salary'], axis=1)
                 .rename(columns={'salary': 'annual_salary'})
                 .assign(bonus=1000)
                 .sort_values('annual_salary', ascending=False)
                 .head(3))
        
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select", pure_relation)
        self.assertIn("id", pure_relation)
        self.assertIn("name", pure_relation)
        self.assertIn("rename", pure_relation)
        self.assertIn("annual_salary", pure_relation)
        self.assertIn("extend", pure_relation)
        self.assertIn("bonus", pure_relation)
        self.assertIn("sort", pure_relation)
        self.assertIn("descending", pure_relation)
        self.assertIn("limit(3)", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_complex_query(self):
        """Test a more complex query with multiple operations"""
        result = (self.employees_df
                 .filter(items=['id', 'name', 'department_id', 'salary'], axis=1)
                 .rename(columns={'id': 'employee_id', 'department_id': 'dept_id'})
                 .assign(bonus=1000)
                 .sort_values(['dept_id', 'salary'], ascending=[True, False])
                 .head(3)
                 .merge(
                     self.departments_df.rename(columns={'id': 'dept_id'}),
                     on='dept_id',
                     how='inner'
                 ))
        
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select", pure_relation)
        self.assertIn("rename", pure_relation)
        self.assertIn("employee_id", pure_relation)
        self.assertIn("dept_id", pure_relation)
        self.assertIn("extend", pure_relation)
        self.assertIn("bonus", pure_relation)
        self.assertIn("sort", pure_relation)
        self.assertIn("ascending", pure_relation)
        self.assertIn("descending", pure_relation)
        self.assertIn("limit(3)", pure_relation)
        self.assertIn("join", pure_relation)
        self.assertIn("company.departments", pure_relation)
        self.assertIn("JoinKind.INNER", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)


    def test_create_df_dict_of_lists(self):
        """Test creating a DataFrame from a dictionary of lists"""
        data = {
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'salary': [50000, 60000, 70000]
        }
        df = create_df(data, "employees", "company")
        
        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df.columns), ['id', 'name', 'salary'])
        
        pure_relation = bind(df, self.runtime).executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
        
        result = df.filter(items=['id', 'name'], axis=1)
        pure_relation = bind(result, self.runtime).executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select", pure_relation)
        self.assertIn("id", pure_relation)
        self.assertIn("name", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)

    def test_create_df_list_of_dicts(self):
        """Test creating a DataFrame from a list of dictionaries"""
        data = [
            {'id': 1, 'name': 'Alice', 'salary': 50000},
            {'id': 2, 'name': 'Bob', 'salary': 60000},
            {'id': 3, 'name': 'Charlie', 'salary': 70000}
        ]
        df = create_df(data, "employees", "company")
        
        self.assertEqual(len(df), 3)
        self.assertListEqual(sorted(list(df.columns)), sorted(['id', 'name', 'salary']))
        
        pure_relation = bind(df, self.runtime).executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_table_creation(self):
        """Test creating a DataFrame with just table name and columns"""
        columns = {
            'id': int,
            'name': str,
            'salary': float
        }
        df = table("employees", columns, "company")
        
        self.assertEqual(len(df), 0)  # Empty DataFrame
        self.assertListEqual(sorted(list(df.columns)), sorted(['id', 'name', 'salary']))
        
        pure_relation = bind(df, self.runtime).executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_table_with_operations(self):
        """Test operations on a DataFrame created with table function"""
        columns = {
            'id': int,
            'name': str,
            'department_id': int,
            'salary': float
        }
        df = table("employees", columns, "company")
        
        data = {
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'department_id': [101, 102, 101],
            'salary': [50000.0, 60000.0, 70000.0]
        }
        for col, values in data.items():
            df[col] = values
        
        result = (df
                 .filter(items=['id', 'name', 'salary'], axis=1)
                 .rename(columns={'salary': 'annual_salary'})
                 .sort_values('annual_salary', ascending=False)
                 .head(2))
        
        pure_relation = bind(result, self.runtime).executable_to_string()
        
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select", pure_relation)
        self.assertIn("id", pure_relation)
        self.assertIn("name", pure_relation)
        self.assertIn("rename", pure_relation)
        self.assertIn("annual_salary", pure_relation)
        self.assertIn("sort", pure_relation)
        self.assertIn("descending", pure_relation)
        self.assertIn("limit(2)", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)


if __name__ == '__main__':
    unittest.main()

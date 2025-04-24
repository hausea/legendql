import unittest
import pandas as pd
from model.schema import Table, Database
from legendql.pandas_ql import PandasQL
from dialect.purerelation.dialect import NonExecutablePureRuntime


class TestPandasIntegration(unittest.TestCase):
    
    def setUp(self):
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
        
        self.runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
    
    def test_simple_select(self):
        """Test simple select operation"""
        pandas_ql = PandasQL.from_df(self.employees_df, "employees", "company")
        
        result = (pandas_ql
                  .select(['id', 'name'])
                  .bind(self.runtime))
        
        pure_relation = result.executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select(~[id, name])", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_filter_and_extend(self):
        """Test filter and extend operations"""
        pandas_ql = PandasQL.from_df(self.employees_df, "employees", "company")
        
        result = (pandas_ql
                  .select(['id', 'name', 'salary'])
                  .filter('salary > 55000')
                  .extend({'bonus': 'salary * 0.1'})
                  .bind(self.runtime))
        
        pure_relation = result.executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select(~[id, name, salary])", pure_relation)
        self.assertIn("filter(salary>55000)", pure_relation)
        self.assertIn("extend(~[bonus:salary*0.1])", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_order_by(self):
        """Test order by operations"""
        pandas_ql = PandasQL.from_df(self.employees_df, "employees", "company")
        
        result = (pandas_ql
                  .select(['department_id', 'salary'])
                  .order_by(['department_id', 'salary'], ascending=[True, False])
                  .bind(self.runtime))
        
        pure_relation = result.executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select(~[department_id, salary])", pure_relation)
        self.assertIn("sort", pure_relation)
        self.assertIn("ascending", pure_relation)
        self.assertIn("descending", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_rename_and_limit(self):
        """Test rename and limit operations"""
        pandas_ql = PandasQL.from_df(self.employees_df, "employees", "company")
        
        result = (pandas_ql
                  .select(['id', 'name', 'department_id'])
                  .rename({'id': 'employee_id', 'department_id': 'dept_id'})
                  .limit(3)
                  .bind(self.runtime))
        
        pure_relation = result.executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select(~[id, name, department_id])", pure_relation)
        self.assertIn("rename", pure_relation)
        self.assertIn("employee_id", pure_relation)
        self.assertIn("dept_id", pure_relation)
        self.assertIn("limit(3)", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_complex_query(self):
        """Test a more complex query with multiple operations"""
        pandas_ql = PandasQL.from_df(self.employees_df, "employees", "company")
        
        result = (pandas_ql
                  .select(['id', 'name', 'department_id', 'salary'])
                  .filter('salary > 50000')
                  .extend({'bonus': 'salary * 0.1', 'total_comp': 'salary + bonus'})
                  .rename({'id': 'employee_id', 'department_id': 'dept_id'})
                  .order_by(['dept_id', 'total_comp'], ascending=[True, False])
                  .limit(3)
                  .bind(self.runtime))
        
        pure_relation = result.executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("select(~[id, name, department_id, salary])", pure_relation)
        self.assertIn("filter(salary>50000)", pure_relation)
        self.assertIn("extend(~[bonus:salary*0.1, total_comp:salary+bonus])", pure_relation)
        self.assertIn("rename", pure_relation)
        self.assertIn("employee_id", pure_relation)
        self.assertIn("dept_id", pure_relation)
        self.assertIn("sort", pure_relation)
        self.assertIn("ascending", pure_relation)
        self.assertIn("descending", pure_relation)
        self.assertIn("limit(3)", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_join_operation(self):
        """Test join operation between employees and departments"""
        employees_ql = PandasQL.from_df(self.employees_df, "employees", "company")
        departments_ql = PandasQL.from_df(self.departments_df, "departments", "company")
        
        result = (employees_ql
                  .select(['id', 'name', 'department_id', 'salary'])
                  .join(departments_ql, 'department_id', how='inner')
                  .select(['employees.id', 'employees.name', 'departments.name', 'salary'])
                  .rename({'departments.name': 'department_name'})
                  .bind(self.runtime))
        
        pure_relation = result.executable_to_string()
        self.assertIn("company.employees", pure_relation)
        self.assertIn("join", pure_relation)
        self.assertIn("company.departments", pure_relation)
        self.assertIn("JoinKind.INNER", pure_relation)
        self.assertIn("department_id", pure_relation)
        self.assertIn("select", pure_relation)
        self.assertIn("rename", pure_relation)
        self.assertIn("department_name", pure_relation)
        self.assertIn("from(local::DuckDuckRuntime)", pure_relation)
    
    def test_eval_execution(self):
        """Test eval execution with runtime"""
        pandas_ql = PandasQL.from_df(self.employees_df, "employees", "company")
        
        try:
            result = (pandas_ql
                      .select(['id', 'name', 'salary'])
                      .filter('salary > 55000')
                      .eval(self.runtime))
            
            self.fail("Should have raised an exception with NonExecutablePureRuntime")
        except Exception as e:
            self.assertIsInstance(e, NotImplementedError)


if __name__ == '__main__':
    unittest.main()

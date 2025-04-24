"""
Example usage of PandasQL with Pandas DataFrames.
"""
import pandas as pd
from legendql.pandas_ql import PandasQL
from dialect.purerelation.dialect import NonExecutablePureRuntime

employees_df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
    'department_id': [101, 102, 101, 103, 102],
    'salary': [50000, 60000, 55000, 65000, 70000]
})

departments_df = pd.DataFrame({
    'id': [101, 102, 103],
    'name': ['Engineering', 'Marketing', 'Sales'],
    'location': ['New York', 'San Francisco', 'Chicago']
})

employees_ql = PandasQL.from_df(employees_df, "employees", "company")
departments_ql = PandasQL.from_df(departments_df, "departments", "company")

runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")

print("Example 1: Basic Select and Filter")
result1 = (employees_ql
          .select(['id', 'name', 'salary'])
          .filter('salary > 55000')
          .bind(runtime))

pure_relation1 = result1.executable_to_string()
print(pure_relation1)
print()

print("Example 2: Extend with Computed Columns")
result2 = (employees_ql
          .select(['id', 'name', 'salary'])
          .extend({'bonus': 'salary * 0.1', 'total_comp': 'salary + bonus'})
          .bind(runtime))

pure_relation2 = result2.executable_to_string()
print(pure_relation2)
print()

print("Example 3: Rename Columns")
result3 = (employees_ql
          .select(['id', 'name', 'department_id'])
          .rename({'id': 'employee_id', 'department_id': 'dept_id'})
          .bind(runtime))

pure_relation3 = result3.executable_to_string()
print(pure_relation3)
print()

print("Example 4: Order By and Limit")
result4 = (employees_ql
          .select(['id', 'name', 'salary'])
          .order_by(['salary', 'name'], ascending=[False, True])
          .limit(3)
          .bind(runtime))

pure_relation4 = result4.executable_to_string()
print(pure_relation4)
print()

print("Example 5: Join Operation")
result5 = (employees_ql
          .select(['id', 'name', 'department_id', 'salary'])
          .join(departments_ql, 'department_id', how='inner')
          .select(['employees.id', 'employees.name', 'departments.name', 'salary'])
          .rename({'departments.name': 'department_name'})
          .bind(runtime))

pure_relation5 = result5.executable_to_string()
print(pure_relation5)
print()

print("Example 6: Complex Query")
result6 = (employees_ql
          .select(['id', 'name', 'department_id', 'salary'])
          .filter('salary > 50000')
          .extend({'bonus': 'salary * 0.1', 'total_comp': 'salary + bonus'})
          .rename({'id': 'employee_id', 'department_id': 'dept_id'})
          .order_by(['dept_id', 'total_comp'], ascending=[True, False])
          .limit(3)
          .bind(runtime))

pure_relation6 = result6.executable_to_string()
print(pure_relation6)
print()

"""
print("Example 7: Group By (Not Implemented)")
try:
    result7 = (employees_ql
              .select(['department_id', 'salary'])
              .group_by('department_id', {'avg_salary': 'avg(salary)', 'count': 'count(id)'})
              .order_by('avg_salary', ascending=False)
              .bind(runtime))
    
    pure_relation7 = result7.executable_to_string()
    print(pure_relation7)
except NotImplementedError:
    print("Group by with aggregation functions is not implemented in the current version.")
print()
"""

print("Example of how this would be used with a real runtime:")
print("# df = result.eval()")
print("# print(df.data())")

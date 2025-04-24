"""
Example usage of Pandas API with LegendQL.

This example demonstrates how standard Pandas operations are parsed into LegendQL metamodel.
"""
import pandas as pd
from typing import Dict, List, Any, Optional, Type
from legendql.pandas_legend import init, from_df, create_df, table, bind, cleanup
from dialect.purerelation.dialect import NonExecutablePureRuntime

init()

print("=== Creating DataFrames with existing Pandas DataFrames ===")
employees_data = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
    'department_id': [101, 102, 101, 103, 102],
    'salary': [50000, 60000, 55000, 65000, 70000]
})

departments_data = pd.DataFrame({
    'id': [101, 102, 103],
    'name': ['Engineering', 'Marketing', 'Sales'],
    'location': ['New York', 'San Francisco', 'Chicago']
})

employees_df = from_df(employees_data, "employees", "company")
departments_df = from_df(departments_data, "departments", "company")

print("\n=== Creating DataFrames directly with LegendQL context ===")
employees_df2 = create_df({
    'id': [1, 2, 3, 4, 5],
    'name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
    'department_id': [101, 102, 101, 103, 102],
    'salary': [50000, 60000, 55000, 65000, 70000]
}, "employees", "company")

departments_df2 = create_df({
    'id': [101, 102, 103],
    'name': ['Engineering', 'Marketing', 'Sales'],
    'location': ['New York', 'San Francisco', 'Chicago']
}, "departments", "company")

print("\n=== Creating DataFrames from list of dictionaries ===")
employees_df3 = create_df([
    {'id': 1, 'name': 'John', 'department_id': 101, 'salary': 50000},
    {'id': 2, 'name': 'Jane', 'department_id': 102, 'salary': 60000},
    {'id': 3, 'name': 'Bob', 'department_id': 101, 'salary': 55000},
    {'id': 4, 'name': 'Alice', 'department_id': 103, 'salary': 65000},
    {'id': 5, 'name': 'Charlie', 'department_id': 102, 'salary': 70000}
], "employees", "company")

print("\n=== Creating DataFrames with just table name and columns ===")
employee_columns: Dict[str, Type] = {
    'id': int,
    'name': str,
    'department_id': int,
    'salary': float
}

department_columns: Dict[str, Type] = {
    'id': int,
    'name': str,
    'location': str
}

employees_df4 = table("employees", employee_columns, "company")
departments_df4 = table("departments", department_columns, "company")

print("\nExample: Operations on a table created with just table name and columns")
employees_data = {
    'id': [1, 2, 3],
    'name': ['John', 'Jane', 'Bob'],
    'department_id': [101, 102, 101],
    'salary': [50000.0, 60000.0, 55000.0]
}
for col, values in employees_data.items():
    employees_df4[col] = values

result_table = (employees_df4
               .filter(items=['id', 'name', 'salary'], axis=1)
               .rename(columns={'salary': 'annual_salary'})
               .sort_values('annual_salary', ascending=False)
               .head(2))

runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
pure_relation_table = bind(result_table, runtime).executable_to_string()
print(pure_relation_table)
print()

print("\n=== Operations on DataFrames created with from_df ===")
runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")

runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")

print("Example 1: Basic Column Selection with filter")
result1 = employees_df.filter(items=['id', 'name', 'salary'], axis=1)
pure_relation1 = bind(result1, runtime).executable_to_string()
print(pure_relation1)
print()

print("Example 2: Column Selection with loc")
result2 = employees_df.loc[:, ['id', 'name', 'salary']]
pure_relation2 = bind(result2, runtime).executable_to_string()
print(pure_relation2)
print()

print("Example 3: Rename Columns")
result3 = employees_df.rename(columns={'id': 'employee_id', 'department_id': 'dept_id'})
pure_relation3 = bind(result3, runtime).executable_to_string()
print(pure_relation3)
print()

print("Example 4: Add New Columns with assign")
result4 = employees_df.assign(bonus=1000)
pure_relation4 = bind(result4, runtime).executable_to_string()
print(pure_relation4)
print()

print("Example 5: Sort Values")
result5 = employees_df.sort_values(by=['department_id', 'salary'], ascending=[True, False])
pure_relation5 = bind(result5, runtime).executable_to_string()
print(pure_relation5)
print()

print("Example 6: Limit Rows with head")
result6 = employees_df.head(3)
pure_relation6 = bind(result6, runtime).executable_to_string()
print(pure_relation6)
print()

print("Example 7: Join DataFrames with merge")
result7 = employees_df.merge(departments_df, left_on='department_id', right_on='id', how='inner')
pure_relation7 = bind(result7, runtime).executable_to_string()
print(pure_relation7)
print()

print("Example 8: Method Chaining")
result8 = (employees_df
         .filter(items=['id', 'name', 'salary'], axis=1)
         .rename(columns={'salary': 'annual_salary'})
         .assign(bonus=1000)
         .sort_values('annual_salary', ascending=False)
         .head(3))
pure_relation8 = bind(result8, runtime).executable_to_string()
print(pure_relation8)
print()

print("Example 9: Group By Single Column")
result9 = employees_df.groupby('department_id')
pure_relation9 = bind(result9, runtime).executable_to_string()
print(pure_relation9)
print()

print("Example 10: Group By Multiple Columns")
result10 = employees_df.groupby(['department_id', 'name'])
pure_relation10 = bind(result10, runtime).executable_to_string()
print(pure_relation10)
print()

print("Example 11: Group By with Method Chaining")
result11 = (employees_df
          .filter(items=['id', 'name', 'department_id', 'salary'], axis=1)
          .groupby('department_id'))
pure_relation11 = bind(result11, runtime).executable_to_string()
print(pure_relation11)
print()

print("Example 12: Group By with Sum Aggregation")
result12 = employees_df.groupby('department_id').sum()
pure_relation12 = bind(result12, runtime).executable_to_string()
print(pure_relation12)
print()

print("Example 13: Group By with Mean Aggregation")
result13 = employees_df.groupby('department_id').mean()
pure_relation13 = bind(result13, runtime).executable_to_string()
print(pure_relation13)
print()

print("Example 14: Group By with Count Aggregation")
result14 = employees_df.groupby('department_id').count()
pure_relation14 = bind(result14, runtime).executable_to_string()
print(pure_relation14)
print()

print("Example 15: Group By with Min Aggregation")
result15 = employees_df.groupby('department_id').min()
pure_relation15 = bind(result15, runtime).executable_to_string()
print(pure_relation15)
print()

print("Example 16: Group By with Max Aggregation")
result16 = employees_df.groupby('department_id').max()
pure_relation16 = bind(result16, runtime).executable_to_string()
print(pure_relation16)
print()

print("Example 17: Group By with Single Column Aggregation")
result17 = employees_df.groupby('department_id').agg({'salary': 'sum'})
pure_relation17 = bind(result17, runtime).executable_to_string()
print(pure_relation17)
print()

print("Example 18: Group By with Multiple Aggregations")
result18 = employees_df.groupby('department_id').agg({
    'salary': ['sum', 'mean', 'min', 'max'],
    'id': 'count'
})
pure_relation18 = bind(result18, runtime).executable_to_string()
print(pure_relation18)
print()

print("Example 19: Group By with Method Chaining and Aggregation")
result19 = (employees_df
          .filter(items=['id', 'name', 'department_id', 'salary'], axis=1)
          .groupby('department_id')
          .agg({'salary': 'mean'}))
pure_relation19 = bind(result19, runtime).executable_to_string()
print(pure_relation19)
print()

cleanup()

print("Example of how this would be used with a real runtime:")
print("# df_result = eval(result8, runtime)")
print("# print(df_result.data())")

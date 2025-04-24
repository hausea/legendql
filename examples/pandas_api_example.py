"""
Example usage of Pandas API with LegendQL.

This example demonstrates how standard Pandas operations are parsed into LegendQL metamodel.
"""
import pandas as pd
from legendql.pandas_legend import init, from_df, bind, cleanup
from dialect.purerelation.dialect import NonExecutablePureRuntime

init()

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

employees_df = from_df(employees_df, "employees", "company")
departments_df = from_df(departments_df, "departments", "company")

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

cleanup()

print("Example of how this would be used with a real runtime:")
print("# df_result = eval(result8, runtime)")
print("# print(df_result.data())")

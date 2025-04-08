from dsl.dsl_functions import over, avg, rows, aggregate, unbounded, count, left
from legendql import *

'''
    Example DSL with Join using Fluent-style API
'''

emp = LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float, "title": str })
dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str, "location": str })

(emp
 .filter(lambda r: r.id > 10)
 .left_join(dep, lambda e, d: (
    e.dept_id == d.id,
    (department_name := d.name, department_id := d.id)))
 .extend(lambda r:[
    (ids := r.id + r.dept_id),
    (avg_val := over(r.location, avg(r.salary),
    sort=[r.name, -r.location],
    frame=rows(0, unbounded())))])
 .group_by(lambda r: aggregate(
    [ r.id, r.name ],
    [ sum_salary := sum(r.salary), count_dept := count(r.department_name) ],
    having=sum_salary > 100_000))
 .filter(lambda r: r.id > 100)
 .extend(lambda r: (calc_col := r.id + r.sum_salary))
 )

for clause in emp._clauses:
    print(clause)


'''
    PRQL example from https://prql-lang.org/book/index.html
    Using reassignment for each expression
'''

lq = LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float, "start_date": str, "benefits": str })
lq = lq.filter(lambda e: e.start_date > '2021-01-01')
lq = lq.extend(lambda e: [
        (gross_salary := e.salary + 10),
        (gross_cost := gross_salary + e.benefits) ])
lq = lq.filter(lambda e: e.gross_cost > 0)
lq = lq.group_by(lambda e: aggregate(
        [e.title, e.country],
        [avg_gross_salary := avg(e.gross_salary), sum_gross_cost := sum(e.gross_cost)],
        having=sum_gross_cost > 100_000))
lq = lq.extend(lambda e: (new_id := f"{e.title}_{e.country}"))
lq = lq.extend(lambda e: (country_code := left(e.country, 2)))
lq = lq.order_by(lambda e: [e.sum_gross_cost, -e.country])
lq = lq.limit(10)

for clause in lq._clauses:
    print(clause)

'''
    Same PRQL example using Fluent API, spacing and line breaks are important
'''

lq = (LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float, "start_date": str, "benefits": float })
 .filter(lambda e: e.start_date > '2021-01-01')
 .extend(lambda e: [
    (gross_salary := e.salary + 10),
    (gross_cost := gross_salary + e.benefits) ])
 .filter(lambda e: e.gross_cost > 0)
 .group_by(lambda e: aggregate(
    [e.title, e.country],
    [avg_gross_salary := avg(e.gross_salary),
     sum_gross_cost := sum(e.gross_cost)],
    having=e.sum_gross_cost > 100_000))
 .extend(lambda e: (new_id := f"{e.title}_{e.country}"))
 .extend(lambda e: (country_code := left(e.country, 2)))
 .order_by(lambda e: [e.sum_gross_cost, -e.country])
 .limit(10))

for clause in lq._clauses:
    print(clause)


'''
    Example Window as an expression
'''
emp = (
LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float, "location": str })
 .extend(lambda r: (
    avg_val := over(
        r.location,
        (avg_val := avg(r.salary)),
        sort=[r.name, -r.location],
        frame=rows(0, unbounded()),
        qualify=avg_val > 100_000)))
 )

for clause in emp._clauses:
    print(clause)

emp = (
LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float, "location": str })
 .extend(lambda r: (avg_val := over(r.location, avg(r.salary))))
 )

for clause in emp._clauses:
    print(clause)

emp = (
LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
 .group_by(lambda r: aggregate(r.title, avg_gross_salary := avg(r.gross_salary)))
 )

for clause in emp._clauses:
    print(clause)

emp = (
LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
 .group_by(lambda r: aggregate([r.title, r.dept_id], avg_gross_salary := avg(r.gross_salary)))
 )

for clause in emp._clauses:
    print(clause)

emp = (
LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
 .group_by(lambda r: aggregate(r.title, avg_gross_salary := avg(r.gross_salary), having=avg_gross_salary > 100_000))
 )

for clause in emp._clauses:
    print(clause)


emp = LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })
loc = LegendQL.from_("location", columns={ "id": int, "name": str, "country": str, "code": str })

(emp
 .left_join(dep, lambda e, d: (e.dept_id == d.id, (new_dept_id := d.id, new_dept_name := d.name)))
 .left_join(loc, lambda d, l: (d.city == l.id, (new_loc_id := l.id, new_loc_name := l.name, new_loc_code := l.code))))

for clause in emp._clauses:
    print(clause)


emp = LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })
loc = LegendQL.from_("location", columns={ "id": int, "name": str, "country": str, "code": str })

(emp
 .left_join(dep, lambda e, d: (e.dept_id == d.id, [(new_dept_id := d.id), (new_dept_name := d.name)]))
 .left_join(loc, lambda d, l: (d.city == l.id, (new_loc_id := l.id, new_loc_name := l.name, new_loc_code := l.code))))

for clause in emp._clauses:
    print(clause)

dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })
dep.extend(lambda e: (id_plus_one := e.id + 1))

print(dep.schema.columns)

for clause in dep._clauses:
    print(clause)


emp = LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })
emp.left_join(dep, lambda e, d: (e.dept_id == d.id, [(new_dept_id := d.id), (new_dept_name := d.name)]))

print(emp.schema.columns)


dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })
dep.rename(lambda d: (new_dept_id := d.id, new_dept_name := d.name))

print(dep.schema.columns)

for clause in dep._clauses:
    print(clause)


dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })
dep.select(lambda d: [d.id, d.name])

print(dep.schema.columns)

for clause in dep._clauses:
    print(clause)


dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })
dep.group_by(lambda d: aggregate([d.id, d.name], (sum_test := sum(d.code), count_test := count(d.city))))

print(dep.schema.columns)

for clause in dep._clauses:
    print(clause)

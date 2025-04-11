import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from dsl.functions import aggregate
from model.schema import Table, Database
from ql.legendql import LegendQL


class TestDslToPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (LegendQL.from_table(database, table)
                      .select(lambda e: [e.id, e.departmentId])
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_filter(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (LegendQL.from_table(database, table)
                      .select(lambda e: [e.id, e.departmentId])
                      .filter(lambda e: e.id == 1)
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->filter(e | $e.id==1)->from(local::DuckDuckRuntime)", pure_relation)

    def test_simple_select_with_extend(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (LegendQL.from_table(database, table)
                      .select(lambda e: [e.id, e.departmentId])
                      .extend(lambda e: [new_col := e.id + 1])
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->extend(~[new_col:e | $e.id+1])->from(local::DuckDuckRuntime)", pure_relation)

    @unittest.skip("need to support to-string for functions and clean up function metamodel")
    def test_simple_select_with_groupBy(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (LegendQL.from_table(database, table)
                      .select(lambda e: [e.id, e.departmentId, e.first, e.last])
                      .group_by(lambda r: aggregate(
                                            [r.last],
                                            [sum_of_id := sum(r.id + 1)],
                                            having=sum_of_id > 0))
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId, first, last])->groupBy(~[last], ~[sum_of_id:r | $r.id+1 : a | $a->sum(), r | $r.sum_of_id > 0])->from(local::DuckDuckRuntime)",
            pure_relation)

    def test_simple_select_with_limit(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        table = Table("table", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        data_frame = (LegendQL.from_table(database, table)
                      .select(lambda e: [e.id, e.departmentId])
                      .limit(1)
                      .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->limit(1)->from(local::DuckDuckRuntime)", pure_relation)

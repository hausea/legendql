import json
import unittest

from model.schema import Schema
from ql.legendql import LegendQL
from runtime.pure.db.duckdb import DuckDBDatabaseType
from runtime.pure.executionserver.runtime import ExecutionServerRuntime


class TestExecutionServerEvaluation(unittest.TestCase):
    def setUp(self):
        pass

    def test_execution_against_execution_server(self):
        schema = Schema("local::DuckDuckDatabase", "employees", {"id": int, "departmentId": int, "first": str, "last": str})
        runtime = ExecutionServerRuntime("local::DuckDuckRuntime", DuckDBDatabaseType("/Users/ahauser/.legend/repl/duck"), "http://localhost:6300", [schema])

        data_frame = (LegendQL.from_schema(schema)
                      .select(lambda r: [r.id, r.departmentId, r.first, r.last])
                      .bind(runtime))

        result = data_frame.eval()
        print(json.dumps(result, indent=4))

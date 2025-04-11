from dataclasses import dataclass
from typing import List

import requests

from dialect.purerelation.dialect import PureRuntime
from model.metamodel import Clause
from model.schema import Table, Database
from runtime.pure.db.type import DatabaseType


@dataclass
class ExecutionServerRuntime(PureRuntime):
    database_type: DatabaseType
    host: str
    database: Database

    def eval(self, clauses: List[Clause]) -> dict:
        lam = self.executable_to_string(clauses)
        model = self._generate_model()
        query = self._parse_lambda(lam)
        pmcd = self._parse_model(model)
        runtime = pmcd["elements"][0]["runtimeValue"]

        execution_input = {"clientVersion": "vX_X_X", "context": {"_type": "BaseExecutionContext"}, "function": query, "runtime": runtime, "model": pmcd}
        return self._execute(execution_input)

    def _parse_model(self, model: str) -> dict:
        return requests.post(self.host + "/api/pure/v1/grammar/grammarToJson/model", data=model).json()

    def _parse_lambda(self, lam: str) -> dict:
        return requests.post(self.host + "/api/pure/v1/grammar/grammarToJson/lambda", data="|" + lam).json()

    def _execute(self, input: dict) -> dict:
        return requests.post(self.host + "/api/pure/v1/execution/execute?serializationFormat=DEFAULT", json=input).json()

    def _generate_model(self) -> str:
        return self.database_type.generate_model(self.name, self.database)
from __future__ import annotations
from dataclasses import dataclass
from typing import List

from model.metamodel import SelectionClause, Runtime, DataFrame, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, JoinClause, JoinType, JoinExpression, Clause, FromClause, Expression, IntegerLiteral, \
    GroupByExpression, ColumnReferenceExpression


@dataclass
class LegendQL:
    _clauses: List[Clause]

    @classmethod
    def from_db(cls, database: str, table: str) -> LegendQL:
        return LegendQL([FromClause(database, table)])

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self._clauses)

    def eval[R: Runtime, T](self, runtime: R) -> T:
        return self.bind(runtime).eval()

    def select(self, *names: str) -> LegendQL:
        self._clauses.append(SelectionClause(list(map(lambda name: ColumnReferenceExpression(name), names))))
        return self

    def extend(self, extend: List[ExtendExpression]) -> LegendQL:
        self._clauses.append(ExtendClause(extend))
        return self

    def filter(self, filter_clause: Expression) -> LegendQL:
        self._clauses.append(FilterClause(filter_clause))
        return self

    def group_by(self, selections: List[Expression], group_by: List[Expression], having: Expression = None) -> LegendQL:
        self._clauses.append(GroupByClause(GroupByExpression(selections, group_by, having)))
        return self

    def limit(self, limit: int) -> LegendQL:
        self._clauses.append(LimitClause(IntegerLiteral(limit)))
        return self

    def join(self, database: str, table: str, join_type: JoinType, on_clause: Expression) -> LegendQL:
        self._clauses.append(JoinClause(FromClause(database, table), join_type, JoinExpression(on_clause)))
        return self

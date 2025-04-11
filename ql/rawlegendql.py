from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Type, Dict

from model.metamodel import SelectionClause, Runtime, DataFrame, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, JoinClause, JoinType, JoinExpression, Clause, FromClause, Expression, IntegerLiteral, \
    GroupByExpression, ColumnReferenceExpression, RenameClause, ColumnAliasExpression, OffsetClause, OrderByExpression, \
    OrderByClause
from model.schema import Table, Database


@dataclass
class RawLegendQL:
    _database: Database
    _table: Table
    _clauses: List[Clause]

    @classmethod
    def from_table(cls, database: Database, table: Table) -> RawLegendQL:
        return RawLegendQL(database, Table(table.table, table.columns.copy()), [FromClause(database.name, table.table)])

    @classmethod
    def from_db(cls, database: Database, table: str, columns: Dict[str, Type]) -> RawLegendQL:
        return RawLegendQL.from_table(database, Table(table, columns))

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self._clauses)

    def eval[R: Runtime, T](self, runtime: R) -> T:
        return self.bind(runtime).eval()

    def _add_clause(self, clause: Clause) -> None:
        self._clauses.append(clause)

    def _update_table(self, table: Table) -> None:
        self._table = table

    def select(self, *names: str) -> RawLegendQL:
        self._add_clause(SelectionClause(list(map(lambda name: ColumnReferenceExpression(name), names))))
        return self

    def rename(self, *renames: Tuple[str, str]) -> RawLegendQL:
        self._add_clause(RenameClause(list(map(lambda rename: ColumnAliasExpression(alias=rename[1], reference=ColumnReferenceExpression(name=rename[0])), renames))))
        return self

    def extend(self, extend: List[Expression]) -> RawLegendQL:
        self._add_clause(ExtendClause(extend))
        return self

    def filter(self, filter_clause: Expression) -> RawLegendQL:
        self._add_clause(FilterClause(filter_clause))
        return self

    def group_by(self, selections: List[Expression], group_by: List[Expression], having: Expression = None) -> RawLegendQL:
        self._add_clause(GroupByClause(GroupByExpression(selections, group_by, having)))
        return self

    def limit(self, limit: int) -> RawLegendQL:
        self._add_clause(LimitClause(IntegerLiteral(limit)))
        return self

    def offset(self, offset: int) -> RawLegendQL:
        self._add_clause(OffsetClause(IntegerLiteral(offset)))
        return self

    def order_by(self, *ordering: OrderByExpression) -> RawLegendQL:
        self._add_clause(OrderByClause(list(ordering)))
        return self

    def join(self, database: str, table: str, join_type: JoinType, on_clause: Expression) -> RawLegendQL:
        self._add_clause(JoinClause(FromClause(database, table), join_type, JoinExpression(on_clause)))
        return self

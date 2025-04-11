from __future__ import annotations

from typing import Callable, Type, Dict, List

from dsl import parser
from model.metamodel import FromClause, OrderByClause, LimitClause, IntegerLiteral, OffsetClause, RenameClause, \
    LeftJoinType, InnerJoinType, Runtime, DataFrame
from dsl.parser import ParseType
from model.metamodel import SelectionClause, ExtendClause, FilterClause, GroupByClause, JoinClause, JoinType, Clause
from model.schema import Table, Database
from ql.rawlegendql import RawLegendQL


class LegendQL:
    _internal: RawLegendQL

    def __init__(self, database: Database, table: Table):
        self._internal = RawLegendQL.from_table(database, table)

    @classmethod
    def from_table(cls, database: Database, table: Table) -> LegendQL:
        return LegendQL(database, table)

    @classmethod
    def from_db(cls, database: Database, table: str, columns: Dict[str, Type]) -> LegendQL:
        return LegendQL.from_table(database, Table(table, columns))

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return self._internal.bind(runtime)

    def eval[R: Runtime, T](self, runtime: R) -> T:
        return self._internal.eval(runtime)

    def select(self, columns: Callable) -> LegendQL:
        expression_and_table = parser.Parser.parse(columns, [self._internal._table], ParseType.select)
        self._internal._add_clause(SelectionClause(expression_and_table[0]))
        self._internal._update_table(expression_and_table[1])
        return self

    def extend(self, columns: Callable) -> LegendQL:
        expression_and_table = parser.Parser.parse(columns, [self._internal._table], ParseType.extend)
        self._internal._add_clause(ExtendClause(expression_and_table[0]))
        self._internal._update_table(expression_and_table[1])
        return self

    def rename(self, columns: Callable) -> LegendQL:
        expression_and_table = parser.Parser.parse(columns, [self._internal._table], ParseType.rename)
        self._internal._add_clause(RenameClause(expression_and_table[0]))
        self._internal._update_table(expression_and_table[1])
        return self

    def filter(self, condition: Callable) -> LegendQL:
        expression_and_table = parser.Parser.parse(condition, [self._internal._table], ParseType.filter)
        self._internal._add_clause(FilterClause(expression_and_table[0]))
        self._internal._update_table(expression_and_table[1])
        return self

    def group_by(self, aggr: Callable) -> LegendQL:
        expression_and_table = parser.Parser.parse(aggr, [self._internal._table], ParseType.group_by)
        self._internal._add_clause(GroupByClause(expression_and_table[0]))
        self._internal._update_table(expression_and_table[1])
        return self

    def _join(self, lq: LegendQL, join: Callable, join_type: JoinType) -> LegendQL:
        expression_and_table = parser.Parser.parse(join, [self._internal._table, lq._internal._table], ParseType.join)
        self._internal._add_clause(JoinClause(FromClause(lq._internal._database.name, lq._internal._table.table), join_type, expression_and_table[0]))
        self._internal._update_table(expression_and_table[1])
        return self

    def join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, InnerJoinType())

    def left_join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, LeftJoinType())

    def order_by(self, columns: Callable) -> LegendQL:
        expression_and_table = parser.Parser.parse(columns, [self._internal._table], ParseType.order_by)
        self._internal._add_clause(OrderByClause(expression_and_table[0]))
        self._internal._update_table(expression_and_table[1])
        return self

    def limit(self, limit: int) -> LegendQL:
        clause = LimitClause(IntegerLiteral(limit))
        self._internal._add_clause(clause)
        return self

    def offset(self, offset: int) -> LegendQL:
        clause = OffsetClause(IntegerLiteral(offset))
        self._internal._add_clause(clause)
        return self

    def take(self, offset: int, limit: int) -> LegendQL:
        clause = OffsetClause(IntegerLiteral(offset))
        self._internal._add_clause(clause)

        clause = LimitClause(IntegerLiteral(limit))
        self._internal._add_clause(clause)
        return self

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Type, Dict, List

from dsl import parser
from dsl.dsl_functions import *
from dsl.metamodel import FromClause, WithClause, OrderByClause, LimitClause, IntegerLiteral, OffsetClause, RenameClause
from dsl.parser import ParseType
from metamodel import SelectClause, ExtendClause, FilterClause, GroupByClause, JoinClause, JoinType, Expression, Clause
from dsl.schema import Schema


class LegendQL:

    def __init__(self, schema: Schema, from_clause: FromClause):
        self.schema = schema
        self._clauses: List[Clause] = [from_clause]

    @classmethod
    def from_(cls, name: str, columns: Dict[str, Type]) -> LegendQL:
        return LegendQL(Schema(name, columns), FromClause(name, name))

    def select(self, columns: Callable) -> LegendQL:
        self.schema.columns.clear()
        clause = SelectClause(parser.Parser.parse(columns, self.schema, ParseType.select))
        self._clauses.append(clause)
        self.schema.update_name()
        return self

    def extend(self, columns: Callable) -> LegendQL:
        clause = ExtendClause(parser.Parser.parse(columns, self.schema, ParseType.extend))
        self._clauses.append(clause)
        self.schema.update_name()
        return self

    def rename(self, columns: Callable) -> LegendQL:
        clause = RenameClause(parser.Parser.parse(columns, self.schema, ParseType.rename))
        self._clauses.append(clause)
        self.schema.update_name()
        return self

    def filter(self, condition: Callable) -> LegendQL:
        clause = FilterClause(parser.Parser.parse(condition, self.schema, ParseType.filter))
        self._clauses.append(clause)
        self.schema.update_name()
        return self

    def group_by(self, aggr: Callable) -> LegendQL:
        self.schema.columns.clear()
        clause = GroupByClause(parser.Parser.parse(aggr, self.schema, ParseType.group_by))
        self._clauses.append(clause)
        self.schema.update_name()
        return self

    def _join(self, lq: LegendQL, join: Callable, join_type: JoinType) -> LegendQL:

        expr = parser.Parser.parse_join(join, self.schema, lq.schema)

        if isinstance(expr, Expression):
            clause = JoinClause(right=lq.schema, condition=expr, type=join_type)
            self._clauses.append(clause)
        elif isinstance(expr, List) and len(expr) == 2:
            clause = RenameClause([expr[1]])
            self._clauses.append(clause)

            clause = JoinClause(right=lq.schema, condition=expr[0], type=join_type)
            self._clauses.append(clause)
        else:
            raise ValueError(f"Badly formed Join: {join} {expr}")

        if any(key in lq.schema.columns for key in self.schema.columns):
            raise ValueError(f"you have not renamed all the overlapping columns: {lq.schema.columns}, {self.schema.columns}")

        self.schema.columns.update(lq.schema.columns)
        self.schema.update_name()

        return self

    def join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, JoinType.INNER)

    def left_join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, JoinType.LEFT)

    def order_by(self, columns: Callable) -> LegendQL:
        clause = OrderByClause(parser.Parser.parse(columns, self.schema, ParseType.order_by))
        self._clauses.append(clause)
        return self

    def limit(self, limit: int) -> LegendQL:
        clause = LimitClause(IntegerLiteral(limit))
        self._clauses.append(clause)
        return self

    def offset(self, offset: int) -> LegendQL:
        clause = OffsetClause(IntegerLiteral(offset))
        self._clauses.append(clause)
        return self

    def take(self, offset: int, limit: int) -> LegendQL:
        clause = OffsetClause(IntegerLiteral(offset))
        self._clauses.append(clause)

        clause = LimitClause(IntegerLiteral(limit))
        self._clauses.append(clause)
        return self

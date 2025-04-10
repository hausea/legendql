from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Type, Dict, List

from dsl import parser
from model.metamodel import FromClause, OrderByClause, LimitClause, IntegerLiteral, OffsetClause, RenameClause, \
    LeftJoinType, InnerJoinType, JoinExpression
from dsl.parser import ParseType
from model.metamodel import SelectionClause, ExtendClause, FilterClause, GroupByClause, JoinClause, JoinType, Expression, Clause
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
        expression_and_schema = parser.Parser.parse(columns, [self.schema], ParseType.select)
        self._clauses.append(SelectionClause(expression_and_schema[0]))
        self.schema = expression_and_schema[1]
        return self

    def extend(self, columns: Callable) -> LegendQL:
        expression_and_schema = parser.Parser.parse(columns, [self.schema], ParseType.extend)
        self._clauses.append(ExtendClause(expression_and_schema[0]))
        self.schema = expression_and_schema[1]
        return self

    def rename(self, columns: Callable) -> LegendQL:
        expression_and_schema = parser.Parser.parse(columns, [self.schema], ParseType.rename)
        self._clauses.append(RenameClause(expression_and_schema[0]))
        self.schema = expression_and_schema[1]
        return self

    def filter(self, condition: Callable) -> LegendQL:
        expression_and_schema = parser.Parser.parse(condition, [self.schema], ParseType.filter)
        self._clauses.append(FilterClause(expression_and_schema[0]))
        self.schema = expression_and_schema[1]
        return self

    def group_by(self, aggr: Callable) -> LegendQL:
        expression_and_schema = parser.Parser.parse(aggr, [self.schema], ParseType.group_by)
        self._clauses.append(GroupByClause(expression_and_schema[0]))
        self.schema = expression_and_schema[1]
        return self

    def _join(self, lq: LegendQL, join: Callable, join_type: JoinType) -> LegendQL:
        expression_and_schema = parser.Parser.parse(join, [self.schema, lq.schema], ParseType.join)
        self._clauses.append(JoinClause(FromClause(lq.schema.name, lq.schema.name), join_type, expression_and_schema[0]))
        self.schema = expression_and_schema[1]
        return self

    def join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, InnerJoinType())

    def left_join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, LeftJoinType())

    def order_by(self, columns: Callable) -> LegendQL:
        expression_and_schema = parser.Parser.parse(columns, [self.schema], ParseType.order_by)
        self._clauses.append(OrderByClause(expression_and_schema[0]))
        self.schema = expression_and_schema[1]
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

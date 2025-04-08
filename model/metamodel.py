from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import date
from enum import Enum
from typing import List, Optional
from dataclasses import dataclass

class Literal[T](ABC):
    @abstractmethod
    def value(self) -> T:
        pass

    @abstractmethod
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

@dataclass
class IntegerLiteral(Literal):
    val: int
    def __init__(self, val):
        self.val = val

    def value(self) -> int:
        return self.val

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_integer_literal(self, parameter)

@dataclass
class StringLiteral(Literal):
    val: str
    def __init__(self, val):
        self.val = val

    def value(self) -> str:
        return self.val

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_string_literal(self, parameter)

@dataclass
class DateLiteral(Literal):
    val: date
    def __init__(self, val):
        self.val = val

    def value(self) -> date:
        return self.val

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_date_literal(self, parameter)

@dataclass
class BooleanLiteral(Literal):
    val: bool
    def __init__(self, val):
        self.val = val

    def value(self) -> bool:
        return self.val

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_boolean_literal(self, parameter)

class Function(ABC):
    @abstractmethod
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

@dataclass
class CountFunction(Function):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_count_function(self, parameter)

class Expression(ABC):
    @abstractmethod
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

class Operator(ABC):
    @abstractmethod
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

class UnaryOperator(Operator, ABC):
    pass

@dataclass
class NotUnaryOperator(UnaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_not_unary_operator(self, parameter)

class BinaryOperator(Operator, ABC):
    pass

@dataclass
class EqualsBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_equals_binary_operator(self, parameter)

@dataclass
class NotEqualsBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_not_equals_binary_operator(self, parameter)

@dataclass
class GreaterThanBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_greater_than_binary_operator(self, parameter)

@dataclass
class GreaterThanEqualsBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_greater_than_equals_operator(self, parameter)

@dataclass
class LessThanBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_less_than_binary_operator(self, parameter)

@dataclass
class LessThanEqualsBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_less_than_equals_binary_operator(self, parameter)

@dataclass
class InBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_in_binary_operator(self, parameter)

@dataclass
class NotInBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_not_in_binary_operator(self, parameter)

@dataclass
class IsBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_is_binary_operator(self, parameter)

@dataclass
class IsNotBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_is_not_binary_operator(self, parameter)

@dataclass
class AndBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_and_binary_operator(self, parameter)

@dataclass
class OrBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_or_binary_operator(self, parameter)

@dataclass
class AddBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_add_binary_operator(self, parameter)

@dataclass
class MultiplyBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_multiply_binary_operator(self, parameter)

@dataclass
class SubtractBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_subtract_binary_operator(self, parameter)

@dataclass
class DivideBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_divide_binary_operator(self, parameter)

@dataclass
class ModuloBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_modulo_binary_operator(self, parameter)

@dataclass
class ExponentBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_exponent_binary_operator(self, parameter)

@dataclass
class BitwiseAndBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_bitwise_and_binary_operator(self, parameter)

@dataclass
class BitwiseOrBinaryOperator(BinaryOperator):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_bitwise_or_binary_operator(self, parameter)

@dataclass
class OperandExpression(Expression):
    expression: Expression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_operand_expression(self, parameter)

@dataclass
class UnaryExpression(Expression):
    operator: UnaryOperator
    expression: OperandExpression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_unary_expression(self, parameter)

@dataclass
class BinaryExpression(Expression):
    left: OperandExpression
    right: OperandExpression
    operator: BinaryOperator

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_binary_expression(self, parameter)

@dataclass
class LiteralExpression(Expression):
    literal: Literal

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_literal_expression(self, parameter)

@dataclass
class AliasExpression(Expression, ABC):
    alias: str = None

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_alias_expression(self, parameter)

@dataclass
class SelectionExpression(Expression):
    name: str = None
    expression: Optional[Expression] = None

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_selection_expression(self, parameter)

@dataclass
class ColumnExpression(Expression):
    name: str
    expression: Expression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_column_expression(self, parameter)

@dataclass
class ColumnReference(Expression):
    name: str
    table: str
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_column_reference(self, parameter)

@dataclass
class IfExpression(Expression):
    test: Expression
    body: Expression
    orelse: Expression
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_if_expression(self, parameter)

@dataclass
class NotExpression(Expression):
    expression: Expression
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_not_expression(self, parameter)

class Sort(Enum):
    ASC = "ASC"
    DESC = "DESC"

    def __str__(self):
        return self.value

@dataclass
class SortExpression(Expression):
    direction: Sort
    expression: Expression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_sort_expression(self, parameter)

@dataclass
class ReferenceExpression(AliasExpression):
    ref: str = None

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_reference_expression(self, parameter)

@dataclass
class FunctionExpression(Expression):
    function: Function
    parameters: List[Expression]

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_function_expression(self, parameter)

class Clause(ABC):
    pass

@dataclass
class RenameClause(Clause):
    expressions: List[Expression]

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_rename_clause(self, parameter)


@dataclass
class FilterClause(Clause):
    expression: Expression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_filter_clause(self, parameter)

@dataclass
class SelectionClause(Clause):
    expressions: List[Expression]

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_selection_clause(self, parameter)

@dataclass
class ExtendClause(Clause):
    expressions: List[Expression]

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_extend_clause(self, parameter)

@dataclass
class ExtendExpression(Expression):
    alias: str
    expression: Expression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_extend_expression(self, parameter)

@dataclass
class GroupByClause(Clause):
    expression: Expression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_group_by_clause(self, parameter)

@dataclass
class GroupByExpression(Expression):
    selections: List[Expression]
    expressions: List[Expression]
    having: Expression = None

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_group_by_expression(self, parameter)

@dataclass
class DistinctClause(Clause):
    expressions: List[Expression]

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_distinct_clause(self, parameter)

@dataclass
class LimitClause(Clause):
    value: IntegerLiteral

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_limit_clause(self, parameter)

@dataclass
class FromClause(Clause):
    database: str
    table: str
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_from_clause(self, parameter)

class JoinType(ABC):
    @abstractmethod
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

@dataclass
class InnerJoinType(JoinType):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_inner_join_type(self, parameter)

@dataclass
class LeftJoinType(JoinType):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_left_join_type(self, parameter)

@dataclass
class JoinExpression(Expression):
    on: Expression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_join_expression(self, parameter)

@dataclass
class JoinClause(Clause):
    from_clause: FromClause
    join_type: JoinType
    on_clause: JoinExpression

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_join_clause(self, parameter)

@dataclass
class OffsetClause(Clause):
    value: IntegerLiteral

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_offset_clause(self, parameter)

@dataclass
class OrderByClause(Clause):
    expressions: List[Expression]

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_order_by_clause(self, parameter)

class Runtime(ABC):
    @abstractmethod
    def eval[T](self, clauses: List[Clause]) -> T:
        pass

    @abstractmethod
    def executable_to_string(self, clauses: List[Clause]) -> str:
        pass

    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_runtime(self, parameter)

@dataclass
class DataFrame(ABC):
    runtime: Runtime
    clauses: List[Clause]

    def eval[T](self) -> T:
        return self.runtime.eval(self.clauses)

    def executable_to_string(self) -> str:
        return self.runtime.executable_to_string(self.clauses)

class ExecutionVisitor(ABC):
    @abstractmethod
    def visit_runtime[P, T, R: Runtime](self, val: R, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_from_clause[P, T](self, val: FromClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_integer_literal[P, T](self, val: IntegerLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_string_literal[P, T](self, val: StringLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_date_literal[P, T](self, val: DateLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_boolean_literal[P, T](self, val: BooleanLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_operand_expression[P, T](self, val: OperandExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_not_unary_operator[P, T](self, val: NotUnaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_equals_binary_operator[P, T](self, val: EqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_not_equals_binary_operator[P, T](self, val: NotEqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_greater_than_binary_operator[P, T](self, val: GreaterThanBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_greater_than_equals_operator[P, T](self, val: GreaterThanEqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_less_than_binary_operator[P, T](self, val: LessThanBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_less_than_equals_binary_operator[P, T](self, val: LessThanEqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_and_binary_operator[P, T](self, val: AndBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_or_binary_operator[P, T](self, val: OrBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_add_binary_operator[P, T](self, val: AddBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_multiply_binary_operator[P, T](self, val: MultiplyBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_subtract_binary_operator[P, T](self, val: SubtractBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_divide_binary_operator[P, T](self, val: DivideBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_literal_expression[P, T](self, val: LiteralExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_unary_expression[P, T](self, val: UnaryExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_binary_expression[P, T](self, val: BinaryExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_alias_expression[P, T](self, val: AliasExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_selection_expression[P, T](self, val: SelectionExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_reference_expression[P, T](self, val: ReferenceExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_function_expression[P, T](self, val: FunctionExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_count_function[P, T](self, val: CountFunction, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_filter_clause[P, T](self, val: FilterClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_selection_clause[P, T](self, val: SelectionClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_extend_clause[P, T](self, val: ExtendClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_extend_expression[P, T](self, val: ExtendExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_group_by_clause[P, T](self, val: GroupByClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_group_by_expression[P, T](self, val: GroupByExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_distinct_clause[P, T](self, val: DistinctClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_limit_clause[P, T](self, val: LimitClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_join_expression[P, T](self, val: JoinExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_join_clause[P, T](self, val: JoinClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_inner_join_type[P, T](self, val: InnerJoinType, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_left_join_type[P, T](self, val: LeftJoinType, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_column_expression[P, T](self, val: ColumnExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_column_reference[P, T](self, val: ColumnReference, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_if_expression[P, T](self, val: IfExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_not_expression[P, T](self, val: NotExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_sort_expression[P, T](self, val: SortExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_rename_clause[P, T](self, val: RenameClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_offset_clause[P, T](self, val: OffsetClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_order_by_clause[P, T](self, val: OrderByClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_in_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_not_in_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_is_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_is_not_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_modulo_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_exponent_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_bitwise_and_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_bitwise_or_binary_operator[P, T](self, self1, parameter: P) -> T:
        raise NotImplementedError()

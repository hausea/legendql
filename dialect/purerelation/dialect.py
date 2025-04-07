from abc import ABC
from dataclasses import dataclass
from typing import Set, List

from model.metamodel import ExecutionVisitor, JoinClause, LimitClause, DistinctClause, GroupByClause, ExtendClause, \
    SelectionClause, FilterClause, FunctionExpression, SelectionExpression, LiteralExpression, BinaryExpression, \
    UnaryExpression, OperandExpression, BooleanLiteral, StringLiteral, IntegerLiteral, Runtime, \
    OrBinaryOperator, AndBinaryOperator, LessThanEqualsBinaryOperator, LessThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, GreaterThanBinaryOperator, NotEqualsBinaryOperator, EqualsBinaryOperator, \
    NotUnaryOperator, InnerJoinType, LeftJoinType, ReferenceExpression, AliasExpression, ExtendExpression, \
    GroupByExpression, CountFunction, JoinExpression, Expression, Clause, FromClause, AddBinaryOperator, \
    MultiplyBinaryOperator, SubtractBinaryOperator, DivideBinaryOperator


@dataclass
class PureRuntime(Runtime, ABC):
    name: str

    def executable_to_string(self, clauses: List[Clause]) -> str:
        visitor = PureRelationExpressionVisitor(self)
        return "->".join(map(lambda clause: clause.visit(visitor, ""), clauses)) + self.visit(visitor, "")

class NonExecutablePureRuntime(PureRuntime):
    def eval(self, clauses: List[Clause]) -> str:
        raise NotImplementedError()

class ReferenceNameExtractorExpressionVisitor(ExecutionVisitor):

    def visit_runtime(self, val: PureRuntime, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_from_clause(self, val: FromClause, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_integer_literal(self, val: IntegerLiteral, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_string_literal(self, val: StringLiteral, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_boolean_literal(self, val: BooleanLiteral, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_operand_expression(self, val: OperandExpression, parameter: Set[str]) -> Set[str]:
        return parameter | val.expression.visit(self, parameter)

    def visit_unary_expression(self, val: UnaryExpression, parameter: Set[str]) -> Set[str]:
        return parameter | val.expression.visit(self, parameter)

    def visit_binary_expression(self, val: BinaryExpression, parameter: Set[str]) -> Set[str]:
        return parameter | val.left.visit(self, set()) | val.right.visit(self, set())

    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_add_binary_operator(self, val: AddBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_multiply_binary_operator(self, val: MultiplyBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_subtract_binary_operator(self, val: SubtractBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_divide_binary_operator(self, val: DivideBinaryOperator, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_literal_expression(self, val: LiteralExpression, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_alias_expression(self, val: AliasExpression, parameter: Set[str]) -> Set[str]:
        return parameter | set(val.alias)

    def visit_selection_expression(self, val: SelectionExpression, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_reference_expression(self, val: ReferenceExpression, parameter: Set[str]) -> Set[str]:
        return parameter | set(val.alias)

    def visit_function_expression(self, val: FunctionExpression, parameter: Set[str]) -> Set[str]:
        result = parameter
        if val.parameters is not None:
            for param in val.parameters:
                result = result | param.visit(self, set())
        return result

    def visit_count_function(self, val: CountFunction, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_filter_clause(self, val: FilterClause, parameter: Set[str]) -> Set[str]:
        return parameter |val.expression.visit(self, set())

    def visit_selection_clause(self, val: SelectionClause, parameter: Set[str]) -> Set[str]:
        result = parameter
        if val.expressions is not None:
            for expression in val.expressions:
                result = result | expression.visit(self, set())
        return result

    def visit_extend_clause(self, val: ExtendClause, parameter: Set[str]) -> Set[str]:
        result = parameter
        if val.expressions is not None:
            for expression in val.expressions:
                result = result | expression.visit(self, set())
        return result

    def visit_extend_expression(self, val: ExtendExpression, parameter: Set[str]) -> Set[str]:
        return val.expression.visit(self, parameter)

    def visit_group_by_clause(self, val: GroupByClause, parameter: Set[str]) -> Set[str]:
        result = parameter
        if val.expressions is not None:
            for expression in val.expressions:
                result = result | expression.visit(self, set())
        if val.having is not None:
            result = result | val.having.visit(self, set())
        return result

    def visit_group_by_expression(self, val: GroupByExpression, parameter: Set[str]) -> Set[str]:
        return val.selection.visit(self, parameter)

    def visit_distinct_clause(self, val: DistinctClause, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_limit_clause(self, val: LimitClause, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_join_clause(self, val: JoinClause, parameter: Set[str]) -> Set[str]:
        return parameter | val.on_clause.visit(self, set())

    def visit_join_expression(self, val: JoinExpression, parameter: Set[str]) -> Set[str]:
        raise val.on.visit(self, parameter)

    def visit_inner_join_type(self, val: InnerJoinType, parameter: Set[str]) -> Set[str]:
        return parameter

    def visit_left_join_type(self, val: LeftJoinType, parameter: Set[str]) -> Set[str]:
        return parameter

@dataclass
class PureRelationExpressionVisitor(ExecutionVisitor):
    runtime: PureRuntime
    var_extractor: ReferenceNameExtractorExpressionVisitor = ReferenceNameExtractorExpressionVisitor()

    def extract_variables(self, expression: Expression) -> List[str]:
        return sorted(expression.visit(self.var_extractor, set()))

    def visit_runtime(self, val: PureRuntime, parameter: str) -> str:
        return "->from(" + val.name + ")"

    def visit_from_clause(self, val: FromClause, parameter: str) -> str:
        return  "#>{" + val.database + "." + val.table + "}#"

    def visit_integer_literal(self, val: IntegerLiteral, parameter: str) -> str:
        return str(val.value())

    def visit_string_literal(self, val: StringLiteral, parameter: str) -> str:
        return "'" + val.value() + "'"

    def visit_boolean_literal(self, val: BooleanLiteral, parameter: str) -> str:
        return str(val.value())

    def visit_operand_expression(self, val: OperandExpression, parameter: str) -> str:
        return val.expression.visit(self, "")
    
    def visit_unary_expression(self, val: UnaryExpression, parameter: str) -> str:
        return val.operator.visit(self, "") + val.expression.visit(self, "")

    def visit_binary_expression(self, val: BinaryExpression, parameter: str) -> str:
        return val.left.visit(self, "") + val.operator.visit(self, "") + val.right.visit(self, "")
    
    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: str) -> str:
        return "!"

    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: str) -> str:
        return "=="

    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: str) -> str:
        return "!="

    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: str) -> str:
        return ">"

    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: str) -> str:
        return ">="

    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: str) -> str:
        return "<"

    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: str) -> str:
        return "<="

    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: str) -> str:
        return "and"

    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: str) -> str:
        return "or"

    def visit_add_binary_operator(self, val: AddBinaryOperator, parameter: str) -> str:
        return "+"

    def visit_multiply_binary_operator(self, val: MultiplyBinaryOperator, parameter: str) -> str:
        return "*"

    def visit_subtract_binary_operator(self, val: SubtractBinaryOperator, parameter: str) -> str:
        return "-"

    def visit_divide_binary_operator(self, val: DivideBinaryOperator, parameter: str) -> str:
        return "/"
    
    def visit_literal_expression(self, val: LiteralExpression, parameter: str) -> str:
        return val.literal.visit(self, "")

    def visit_alias_expression(self, val: AliasExpression, parameter: str) -> str:
        return "$" + val.alias

    def visit_selection_expression(self, val: SelectionExpression, parameter: str) -> str:
        return val.name

    def visit_reference_expression(self, val: ReferenceExpression, parameter: str) -> str:
        return "$" + val.alias + "." + val.ref

    def visit_function_expression(self, val: FunctionExpression, parameter: str) -> str:
        #TODO: AJH: this probably isn't right
        parameters = ", ".join(map(lambda expr: expr.visit(self, ""), val.parameters))
        function_string = val.function.visit(self, "")
        return parameters + function_string

    def visit_count_function(self, val: CountFunction, parameter: str) -> str:
        return "->count()"

    def visit_filter_clause(self, val: FilterClause, parameter: str) -> str:
        variables = self.extract_variables(val.expression)
        return "filter(" + ", ".join(variables) + " | " + val.expression.visit(self, "") + ")"

    def visit_selection_clause(self, val: SelectionClause, parameter: str) -> str:
        return "select(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_extend_clause(self, val: ExtendClause, parameter: str) -> str:
        return "extend(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_extend_expression(self, val: ExtendExpression, parameter: str) -> str:
        variables = self.extract_variables(val.expression)
        return val.alias + ":" + ", ".join(variables) + " | " + val.expression.visit(self, "")

    def visit_group_by_clause(self, val: GroupByClause, parameter: str) -> str:
        #->groupBy(~[departmentId], ~[count: x | $x.departmentId : d | $d->count(), count2: x | $x.departmentId : d | $d->count()])

        selections = "~[" + ", ".join(map(lambda selection: selection.visit(self, ""), val.selections)) + "]"
        expressions = "~[" + ", ".join(map(lambda expression: expression.visit(self, ""), val.expressions)) + "]"
        having = ", " + val.having.visit(self, "") if val.having else ""
        return "groupBy(" + selections + ", " + expressions + having + ")"

    def visit_group_by_expression(self, val: GroupByExpression, parameter: str) -> str:
        selection_vars = self.extract_variables(val.selection)
        reduction_vars = self.extract_variables(val.reduction)
        return val.alias + ": " + ", ".join(selection_vars) + " | " + val.selection.visit(self, "") + " : " + ", ".join(reduction_vars) + " | " + val.reduction.visit(self, "")

    def visit_distinct_clause(self, val: DistinctClause, parameter: str) -> str:
        return "distinct(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_limit_clause(self, val: LimitClause, parameter: str) -> str:
        return "limit(" + val.value.visit(self, "") + ")"

    def visit_join_expression(self, val: JoinExpression, parameter: str) -> str:
        on_vars = self.extract_variables(val.on)
        return "{" + ", ".join(on_vars) + " | " + val.on.visit(self, "") + "}"

    def visit_join_clause(self, val: JoinClause, parameter: str) -> str:
        return "join(" + val.from_clause.visit(self, "")  + ", " + val.join_type.visit(self, "") + ", " + val.on_clause.visit(self, "") + ")"

    def visit_inner_join_type(self, val: InnerJoinType, parameter: str) -> str:
        return "JoinKind.INNER"

    def visit_left_join_type(self, val: LeftJoinType, parameter: str) -> str:
        return "JoinKind.LEFT"

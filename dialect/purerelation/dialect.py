from abc import ABC
from dataclasses import dataclass

from model.metamodel import ExecutionVisitor, JoinClause, LimitClause, DistinctClause, GroupByClause, ExtendClause, \
    SelectionClause, FilterClause, FunctionExpression, ReferenceExpression, LiteralExpression, BinaryExpression, \
    UnaryExpression, OperandExpression, BooleanLiteral, StringLiteral, IntegerLiteral, Query, Runtime, Executable, \
    Results, OrBinaryOperator, AndBinaryOperator, LessThanEqualsBinaryOperator, LessThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, GreaterThanBinaryOperator, NotEqualsBinaryOperator, EqualsBinaryOperator, \
    NotUnaryOperator

@dataclass
class PureRuntime(Runtime, ABC):
    database: str
    table: str

    def executable_to_string(self, executable: Executable) -> str:
        visitor = PureRelationExpressionVisitor()
        return (visitor.visit_runtime(self, "") +
                executable.visit(visitor, self.visit(visitor, "")))

class NonExecutablePureRuntime(PureRuntime):
    def eval(self, executable: Executable) -> Results:
        raise NotImplementedError()

class PureRelationExpressionVisitor(ExecutionVisitor):

    def visit_runtime(self, val: PureRuntime, parameter: str) -> str:
        return "#>{" + val.database + "." + val.table + "}#"

    def visit_query(self, val: Query, parameter: str) -> str:
        for clause in val.clauses:
            parameter = "->" + clause.visit(self, parameter)
        return parameter

    def visit_integer_literal(self, val: IntegerLiteral, parameter: str) -> str:
        raise val.value()

    def visit_string_literal(self, val: StringLiteral, parameter: str) -> str:
        return val.value()

    def visit_boolean_literal(self, val: BooleanLiteral, parameter: str) -> str:
        return val.value()

    def visit_operand_expression(self, val: OperandExpression, parameter: str) -> str:
        return val.expression.visit(self, parameter)
    
    def visit_unary_expression(self, val: UnaryExpression, parameter: str) -> str:
        return val.operator.visit(self, parameter) + val.expression.visit(self, parameter)

    def visit_binary_expression(self, val: BinaryExpression, parameter: str) -> str:
        return val.left.visit(self, parameter) + val.operator.visit(self, parameter) + val.right.visit(self, parameter)
        # return val.left.visit(self, parameter) + val.right.visit(self, parameter)

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
        return "&"

    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: str) -> str:
        return "||"
    
    def visit_literal_expression(self, val: LiteralExpression, parameter: str) -> str:
        return val.literal.visit(self, parameter)

    def visit_reference_expression(self, val: ReferenceExpression, parameter: str) -> str:
        return val.alias

    def visit_function_expression(self, val: FunctionExpression, parameter: str) -> str:
        raise NotImplementedError()

    def visit_filter_clause(self, val: FilterClause, parameter: str) -> str:
        visit = val.expression.visit(self, "")
        # How to inject into reference visitor?
        return "filter(x | " + visit + ")"

    # TODO: Seems like Pure Relational does not allow renames in select?
    def visit_selection_clause(self, val: SelectionClause, parameter: str) -> str:
        return "select(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_extend_clause(self, val: ExtendClause, parameter: str) -> str:
        raise NotImplementedError()

    def visit_group_by_clause(self, val: GroupByClause, parameter: str) -> str:
        raise NotImplementedError()

    def visit_distinct_clause(self, val: DistinctClause, parameter: str) -> str:
        return "distinct()"

    def visit_limit_clause(self, val: LimitClause, parameter: str) -> str:
        return f"limit({val.value})"

    def visit_join_clause(self, val: JoinClause, parameter: str) -> str:
        raise NotImplementedError()

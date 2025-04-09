from abc import ABC
from dataclasses import dataclass
from typing import Set, List

from model.metamodel import ExecutionVisitor, JoinClause, LimitClause, DistinctClause, GroupByClause, ExtendClause, \
    SelectionClause, FilterClause, FunctionExpression, LiteralExpression, BinaryExpression, \
    UnaryExpression, OperandExpression, BooleanLiteral, StringLiteral, IntegerLiteral, Runtime, \
    OrBinaryOperator, AndBinaryOperator, LessThanEqualsBinaryOperator, LessThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, GreaterThanBinaryOperator, NotEqualsBinaryOperator, EqualsBinaryOperator, \
    NotUnaryOperator, InnerJoinType, LeftJoinType, ColumnAliasExpression, \
    CountFunction, JoinExpression, Clause, FromClause, AddBinaryOperator, \
    MultiplyBinaryOperator, SubtractBinaryOperator, DivideBinaryOperator, OffsetClause, RenameClause, \
    OrderByExpression, NotExpression, IfExpression, ColumnReferenceExpression, DateLiteral, GroupByExpression, \
    ComputedColumnAliasExpression, VariableAliasExpression, MapReduceExpression, LambdaExpression, AverageFunction, \
    AscendingOrderType, DescendingOrderType, OrderByClause


@dataclass
class PureRuntime(Runtime, ABC):
    name: str

    def executable_to_string(self, clauses: List[Clause]) -> str:
        visitor = PureRelationExpressionVisitor(self)
        return "->".join(map(lambda clause: clause.visit(visitor, ""), clauses)) + self.visit(visitor, "")

class NonExecutablePureRuntime(PureRuntime):
    def eval(self, clauses: List[Clause]) -> str:
        raise NotImplementedError()

@dataclass
class PureRelationExpressionVisitor(ExecutionVisitor):
    runtime: PureRuntime

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

    def visit_variable_alias_expression(self, val: VariableAliasExpression, parameter: str) -> str:
        return "$" + val.alias

    def visit_computed_column_alias_expression(self, val: ComputedColumnAliasExpression, parameter: str) -> str:
        return val.alias + ":" + val.expression.visit(self, "")

    def visit_column_alias_expression(self, val: ColumnAliasExpression, parameter: str) -> str:
        return "$" + val.alias + "." + val.reference.visit(self, "")

    def visit_function_expression(self, val: FunctionExpression, parameter: str) -> str:
        #TODO: AJH: this probably isn't right
        parameters = ", ".join(map(lambda expr: expr.visit(self, ""), val.parameters))
        function_string = val.function.visit(self, "")
        return parameters + function_string

    def visit_map_reduce_expression(self, val: MapReduceExpression, parameter: str) -> str:
        return val.map_expression.visit(self, "") + " : " + val.reduce_expression.visit(self, "")

    def visit_lambda_expression(self, val: LambdaExpression, parameter: str) -> str:
        return ", ".join(val.parameters) + " | " + val.expression.visit(self, "")

    def visit_count_function(self, val: CountFunction, parameter: str) -> str:
        return "->count()"

    def visit_average_function(self, val: AverageFunction, parameter: str) -> str:
        return "->avg()"

    def visit_filter_clause(self, val: FilterClause, parameter: str) -> str:
        return "filter(" + val.expression.visit(self, "") + ")"

    def visit_selection_clause(self, val: SelectionClause, parameter: str) -> str:
        return "select(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_extend_clause(self, val: ExtendClause, parameter: str) -> str:
        return "extend(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_group_by_clause(self, val: GroupByClause, parameter: str) -> str:
        return "groupBy(" + val.expression.visit(self, "") + ")"

    def visit_group_by_expression(self, val: GroupByExpression, parameter: str) -> str:
        selections = "~[" + ", ".join(map(lambda selection: selection.visit(self, ""), val.selections)) + "]"
        expressions = "~[" + ", ".join(map(lambda expression: expression.visit(self, ""), val.expressions)) + "]"
        having = ", " + val.having.visit(self, "") if val.having else ""
        return selections + ", " + expressions + having

    def visit_distinct_clause(self, val: DistinctClause, parameter: str) -> str:
        return "distinct(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_order_by_clause(self, val: OrderByClause, parameter: str) -> str:
        return "sort([" + ", ".join(map(lambda expr: expr.visit(self, ""), val.ordering)) + "])"

    def visit_limit_clause(self, val: LimitClause, parameter: str) -> str:
        return "limit(" + val.value.visit(self, "") + ")"

    def visit_join_expression(self, val: JoinExpression, parameter: str) -> str:
        return "{" + val.on.visit(self, "") + "}"

    def visit_join_clause(self, val: JoinClause, parameter: str) -> str:
        return "join(" + val.from_clause.visit(self, "")  + ", " + val.join_type.visit(self, "") + ", " + val.on_clause.visit(self, "") + ")"

    def visit_inner_join_type(self, val: InnerJoinType, parameter: str) -> str:
        return "JoinKind.INNER"

    def visit_left_join_type(self, val: LeftJoinType, parameter: str) -> str:
        return "JoinKind.LEFT"

    def visit_date_literal(self, val: DateLiteral, parameter: str) -> str:
        raise NotImplementedError()

    def visit_column_reference_expression(self, val: ColumnReferenceExpression, parameter: str) -> str:
        return val.name

    def visit_if_expression(self, val: IfExpression, parameter: str) -> str:
        raise NotImplementedError()

    def visit_not_expression(self, val: NotExpression, parameter: str) -> str:
        raise NotImplementedError()

    def visit_order_by_expression(self, val: OrderByExpression, parameter: str) -> str:
        return f"~{val.expression.visit(self, parameter)}->{val.direction.visit(self, parameter)}()"

    def visit_ascending_order_type(self, val: AscendingOrderType, parameter: str) -> str:
        return "ascending"

    def visit_descending_order_type(self, val: DescendingOrderType, parameter: str) -> str:
        return "descending"

    def visit_rename_clause(self, val: RenameClause, parameter: str) -> str:
        renames = []
        for columnAlias in val.columnAliases:
            renames.append(f"rename(~{columnAlias.reference.visit(self, parameter)}, ~{columnAlias.alias})")
        return "->".join(renames)

    def visit_offset_clause(self, val: OffsetClause, parameter: str) -> str:
        return f"drop({val.value.visit(self, parameter)})"

    def visit_in_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()

    def visit_not_in_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()

    def visit_is_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()

    def visit_is_not_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()

    def visit_modulo_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()

    def visit_exponent_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()

    def visit_bitwise_and_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()

    def visit_bitwise_or_binary_operator(self, self1, parameter: str) -> str:
        raise NotImplementedError()
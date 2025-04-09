"""
Lambda parser for the cloud-dataframe DSL.

This module provides utilities for parsing Python lambda functions
and converting them to SQL expressions.
"""
import ast
import importlib
import inspect
from _ast import operator
from enum import Enum
from typing import Callable, List, Union, Dict

from functions import StringConcatFunction
from model.metamodel import Expression, BinaryExpression, BinaryOperator, \
    ColumnReferenceExpression, BooleanLiteral, IfExpression, NotExpression, OrderByExpression, \
    FunctionExpression, \
    OperandExpression, AndBinaryOperator, OrBinaryOperator, IntegerLiteral, StringLiteral, EqualsBinaryOperator, \
    NotEqualsBinaryOperator, LessThanBinaryOperator, LessThanEqualsBinaryOperator, GreaterThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, InBinaryOperator, NotInBinaryOperator, IsBinaryOperator, IsNotBinaryOperator, \
    AddBinaryOperator, SubtractBinaryOperator, MultiplyBinaryOperator, DivideBinaryOperator, ModuloBinaryOperator, \
    ExponentBinaryOperator, BitwiseOrBinaryOperator, BitwiseAndBinaryOperator, DateLiteral, GroupByExpression, \
    DescendingOrderType, ComputedColumnAliasExpression
from dsl.schema import Schema

class ParseType(Enum):
    extend = "extend"
    join = "join"
    rename = "rename"
    select = "select"
    group_by = "group_by"
    filter = "filter"
    order_by = "order_by"
    dummy = "dummy"

class Parser:

    @staticmethod
    def parse(func: Callable, schema: Schema, ptype: ParseType) -> Union[Expression, List[Expression]]:
        """
        Parse a lambda function and convert it to an Expression or list of Expressions.

        Args:
            func: The lambda function to parse. Can be:
                - A lambda that returns a boolean expression (e.g., lambda x: x.age > 30)
                - A lambda that returns a column reference (e.g., lambda x: x.name)
                - A lambda that returns an array of column references (e.g., lambda x: [x.name, x.age])
                - A lambda that returns tuples with sort direction (e.g., lambda x: [(x.department, Sort.DESC)])
            schema: the current LegendQL query context
            ptype: The LegendQL function calling this parser (extend, join, select ..)

        Returns:
            An Expression or list of Expressions representing the lambda function
        """
        # Get the source code of the lambda function
        lambda_node = Parser._get_lambda_node(func)
        lambda_args = lambda_node.args.args

        if len(lambda_args) != 1:
            raise ValueError(f"Lambda MUST exactly 1 argument: {lambda_args}")

        alias = {lambda_args[0].arg: schema}

        # Parse the lambda body
        result = Parser._parse_expression(lambda_node.body, alias, ptype)

        # [lambda_body, lambda_args] = _get_lambda_source(func)
        # alias = {lambda_args[0].arg: query.name}
        # result = Parser._parse_expression(lambda_body, alias)

        return result

    @staticmethod
    def parse_join(func: Callable, schema: Schema, join: Schema) -> Union[Expression, List[Expression]]:
        """
        Specifically parse the JOIN lambda function and convert it to an Expression or list of Expressions.

        Args:
            func: The lambda function to parse. Must be a 2 argument lambda:
                - A lambda that returns a boolean expression (e.g., lambda x, y: x.col1 == y.col2)
            schema: the current LegendQL query context
            join: the LegendQL query context we want to join to

        Returns:
            An Expression representing the lambda function
        """
        # Get the source code of the lambda function
        lambda_node = Parser._get_lambda_node(func)
        lambda_args = lambda_node.args.args

        if len(lambda_args) != 2:
            raise ValueError(f"Join Lambda MUST have 2 arguments: {lambda_args}")

        # map the lambda args to the table names for the query
        alias = {lambda_args[0].arg: schema, lambda_args[1].arg: join}

        # Parse the lambda body
        result = Parser._parse_expression(lambda_node.body, alias, ParseType.join)

        return result

    @staticmethod
    def _get_lambda_node(func):
        source_lines, _ = inspect.getsourcelines(func)
        source_text = ''.join(source_lines).strip().replace("\n", "")

        try:
            # if it is lambda on own line this should work
            source_ast = ast.parse(source_text)
        except:
            # fluent api way
            idx = source_text.find("lambda")
            source_text = source_text[idx:len(source_text) - 1]

            try:
                # fluent api way
                source_ast = ast.parse(source_text)
            except:
                # is it on the last line? try to strip out one more
                source_text = source_text[:len(source_text) - 1]

                try:
                    source_ast = ast.parse(source_text)
                except:
                    raise ValueError(f"Could not get Lambda func: {source_text}")

        lambda_node = next((node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)), None)

        return lambda_node

    @staticmethod
    def _parse_expression(node: ast.AST, alias: Dict[str, Schema], ptype: ParseType) -> Union[Expression, List[Expression]]:
        """
        Parse an AST node and convert it to an Expression or list of Expressions.

        Args:
            node: The AST node to parse
            alias: lambda args mapped to table aliases

        Returns:
            An Expression or list of Expressions representing the AST node,
            or list containing tuples of (Expression, sort_direction) for order_by clauses
            :param ptype:
        """
        if node is None:
            raise ValueError("node in Parser._parse_expression is None")

        if isinstance(node, ast.NamedExpr):
            # column rename or new column using := (walrus) operator
            if isinstance(node.target, ast.Name):
                target_name = node.target.id
            else:
                raise ValueError(f"Rename column must be valid column name: {node.target}")

            expr = Parser._parse_expression(node.value, alias, ptype)

            if ptype == ParseType.extend:
                # we know extend function only has one lambda arg
                _, schema = next(iter(alias.items()))
                #  we know that extend always adds a column so add it into schema here
                schema.columns[target_name] = None

            if ptype == ParseType.group_by:
                # we know group_by function only has one lambda arg
                _, schema = next(iter(alias.items()))
                #  we know that extend always adds a column so add it into schema here
                schema.columns[target_name] = None
                ptype = ParseType.dummy

            if ptype == ParseType.join or ptype == ParseType.rename:
                # only support rename in joins
                if isinstance(node.value, ast.Attribute) and isinstance(node.value.value, ast.Name):
                    schema = alias[node.value.value.id]
                    # add new rename column preserving order
                    schema.columns = {node.target.id if k == node.value.attr else k : v for k, v in schema.columns.items()}
                else:
                    raise ValueError(f"You can only rename columns in rename() and join() no expressions allowed: {node.value}")

            if isinstance(expr, Expression):
                return ComputedColumnAliasExpression(alias=target_name, expression=expr)
            else:
                raise ValueError(f"Unknown expression: {node.value}, {expr}")

        if isinstance(node, ast.Compare):
            # Handle comparison operations (e.g., x > 5, y == 'value')
            left = Parser._parse_expression(node.left, alias, ptype)

            # We only handle the first comparator for simplicity
            # In a real implementation, we would handle multiple comparators
            op = node.ops[0]
            right = Parser._parse_expression(node.comparators[0], alias, ptype)

            comp_op = Parser._get_comparison_operator(op)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported Compare object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported Compare object {right}")

            return BinaryExpression(left=OperandExpression(left), operator=comp_op, right=OperandExpression(right))

        elif isinstance(node, ast.BinOp):
            # Handle binary operations (e.g., x + y, x - y, x * y)
            left = Parser._parse_expression(node.left, alias, ptype)
            right = Parser._parse_expression(node.right, alias, ptype)

            comp_op = Parser._get_binary_operator(node.op)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported BinOp object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported BinOp object {right}")

            return BinaryExpression(left=OperandExpression(left), operator=comp_op, right=OperandExpression(right))

        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations (e.g., x and y, x or y)
            values = [Parser._parse_expression(val, alias, ptype) for val in node.values]

            # Combine the values with the appropriate operator
            comp_op = AndBinaryOperator() if isinstance(node.op, ast.And) else OrBinaryOperator()

            # Ensure all values are Expression objects, not lists or tuples
            processed_values = []
            for val in values:
                if isinstance(val, list) or isinstance(val, tuple):
                    raise ValueError(f"Unsupported BoolOp object {val}")
                else:
                    processed_values.append(val)

            # Start with the first two values
            result = BinaryExpression(left=OperandExpression(processed_values[0]), operator=comp_op, right=OperandExpression(processed_values[1]))

            # Add the remaining values
            for value in processed_values[2:]:
                result = BinaryExpression(left=OperandExpression(result), operator=comp_op, right=OperandExpression(value))

            return result

        elif isinstance(node, ast.Attribute):
            # Handle column references (e.g. x.column_name)
            if isinstance(node.value, ast.Name):
                # validate the column name
                schema = alias[node.value.id]

                if ptype == ParseType.select or ptype == ParseType.group_by:
                    schema.columns[node.attr] = None

                if not schema.validate_column(node.attr):
                    raise ValueError(f"Column '{node.attr}' not found in table schema '{schema}'")

                return ColumnReferenceExpression(name=node.attr)
            else:
                raise ValueError(f"Unsupported Column Reference {node.value}")

        elif isinstance(node, ast.Constant):
            # Handle literal values (e.g., 5, 'value', True)
            from model.metamodel import LiteralExpression
            if isinstance(node.value, int):
                return LiteralExpression(IntegerLiteral(node.value))
            if isinstance(node.value, bool):
                return LiteralExpression(BooleanLiteral(node.value))
            if isinstance(node.value, str):
                return LiteralExpression(StringLiteral(node.value))

            raise ValueError(f"Cannot convert literal type {type(node.value)}")

        elif isinstance(node, ast.Name):
            # Handle variable names (e.g., x, y)
            if alias.get(node.id):
                # This is the lambda parameter itself
                raise ValueError(f"Cannot reference the lambda parameter by itself {node.id}")
            elif node.id == "True":
                from model.metamodel import LiteralExpression
                return LiteralExpression(BooleanLiteral(True))
            elif node.id == "False":
                from model.metamodel import LiteralExpression
                return LiteralExpression(BooleanLiteral(False))
            else:
                # This is a variable reference
                return ColumnReferenceExpression(name=node.id)

        elif isinstance(node, ast.UnaryOp):
            # Handle unary operations (e.g., not x)
            operand = Parser._parse_expression(node.operand, alias, ptype)

            # Ensure operand is an Expression object, not a list or tuple
            if isinstance(operand, list) or isinstance(operand, tuple):
                # Use a fallback for list/tuple values in unary operations
                raise ValueError(f"Unsupported expression to UnaryOp: {operand}")

            if isinstance(node.op, ast.Not):
                return NotExpression(operand)
            elif isinstance(node.op, ast.USub):
                if isinstance(node.operand, ast.Constant):
                    return operand
                else:
                    #sort
                    return OrderByExpression(direction=DescendingOrderType(), expression=operand)
            else:
                # Other unary operations (e.g., +, -)
                # In a real implementation, we would handle this more robustly
                return operand

        elif isinstance(node, ast.IfExp):
            # Handle conditional expressions (e.g., x if y else z)
            # In a real implementation, we would handle this more robustly
            test = Parser._parse_expression(node.test, alias, ptype)
            body = Parser._parse_expression(node.body, alias, ptype)
            orelse = Parser._parse_expression(node.orelse, alias, ptype)

            # Ensure all values are Expression objects, not lists or tuples
            if isinstance(test, list) or isinstance(test, tuple):
                raise ValueError(f"Unsupported IfExp: {test}")
            if isinstance(body, list) or isinstance(body, tuple):
                raise ValueError(f"Unsupported IfExp: {body}")
            if isinstance(orelse, list) or isinstance(orelse, tuple):
                raise ValueError(f"Unsupported IfExp: {orelse}")

            # Create a CASE WHEN expression
            return IfExpression(test=test, body=body, orelse=orelse)

        elif isinstance(node, ast.List):
            # Handle tuples and lists (e.g., (1, 2, 3), [1, 2, 3])
            # This is used for array returns in lambdas like lambda x: [x.name, x.age]
            elements = []
            for elt in node.elts:
                elements.append(Parser._parse_expression(elt, alias, ptype))
            return elements

        elif isinstance(node, ast.Tuple):
            # Handle Join with rename ( x.col1 == y.col1, [ (x_col1 := x.col1 ), (y_col1 := y.col1 ) ]
            elements = []
            for elt in node.elts:
                elements.append(Parser._parse_expression(elt, alias, ptype))
            return elements

        elif isinstance(node, ast.Call):
            # Handle function calls (e.g., sum(x.col1 - x.col2))

            args_list = []
            # kwargs = {}

            if isinstance(node.func, ast.Name):
                # Parse the arguments to the function

                for arg in node.args:
                    parsed_arg = Parser._parse_expression(arg, alias, ptype)
                    args_list.append(parsed_arg)

                # Handle keyword arguments
                # kwargs = {}
                for kw in node.keywords:
                    parsed_kw = Parser._parse_expression(kw.value, alias, ptype)
                    # kwargs[kw.arg] = parsed_kw
                    args_list.append(parsed_kw)

            else:
                ValueError(f"Unsupported function type: {node.func}")

            if node.func.id == "date":
                from model.metamodel import LiteralExpression
                compiled = compile(ast.fix_missing_locations(ast.Expression(body=node)), '', 'eval')
                val = eval(compiled, None, None)
                return LiteralExpression(literal=DateLiteral(val))

            #if node.func.id not in known_functions:
            #    ValueError(f"Unknown function name: {node.func.id}")

            # very brittle, lots more checks needed here
            module = importlib.import_module("functions")
            class_ = getattr(module, f"{node.func.id.title()}Function")
            instance = class_()

            if node.func.id == "aggregate":
                selections = args_list[0]
                expressions = args_list[1]
                having = args_list[2] if len(args_list) == 3 else None
                return GroupByExpression(selections=selections, expressions=expressions, having=having)

            return FunctionExpression(instance, parameters=args_list)

        elif isinstance(node, ast.JoinedStr):
            # Handle fstring (e.g. f"hello{blah}")
            # In a real implementation, we would handle this more robustly
            expr = []
            for value in node.values:
                if isinstance(value, ast.Constant):
                    expr.append(Parser._parse_expression(value, alias, ptype))
                elif isinstance(value, ast.FormattedValue):
                    if value.format_spec is not None:
                        raise ValueError(f"Format Spec Not Supported: {value.format_spec}")
                    else:
                        expr.append(Parser._parse_expression(value.value, alias, ptype))

            return FunctionExpression(StringConcatFunction(), expr)

        elif isinstance(node, ast.Subscript):
            # Handle subscript operations (e.g., x[0], x['key'])
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Dict):
            # Handle dictionaries (e.g., {'a': 1, 'b': 2})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Set):
            # Handle sets (e.g., {1, 2, 3})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.ListComp) or isinstance(node, ast.SetComp) or isinstance(node, ast.DictComp) or isinstance(node, ast.GeneratorExp):
            # Handle comprehensions (e.g., [x for x in y], {x: y for x in z})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        else:
            # Throw
            raise ValueError(f"Unsupported expression: {node}")

    @staticmethod
    def _get_comparison_operator(op: ast.cmpop) -> BinaryOperator:
        """
        Convert an AST comparison operator to a SQL operator.

        Args:
            op: The AST comparison operator

        Returns:
            The equivalent SQL operator
        """
        if isinstance(op, ast.Eq):
            return EqualsBinaryOperator()
        elif isinstance(op, ast.NotEq):
            return NotEqualsBinaryOperator()
        elif isinstance(op, ast.Lt):
            return LessThanBinaryOperator()
        elif isinstance(op, ast.LtE):
            return LessThanEqualsBinaryOperator()
        elif isinstance(op, ast.Gt):
            return GreaterThanBinaryOperator()
        elif isinstance(op, ast.GtE):
            return GreaterThanEqualsBinaryOperator()
        elif isinstance(op, ast.In):
            return InBinaryOperator()
        elif isinstance(op, ast.NotIn):
            return NotInBinaryOperator()
        elif isinstance(op, ast.Is):
            return IsBinaryOperator()
        elif isinstance(op, ast.IsNot):
            return IsNotBinaryOperator()
        else:
            raise ValueError(f"Unsupported comparison operator {op}")

    @staticmethod
    def _get_binary_operator(op: operator) -> BinaryOperator:
        # Map Python operators to Binary operators
        if isinstance(op, ast.Add):
            return AddBinaryOperator()
        elif isinstance(op, ast.Sub):
            return SubtractBinaryOperator()
        elif isinstance(op, ast.Mult):
            return MultiplyBinaryOperator()
        elif isinstance(op, ast.Div):
            return DivideBinaryOperator()
        elif isinstance(op, ast.Mod):
            return ModuloBinaryOperator()
        elif isinstance(op, ast.Pow):
            return ExponentBinaryOperator()
        elif isinstance(op, ast.BitOr):
            return BitwiseOrBinaryOperator()
        elif isinstance(op, ast.BitAnd):
            return BitwiseAndBinaryOperator()
        else:
            raise ValueError(f"Unsupported binary operator {op}")

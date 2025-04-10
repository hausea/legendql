from dataclasses import dataclass

from model.metamodel import Function, ExecutionVisitor


@dataclass
class AggregationFunction(Function):
    # sum(), min(), max(), count(), avg()
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class ScalarFunction(Function):
    # date_diff(), left(), abs() ..
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class WindowFunction(Function):
    # rank(), row_number(), first(), last() ..
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RankFunction(WindowFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RowNumberFunction(WindowFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class LeadFunction(WindowFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class LagFunction(WindowFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class LeftFunction(ScalarFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class StringConcatFunction(ScalarFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class AvgFunction(AggregationFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class CountFunction(AggregationFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class SumFunction(AggregationFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class OverFunction(ScalarFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RowsFunction(ScalarFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RangeFunction(ScalarFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class UnboundedFunction(ScalarFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class AggregateFunction(ScalarFunction):
    def visit[P, T](self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

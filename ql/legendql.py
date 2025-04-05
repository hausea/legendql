from model.metamodel import Query, SelectionClause, Runtime, DataFrame, FilterClause


class LegendQL:
    query: Query = Query()

    @classmethod
    def create(cls):
        return LegendQL()

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self.query)

    def eval[R: Runtime](self, runtime: R) -> DataFrame:
        return self.bind(runtime).eval()

    def select(self, select: SelectionClause):
        self.query.clauses.append(select)
        return self

    def filter(self, filter: FilterClause):
        self.query.clauses.append(filter)
        return self

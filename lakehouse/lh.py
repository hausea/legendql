from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, List, Tuple, Union, Type

from lakehouse.extract import Legend, IngestSource, Polars, CSV, Pandas
from model.schema import Table
from legendql.ql import LegendQL


class MilestoneType(Enum):
    batch_milestone = 'batch_milestone'
    append_only = "append_only"
    overwrite = "overwrite"

class SnapshotType(Enum):
    incremental = "incremental"
    full = "full"
    none = "none"

@dataclass
class Versioning:
    type: Tuple[MilestoneType, SnapshotType]

@dataclass
class IngestTrigger:
    pass

@dataclass
class Ingest(Dict):
    datasets: Dict[str, Dataset]

    def __getattr__(self, key) -> Table:
        try:
            return self.datasets[key].get_table_definition()
        except KeyError as e:
            raise AttributeError(f"'IngestConfig' object has no Dataset '{key}'") from e

@dataclass
class MaterializedView(Dict):
    views: Dict[str, View]

    def __getattr__(self, key) -> Table:
        try:
            return self.views[key].source.query.get_table_definition()
        except KeyError as e:
            raise AttributeError(f"'MaterializedView' object has no View '{key}'") from e

@dataclass
class Scheduled(IngestTrigger):
    pass

@dataclass
class Dependency(IngestTrigger):
    dependencies: List[Ingest]

@dataclass
class AnyDependency(Dependency):
    pass

@dataclass
class AllDependency(Dependency):
    pass


@dataclass
class Queryable(Dict):
    def get_table_definition(self) -> Table:
        pass

@dataclass
class AccessPoint(Queryable):
    query: LegendQL
    def get_table_definition(self) -> Table:
        return self.query.get_table_definition()

@dataclass
class DataProduct(Dict):
    name: str
    access_points: Dict[str, AccessPoint]

    def __getattr__(self, key) -> Table:
        try:
            return self.access_points[key].get_table_definition()
        except KeyError as e:
            raise AttributeError(f"'DataProduct' object has no AccessPoint '{key}'") from e

@dataclass
class Environment:
    pass

@dataclass
class Development(Environment):
    pass

@dataclass
class Production(Environment):
    pass

@dataclass
class Tenant:
    name: str
    environment: Environment

@dataclass
class Classification:
    level: str

@dataclass
class TableOptions:
    pass

@dataclass
class Dataset(Queryable):
    schema: str
    table: str
    primary_key: Union[str, List[str]]
    versioning: Versioning
    source: IngestSource
    columns: Optional[Dict[str, Optional[Type]]] = None
    classification: Optional[Classification] = None
    options: Optional[TableOptions] = None
    trigger: Optional[IngestTrigger] = None

    def get_table_definition(self) -> Table:
        if self.columns is not None:
            return Table(self.table, self.columns)
        else:
            return Table(self.table, self.source.auto_infer_columns())

@dataclass
class View(Dataset):
    source: Legend
    def get_table_definition(self) -> Table:
        return self.source.query.get_table_definition()

dev = Development()
prod = Production()

class Lakehouse(Dict):
    external_public = Classification("DP00")
    enterprise = Classification("DP10")
    producer_entitled = Classification("DP20")
    high_risk = Classification("DP30")
    producer_only = Classification("")

    batch_milestone_full = Versioning((MilestoneType.batch_milestone, SnapshotType.incremental))
    batch_milestone_incremental = Versioning((MilestoneType.batch_milestone, SnapshotType.full))
    append_only = Versioning((MilestoneType.append_only, SnapshotType.incremental))
    overwrite = Versioning((MilestoneType.overwrite, SnapshotType.none))

    pandas = Pandas
    polars = Polars
    csv = CSV

    any = AnyDependency
    all = AllDependency

    def __init__(self, tenant: Tenant, deployment_id: int, schema: str = None):
        super().__init__()
        self.tenant = tenant
        self.deployment_id = deployment_id
        self.schema = schema
        self.configuration: Dict[str, Queryable] = {}
        self.data_products: Dict[str, DataProduct] = {}
        # CALL PRODUCER SETUP SCRIPT HERE!!

    @classmethod
    def query(cls, queryable: Queryable) -> LegendQL:
        return LegendQL.from_lh(queryable.get_table_definition())

    def config(self, config: Dict[str, Queryable]) -> Lakehouse:
        #self.configuration.update(config)
        return self

    def dataset(
            self,
            table: str,
            primary_key: Union[str, List[str]],
            versioning: Versioning,
            source: IngestSource,
            schema: str = None,
            columns: Dict[str, Optional[Type]] = None,
            classification: Classification = None,
            options: TableOptions = None,
            trigger: IngestTrigger = None) -> Dataset:

        if schema is None:
            schema = self.schema

        dataset = Dataset(schema, table, primary_key, versioning, source, columns, classification, options, trigger)
        self.configuration[table] = dataset
        return dataset

    def run_ingest(self, dataset: Dataset):
        # first, upload the ingest spec to the lakehouse
        # self._upload_ingest_spec(ingest)
        # then call first ingest API to get s3 location for upload
        # location = self._get_staging_location()
        # then actually do the extract and push to location
        # self._extract_and_stage_data(location)
        # finally, do the actual ingest
        # self._ingest_data()
        pass

    def view(self,
        table: str,
        primary_key: Union[str, List[str]],
        versioning: Versioning,
        source: LegendQL,
        schema: str = None,
        columns: Dict[str, Optional[Type]] = None,
        classification: Optional[Classification] = None,
        options: TableOptions = None, trigger: IngestTrigger = None) -> View:

        if schema is None:
            schema = self.schema

        view = View(schema, table, primary_key, versioning, Legend(source), columns, classification, options, trigger)
        self.configuration[table] = view
        return view

    def run_matview(self, view: View):
        # first, upload the ingest spec to the lakehouse
        # self._upload_ingest_spec(ingest)
        # run the materialization
        # self._run_materialization()
        pass

    def data_product(self, name: str, display_name: str,
                     access_points: Dict[str, LegendQL]) -> DataProduct:
        dp = DataProduct(name, {name: AccessPoint(query) for (name, query) in access_points.items()})
        self.data_products[name] = dp

        for ap_name, query in access_points.items():
            self.configuration[ap_name] = AccessPoint(query)

        return dp

    def publish_data_product(self, dp: DataProduct):
        # dp = self.configuration[name]
        # CALL DATA PRODUCT PUBLISH/DEPLOY API
        pass

    def discover_data_products(self) -> Dict[str, DataProduct]:
        return self.data_products

    def import_(self, dp: Dict[str, DataProduct], data_product: str, access_point: str, import_alias: str = None):
        if import_alias is  None:
            import_alias = access_point
        self.configuration[import_alias] = dp[data_product].access_points[access_point]

    def __getattr__(self, key: str) -> Union[Queryable, DataProduct]:
        try:
            return self.configuration[key]
        except KeyError as e:
            try:
                return self.data_products[key]
            except KeyError as e:
                raise AttributeError(f"'Lakehouse' has no '{key}'") from e

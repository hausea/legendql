from typing import Dict, Optional, Type

from legendql.ql import LegendQL
from model.schema import Database, Table


def table(db_name: str, table_name: str, columns: Dict[str, Optional[Type]]) -> LegendQL:
    db_table = Table(table_name, columns)
    database = Database(db_name, [db_table])
    return LegendQL.from_table(database, db_table)

def db(db_name: str, tables: Dict[str, Dict[str, Optional[Type]]]) -> [LegendQL]:
    db_tables = [Table(table_name, columns) for (table_name, columns) in tables.items()]
    database = Database(db_name, db_tables)
    return [LegendQL.from_table(database, db_table) for db_table in db_tables]

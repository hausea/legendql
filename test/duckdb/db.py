import os
import tempfile
from datetime import datetime, date

import duckdb
from duckdb import DuckDBPyRelation

from model.schema import Table


class DuckDB:
    database_path: str

    def __init__(self, path: str = None) -> None:
        self.database_path = path

    def get_db_path(self) -> str:
        return str(self.database_path)

    def start(self):
        con = duckdb.connect(self.database_path)
        con.close()

    def stop(self):
        pass

    def exec_sql(self, sql: str) -> DuckDBPyRelation:
        con = duckdb.connect(self.database_path)
        return con.sql(sql)

    def load_csv(self, table: Table , csv_path: str):
        self.exec_sql(f"INSERT INTO {table.table} SELECT * FROM read_csv('{csv_path}');")

    def create_table(self, table: Table):
        columns = []
        for (col, typ) in table.columns.items():
            columns.append(f"{col} {self._to_column_type(typ)}")
        create = f"CREATE TABLE {table.table} ({', '.join(columns)});"
        self.exec_sql(create)

    def drop_table(self, table: Table):
        self.exec_sql(f"DROP TABLE {table.table};")

    def _to_column_type(self, typ):
        if typ == str:
            return "VARCHAR(0)"
        if typ == int:
            return "BIGINT"
        if typ == date:
            return "DATE"
        raise TypeError(f"Unkonwn column type {typ}")

class TestDuckDB:
    database_path: tempfile.NamedTemporaryFile
    db: DuckDB

    def start(self):
        file = tempfile.NamedTemporaryFile(mode="w+", delete_on_close=True)
        self.database_path = file.name
        file.close()
        self.db = DuckDB(self.database_path)
        self.db.start()

    def stop(self):
        os.remove(self.database_path)
        self.db.stop()

def main():
    duckdb = DuckDB("/Users/ahauser/Downloads/duckdb")
    duckdb.start()
    table = Table("employees", {"id": int, "departmentId": int, "first": str, "last": str})
    print(duckdb.create_table(table))
    duckdb.load_csv(table, "../data/employees.csv")
    print(duckdb.drop_table(table))
    duckdb.stop()

if __name__ == "__main__":
    main()

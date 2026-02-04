import duckdb
import os
import pandas as pd

class Lakehouse:
    def __init__(self, base_path="lakehouse", db_path="lakehouse.db"):
        self.base_path = base_path
        self.db_path = db_path
        self.con = duckdb.connect(db_path)

        # Ensure folder structure exists
        os.makedirs(f"{base_path}/bronze", exist_ok=True)
        os.makedirs(f"{base_path}/silver", exist_ok=True)
        os.makedirs(f"{base_path}/gold", exist_ok=True)

    def write_bronze(self, name, df):
        path = f"{self.base_path}/bronze/{name}.parquet"
        df.to_parquet(path)
        return path

    def write_silver(self, name, query):
        df = self.con.execute(query).df()
        path = f"{self.base_path}/silver/{name}.parquet"
        df.to_parquet(path)
        return path

    def write_gold(self, name, query):
        df = self.con.execute(query).df()
        path = f"{self.base_path}/gold/{name}.parquet"
        df.to_parquet(path)
        return path

    def query(self, sql):
        return self.con.execute(sql).df()
import duckdb
import os

DB_PATH = "lakehouse/duckdb/mobility.duckdb"

def build_silver(polygon, start_date, end_date):
    con = duckdb.connect(DB_PATH)

    print("[SILVER] Building MITMA silver layer")

    con.execute("""
    CREATE OR REPLACE TABLE silver_mitma AS
    SELECT *
    FROM read_csv_auto(
        'lakehouse/bronze/mitma/*_datos_agregados.csv',
        union_by_name=true
    )
    """)

    con.close()
    print("[SILVER] MITMA OK")

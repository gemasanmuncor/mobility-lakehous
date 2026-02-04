import duckdb

con = duckdb.connect("lakehouse/duckdb/mobility.duckdb")

con.execute("""
CREATE OR REPLACE TABLE silver_od AS
SELECT *
FROM read_csv_auto('s3://mobility-lakehouse-bronze/mitma/*.csv')
WHERE trips > 0
""")

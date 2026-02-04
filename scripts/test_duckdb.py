import duckdb

con = duckdb.connect("mobility.duckdb")
print(con.execute("SELECT 'DuckDB OK' AS status").fetchdf())

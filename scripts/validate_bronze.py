import duckdb
import os

def main():
    print("[VALIDATE] Bronze MITMA files:")
    print(os.listdir("lakehouse/bronze/mitma"))

    print("[VALIDATE] Bronze INE files:")
    print(os.listdir("lakehouse/bronze/ine"))

    con = duckdb.connect("lakehouse/duckdb/mobility.duckdb")

    print("[VALIDATE] Tablas en DuckDB:")
    print(con.execute("SHOW TABLES").fetchall())

    print("[VALIDATE] Registros silver_mitma:")
    print(con.execute("SELECT COUNT(*) FROM silver_mitma").fetchone())

    print("[VALIDATE] Registros silver_ine_population:")
    print(con.execute("SELECT COUNT(*) FROM silver_ine_population").fetchone())

    con.close()
    print("[VALIDATE] OK")

if __name__ == "__main__":
    main()

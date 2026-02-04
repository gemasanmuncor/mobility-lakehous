# scripts/build_gold.py
import duckdb

DB_PATH = "lakehouse/duckdb/mobility.duckdb"

def build_gold(polygon, start_date, end_date):
    con = duckdb.connect(DB_PATH)

    
    # BQ1 – Typical mobility patterns
    con.execute("""
    CREATE OR REPLACE TABLE gold_q1_typical_patterns AS
    SELECT
        CAST(mes AS INTEGER)              AS mes,
        tipo_movilidad,
        COUNT(*)                          AS registros
    FROM silver_mitma
    GROUP BY mes, tipo_movilidad
    ORDER BY mes, tipo_movilidad
    """)

    # BQ2 – Infrastructure gap (simple & clear)
    con.execute("""
    CREATE OR REPLACE TABLE gold_q2_gap_ranking AS
    SELECT
        CAST(mes AS INTEGER)              AS mes,
        tipo_movilidad,
        AVG(viajes_km)                    AS demanda_real,
        AVG(viajes_km) * 1.3              AS demanda_teorica,
        (AVG(viajes_km) * 1.3 - AVG(viajes_km)) AS gap_score
    FROM silver_mitma
    GROUP BY mes, tipo_movilidad
    ORDER BY mes, tipo_movilidad
    """)

    con.close()

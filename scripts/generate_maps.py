import duckdb
import os
from datetime import datetime
from maps.bq1_map import build_bq1_map
from maps.bq2_map import build_bq2_map

OUTPUT_DIR = "output/maps"
DB_PATH = "lakehouse/duckdb/mobility.duckdb"

def _month_from_date(date_str):
    return int(date_str.split("-")[1])

def generate_maps(polygon, start_date, end_date):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    start_month = _month_from_date(start_date)
    end_month = _month_from_date(end_date)

    con = duckdb.connect(DB_PATH)

    # ---------- BQ1 ----------
    df_bq1 = con.execute(f"""
        SELECT mes, tipo_movilidad, registros
        FROM gold_q1_typical_patterns
        WHERE mes BETWEEN {start_month} AND {end_month}
        ORDER BY mes, tipo_movilidad
    """).df()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bq1_path = f"{OUTPUT_DIR}/bq1_map_{ts}.html"

    build_bq1_map(df_bq1, polygon, bq1_path)

    # ---------- BQ2 ----------
    df_bq2 = con.execute(f"""
        SELECT mes, tipo_movilidad, gap_score
        FROM gold_q2_gap_ranking
        WHERE mes BETWEEN {start_month} AND {end_month}
        ORDER BY mes, tipo_movilidad
    """).df()

    bq2_path = f"{OUTPUT_DIR}/bq2_map_{ts}.html"

    build_bq2_map(df_bq2, polygon, bq2_path)

    con.close()

    return [bq1_path, bq2_path]

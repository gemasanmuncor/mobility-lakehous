import duckdb
import os

OUTPUT_DIR = "lakehouse/output"

def generate_outputs(polygon, start_date, end_date):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    con = duckdb.connect("lakehouse/duckdb/mobility.duckdb")

    # --- Simple summary for report ---
    q1 = con.execute("""
        SELECT *
        FROM gold_q1_typical_patterns
        LIMIT 10
    """).df()

    q2 = con.execute("""
        SELECT *
        FROM gold_q2_gap_ranking
        LIMIT 10
    """).df()

    # Guardamos CSV simples (no rompe nada)
    q1_path = f"{OUTPUT_DIR}/bq1_summary.csv"
    q2_path = f"{OUTPUT_DIR}/bq2_summary.csv"

    q1.to_csv(q1_path, index=False)
    q2.to_csv(q2_path, index=False)

    con.close()

    print(f"[OUTPUT] Generated {q1_path} and {q2_path}")

    return [q1_path, q2_path]

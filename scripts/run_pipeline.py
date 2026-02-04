from scripts.ingest_ine import ingest_ine
from scripts.ingest_mitma import ingest_mitma
from scripts.build_silver import build_silver
from scripts.build_gold import build_gold
from scripts.generate_maps import generate_maps
from scripts.generate_outputs import generate_outputs


def run_pipeline(polygon: str, start_date: str, end_date: str):
    print("Running mobility pipeline")
    print("Polygon:", polygon)
    print("Start date:", start_date)
    print("End date:", end_date)

    ingest_ine(polygon, start_date, end_date)
    ingest_mitma(polygon, start_date, end_date)
    build_silver(polygon, start_date, end_date)
    build_gold(polygon, start_date, end_date)
    generate_maps(polygon, start_date, end_date)
    generate_outputs(polygon, start_date, end_date)


    print("✅ Pipeline finished successfully")

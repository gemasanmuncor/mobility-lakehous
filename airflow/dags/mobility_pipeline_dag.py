from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging


def run_pipeline_task(**context):
    """
    Task wrapper para Airflow.
    Asi leo  parámetros del Trigger DAG  y llama al pipeline real.
    """

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    polygon = conf.get("polygon")
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")

    logging.info("Running mobility pipeline")
    logging.info(f"Polygon: {polygon}")
    logging.info(f"Start date: {start_date}")
    logging.info(f"End date: {end_date}")

    if not polygon:
        raise ValueError("polygon is required in DAG Run config")

    # Import REAL del pipeline (ya probado en consola)
    from scripts.run_pipeline import run_pipeline

    run_pipeline(
        polygon=polygon,
        start_date=start_date,
        end_date=end_date,
    )


with DAG(
    dag_id="mobility_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # solo manual
    catchup=False,
    tags=["mobility", "lakehouse"],
) as dag:

    run_full_pipeline = PythonOperator(
        task_id="run_full_pipeline",
        python_callable=run_pipeline_task,
    )

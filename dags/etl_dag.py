import datetime from datetime
import airflow from DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
    "etl_dag",
    description="ETL DAG",
    schedule_interval='* /3 * * * *',
    start_date=datetime(2024, 6, 27),
    catchup=False,
    default_view="graph",
    tags=["brach", "tag", "pipeline"],
) as dag:
    def extract():
        pass
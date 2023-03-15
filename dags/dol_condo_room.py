from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pprint import pprint

import pytz

tzInfo = pytz.timezone('Asia/Bangkok')
source_path = "/opt/airflow/dags/source/moc_api"
output_path = "/opt/airflow/dags/output/moc_api"
ingest_date = datetime.now(tz=tzInfo)

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('DOL_CONDO_ROOM',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def ingestion():
    pprint("Ingestion...")

def fetch_metadata():
    pprint("Fetch metadata...")

def load_data_toDB():
    pprint("Loading Data...")

def stamp_logging():
    pprint("Logging...")

def clearing_output():
    pprint("Clear output...")


with dag:
    ingestion_from_api = PythonOperator(
        task_id='ingestion_from_api',
        python_callable=ingestion,
    )

    load_metadata = PythonOperator(
        task_id='load_metadata',
        python_callable=fetch_metadata,
    )

    load_data_to_DB = PythonOperator(
        task_id='load_data_to_DB',
        python_callable=load_data_toDB,
    )

    stamp_log = PythonOperator(
        task_id='stamp_log',
        python_callable=stamp_logging,
    )

    clear_output = PythonOperator(
        task_id='clear_output',
        python_callable=clearing_output,
    )

ingestion_from_api >> load_metadata >> load_data_to_DB >> stamp_log >> clear_output

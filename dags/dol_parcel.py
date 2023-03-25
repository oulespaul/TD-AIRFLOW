from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
from pprint import pprint

import pytz
import requests
import pyodbc
import pandas as pd

tzInfo = pytz.timezone('Asia/Bangkok')
source_path = "/opt/airflow/dags/source/moc_api"
output_path = "/opt/airflow/dags/output/moc_api"
ingest_date = datetime.now(tz=tzInfo)

#Config
property_type = "Parcel"

doi_host = "https://10.13.16.166"
auth_path = "/api/v1/Auth/Validate"
main_change_path = "/apiExchange/v1/Change"

consumer_secret = "cs_mw1NAJU1pqNQkZVlXdVNvoCO6zutNkqXuSxcFdQ7Y2J"
consumer_key = "ck_bwTrnf24CssYIV7EzHxC5trFB07ioRggKl6NkWEwZu9"

server_host = '192.168.45.83'
server_port = "4070"
database = 'TRD_Raw'
username = 'udlake'
password = 'ekA@lataduat'
driver= '{SQL Server}'

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('DOL_PARCEL',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def authenticate(**kwargs):
    ti = kwargs['ti']
    HEADERS = {"Consumer-Key": consumer_key}
    PARAMS = {"ConsumerSecret": consumer_secret}
    try:
        response = requests.get(
            url=f"{doi_host}{auth_path}",
            params=PARAMS,
            headers=HEADERS,
            verify=False
        )

        if (response.status_code == 200):
            token = response.json()["token"]
            ti.xcom_push(key='auth_token', value=token)
            return token
        else:
            return None
    except:
        print("Authenticate failed!")

def get_land_office(**kwargs):
    ti = kwargs['ti']
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        sql_query = 'SELECT * FROM common.dbo.TB_MAS_LANDOFFICESEQ'
        offices = pd.read_sql(sql_query, connection)
        land_offices = offices["LANDOFFICE_ID"]

        land_offices_df = land_offices.to_json()
        ti.xcom_push(key='land_offices', value=land_offices_df)

        print(f"Total land office: {land_offices.count()}")

        connection.close()
    except:
        print("Get Land office failed!")

def get_column_mapping(**kwargs):
    ti = kwargs['ti']
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        sql_query = f"SELECT * FROM TDSERVICE.dbo.DOL_CHANGE_API_MAPPING WHERE property_type = '{property_type}'"
        mapping = pd.read_sql(sql_query, connection)

        mapping_column_df = mapping.to_json()
        ti.xcom_push(key='mapping_column', value=mapping_column_df)
        
        connection.close()
        return mapping
    except:
        print("Get Mapping column failed!")

def main(**kwargs):
    ti = kwargs['ti']
    token = ti.xcom_pull(key='auth_token')
    print(f"token -> {token}")

    land_offices_serialize = ti.xcom_pull(key='land_offices')
    land_offices_df = pd.read_json(land_offices_serialize)
    print(f"land_offices_df -> {land_offices_df}")

    mapping_column_serialize = ti.xcom_pull(key='mapping_column')
    mapping_column_df = pd.read_json(mapping_column_serialize)
    print(f"mapping_column_df -> {mapping_column_df}")

with dag:
    authentication = PythonOperator(
        task_id='authentication',
        python_callable=authenticate,
    )

    get_land_offices = PythonOperator(
        task_id='get_land_offices',
        python_callable=get_land_office,
    )

    get_columns_mapping = PythonOperator(
        task_id='get_columns_mapping',
        python_callable=get_column_mapping,
    )

    ingestion_and_load = PythonOperator(
        task_id='ingestion_and_load',
        python_callable=main,
    )

authentication >> get_land_offices >> get_columns_mapping >> ingestion_and_load
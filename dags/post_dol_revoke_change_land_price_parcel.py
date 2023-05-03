from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import requests
import pyodbc
import json
import pandas as pd

#Config
property_type = "RevokeChangeLandPriceParcel"

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
driver= '{ODBC Driver 17 for SQL Server}'

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('DOL_REVOKE_CHANGE_LAND_PRICE_PARCEL',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def authenticate():
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
            return response.json()["token"]
        else:
            return None
    except:
        print("Authenticate failed!")

def retrive_data_from_db():
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        sql_query = """
        SELECT
            A.PARCEL_VAL_ID,
            '2566' AS LANDPRICE_START_YEAR,
            '2569' AS LANDPRICE_END_YEAR,
            CONVERT(varchar, A.PUBLIC_DATE, 110) AS PUBLIC_DATE,
            CONVERT(varchar, A.ENFORCE_DATE, 110) AS ENFORCE_DATE,
            '12-31-2026' AS END_DATE,
            A.CHANGWAT_CODE,
            A.BRANCH_CODE,
            A.UTM_CODE,
            A.UTM_NO_P,
            A.UTM_NO,
            A.UTM_PAGE,
            A.UTM_RATIO,
            A.UTM_LANDNO,
            A.CHANODE_NO,
            A.SURVEY_NO,
            A.TUMBON_CODE,
            A.AMPHUR_CODE,
            CONCAT (A.NRAI ,'-' , A.NNHAN ,'-' , A.NWAH + A.DREMAIN) AS LAND_AREA_T,
            A.VAL_P_WA
            FROM (
            (SELECT * FROM land.dbo.PARCEL_VAL_10)A
            INNER JOIN
            (SELECT * FROM land.dbo.ORDER_VAL_REVOKE )B
                ON A.REMARK_FOLDER = B.REMARK_FOLDER
                    AND A.BRANCH_CODE = B.BRANCH_CODE
            )
            WHERE 1=1
                AND A.BRANCH_CODE = '10000000'
                    AND A.PERIODS_ID = 7
                    AND A.MAPZONE IN (47,48)
                    AND A.FLAG_PUBLIC = 1 
                AND A.FLAG_TYPE = 1
                AND A.REMARK_FOLDER IS NOT NULL
                AND A.FLAG_COMMIT = 'Y' 
                AND A.[STATUS] = 'REVOKE'
                AND B.ORDER_STATUS = 1
                AND A.POST_DOL IN (1,2);
"""

        result = pd.read_sql(sql_query, connection)
        connection.close()

        return result
    except:
        print("Get Data failed!")

def get_column_mapping():
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        sql_query = f"SELECT * FROM TDSERVICE.dbo.DOL_CHANGE_API_MAPPING_POST WHERE property_type = '{property_type}'"

        mapping = pd.read_sql(sql_query, connection)
        connection.close()

        return mapping
    except:
        print("Get Mapping column failed!")

def post_to_dol(auth_token, property_type, data):
    HEADERS = {
        "Content-Type": "application/json",
        "Consumer-Key": consumer_key,
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        response = requests.post(
            url=f"{doi_host}{main_change_path}/{property_type}",
            data=data,
            headers=HEADERS,
            verify=False
        )

        if (response.status_code == 200):
            return response.json()["result"]
        else:
            print(f"Post Data failed: {response.status_code}")
            return pd.DataFrame({})
    except:
        print("Post Data failed!")
        return pd.DataFrame({})

def update_post_status(id, status):
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        sql = f"UPDATE land.dbo.PARCEL_VAL_10 SET POST_DOL = {status}  WHERE PARCEL_VAL_ID = '{id}'"

        connection.execute(sql)
        connection.commit()

        print("Insert Success!")

        connection.close()
    except:
        print("Insert data failed!")

def process():
    data = retrive_data_from_db()
    mappings = get_column_mapping(property_type)

    mapped_df = data.rename(columns=mappings.set_index('destination_column')['source_column']).astype(
        {
            "UTMMAP2": str,
            "UTM_LANDNO": int,
            "PARCEL_NO": int,
            "SURVEY_NO": int,
            "VAL_P_WA": int
        }
    )
    mapped_df = mapped_df.fillna("")
    records = mapped_df.to_json(orient='records')
    list = json.loads(records)

    for item in list:
        auth_token = authenticate()
        res = post_to_dol(auth_token, property_type, json.dumps(item))

        id = item["PARCEL_VAL_ID"]
        print(f"Id: {id} -> Post Success : {res}")

        if (res):
            update_post_status(id, 3)
        else:
            update_post_status(id, 2)


with dag:
    retrive_data_and_post_to_dol = PythonOperator(
        task_id='retrive_data_and_post_to_dol',
        python_callable=process,
    )

retrive_data_and_post_to_dol
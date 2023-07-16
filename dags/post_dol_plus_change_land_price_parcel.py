from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import time
import requests
import pyodbc
import json
import pandas as pd

# Config
property_type = "PlusChangeLandPriceParcel"

doi_host = "https://10.13.16.166"
auth_path = "/api/v1/Auth/Validate"
main_change_path = "/apiExchange/v1/Change"

consumer_secret = "cs_mw1NAJU1pqNQkZVlXdVNvoCO6zutNkqXuSxcFdQ7Y2J"
consumer_key = "ck_bwTrnf24CssYIV7EzHxC5trFB07ioRggKl6NkWEwZu9"

server_host = '192.168.41.31'
server_port = '1433'
database = 'master'
username = 'sa'
password = 'TRDl;ylfuKUB'
driver = '{ODBC Driver 17 for SQL Server}'

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('POST_DOL_PLUS_CHANGE_LAND_PRICE_PARCEL',
          schedule_interval='@daily',
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


def retrive_data_from_db(land_office):
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD=" + "{" + password + "}"
        connection = pyodbc.connect(conn_str)
        table_landoffice = land_office[:2]

        sql_query = f"""
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
            (SELECT * FROM land.dbo.PARCEL_VAL_{table_landoffice})A
            INNER JOIN
            (SELECT * FROM land.dbo.ORDER_VAL)B
                ON A.REMARK_FOLDER = B.REMARK_FOLDER
                    AND A.BRANCH_CODE = B.BRANCH_CODE
            )
            WHERE 1=1
                AND A.BRANCH_CODE = '{land_office}'
                    AND A.PERIODS_ID = 7
                    AND A.MAPZONE IN (47 , 48)
                    AND A.FLAG_PUBLIC = 1 
                AND A.REMARK_FOLDER IS NOT NULL
                AND A.FLAG_COMMIT = 'Y' 
                AND A.[STATUS] in ('PLUS','PLUSS')
                AND B.ORDER_STATUS = 1
                AND A.POST_DOL IN (1,2);
"""

        result = pd.read_sql(sql_query, connection)
        connection.close()

        return result
    except Exception as e:
        print("Get Data failed!")
        print(e)


def get_column_mapping():
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD=" + "{" + password + "}"
        connection = pyodbc.connect(conn_str)

        sql_query = f"SELECT * FROM TDSERVICE.dbo.DOL_CHANGE_API_MAPPING_POST WHERE property_type = '{property_type}'"

        mapping = pd.read_sql(sql_query, connection)
        connection.close()

        return mapping
    except Exception as e:
        print("Get Mapping column failed!")
        print(e)


def post_to_dol(auth_token, data):
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
            return True
        else:
            print(f"Post Data failed: {response.status_code}")
            return False
    except Exception as e:
        print("Post Data failed!")
        print(e)
        return False


def update_post_status(id, status, land_office):
    table_landoffice = land_office[:2]

    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD=" + "{" + password + "}"
        connection = pyodbc.connect(conn_str)

        sql = f"UPDATE land.dbo.PARCEL_VAL_{table_landoffice} SET POST_DOL = {status}, SEND_DATE = GETDATE()  WHERE PARCEL_VAL_ID = '{id}'"

        connection.execute(sql)
        connection.commit()

        print("Update status Success!")

        connection.close()
    except Exception as e:
        print("Update status data failed!")
        print(e)

def get_land_office():
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD=" + "{" + password + "}"
        connection = pyodbc.connect(conn_str)

        sql_query = 'SELECT * FROM common.dbo.TB_MAS_LANDOFFICESEQ'
        offices = pd.read_sql(sql_query, connection)
        land_offices = offices["LANDOFFICE_ID"]

        print(f"Total land office: {land_offices.count()}")

        connection.close()

        return land_offices
    except Exception as e:
        print("Get Land office failed!")
        print(e)

def process():
    mappings = get_column_mapping()
    land_offices = get_land_office()

    for land_office in land_offices:
        data = retrive_data_from_db(land_office)
        data_size = data.shape[0]
        print(f"{land_office} -> {data_size} items")

        if(data_size == 0):
            continue

        data = data.fillna("0")
        mapped_df = data.rename(columns=mappings.set_index('destination_column')['source_column']).astype(
            {
                "UTMMAP2": int,
                "UTM_LANDNO": int,
                "PARCEL_NO": int,
                "SURVEY_NO": int,
                "VAL_P_WA": int
            }
            ).astype(
                {
                    "UTMMAP2": str,
                }
            )
        mapped_df = mapped_df.fillna("")
        records = mapped_df.to_json(orient='records')
        list = json.loads(records)

        for item in list:
            auth_token = authenticate()
            res = post_to_dol(auth_token, json.dumps(item))

            id = item["PARCEL_VAL_ID"]
            print(f"Id: {id} -> Post Success : {res}")

            if (res == True):
                update_post_status(id, 3, land_office)
            else:
                update_post_status(id, 2, land_office)
        
        time.sleep(5)


with dag:
    retrive_data_and_post_to_dol = PythonOperator(
        task_id='retrive_data_and_post_to_dol',
        python_callable=process,
    )

retrive_data_and_post_to_dol

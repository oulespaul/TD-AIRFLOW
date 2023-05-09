from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import time
import pytz
import requests
import pyodbc
import pandas as pd

tzInfo = pytz.timezone('Asia/Bangkok')
source_path = "/opt/airflow/dags/source/moc_api"
output_path = "/opt/airflow/dags/output/moc_api"
ingest_date = datetime.now(tz=tzInfo)

#Config
property_type = "MapLandNS3A47"

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

dag = DAG('DOL_MAP_LAND_NS3A_47',
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

def ingestion_data(property_type, land_office, auth_token, yearTrigger, monthTrigger):
    HEADERS = {
        "Consumer-Key": consumer_key,
        "Authorization": f"Bearer {auth_token}"
    }
    PARAMS = {
        "OrganizationID": land_office,
        "Month": monthTrigger,
        "Year": yearTrigger
    }
    try:
        response = requests.get(
            url=f"{doi_host}{main_change_path}/{property_type}",
            params=PARAMS,
            headers=HEADERS,
            verify=False
        )

        if (response.status_code == 200):
            result = response.json()
            result_df = pd.json_normalize(result)
            data = result_df
            return data.fillna('')
        else:
            print(f"Ingestion Data failed: {response.status_code}")
            return pd.DataFrame({})
    except:
        print("Ingestion Data failed!")
        return pd.DataFrame({})

def get_land_office():
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        sql_query = 'SELECT * FROM common.dbo.TB_MAS_LANDOFFICESEQ'
        offices = pd.read_sql(sql_query, connection)
        land_offices = offices["LANDOFFICE_ID"]
        print(f"Total land office: {land_offices.count()}")
        connection.close()
        return land_offices
    except:
        print("Get Land office failed!")

def get_column_mapping(property_type):
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        sql_query = f"SELECT * FROM TDSERVICE.dbo.DOL_CHANGE_API_MAPPING WHERE property_type = '{property_type}'"
        mapping = pd.read_sql(sql_query, connection)

        connection.close()
        return mapping
    except:
        print("Get Mapping column failed!")

def load_to_lake(data, mapping_column):
    destination_table = mapping_column['destination_table'][0]
    destination_column = ','.join(mapping_column['destination_column'].astype(str))
    source_column = mapping_column['source_column']

    group_index = (data.index // 500)

    # Group the rows by the calculated index
    grouped_df = data.groupby(group_index)

    # Iterate over the groups
    for group_name, group_df in grouped_df:
        print(f"Group {group_name}")
        values_sql_list = []
        for _index, row in group_df.iterrows():
            row_value = []
            for column in source_column:
                value = f"'{row[column]}'" if column != "geometry" else f"{row[column]}"
                row_value.append(value)
            row_value_sql = ",".join(row_value)
            values_sql_list.append(f"({row_value_sql}, CURRENT_TIMESTAMP)")
        
        values_sql = ",".join(values_sql_list)
        insert_sql = f"INSERT INTO {destination_table} ({destination_column}, IMPORT_DATE) VALUES {values_sql};"

        insert_data(insert_sql)

def insert_data(sql):
    try:
        conn_str = f"DRIVER={driver};SERVER={server_host},{server_port};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(conn_str)

        connection.execute(sql)
        connection.commit()

        print("Insert Success!")

        connection.close()
    except:
        print("Insert data failed!")

def transform_data(data):
    feature_df = pd.DataFrame()
    gis_features = data.get('LocationGeospatial.features')

    for feature in gis_features[0]:
        coordinates = feature['geometry']['coordinates']

        for polygon in coordinates:
            sql_coordinates = ''

            sql_coordinates += "geometry::STGeomFromText('POLYGON (("
            for point in polygon:
                sql_coordinates += f"{str(point[0])} {str(point[1])},"
            # Remove the trailing comma and add closing parentheses for the polygon
            sql_coordinates = sql_coordinates[:-1] + '))' + "', 0)"

            feature['geometry'] = sql_coordinates
            feature_df = pd.concat([feature_df, pd.json_normalize(feature)], ignore_index=True).fillna('')

    return feature_df

def ingestion(**kwargs):
    triggerParams = kwargs["params"]
    year = ingest_date.year + 543
    month = ingest_date.strftime('%m')

    yearTrigger = triggerParams.get("year", year)
    monthTrigger = triggerParams.get("month", month)

    print(f"trigger -> {yearTrigger}:{monthTrigger}")

    land_offices = get_land_office()
    mapping = get_column_mapping(property_type)
    
    for land_office in land_offices:
        auth_token = authenticate()
        data = ingestion_data(property_type, land_office, auth_token, yearTrigger, monthTrigger)
        data_size = data.shape[0]
        print(f"{land_office} -> {data_size} items")

        if(data_size == 0):
            continue
        data_transformed = transform_data(data)
        load_to_lake(data_transformed, mapping)
        time.sleep(5)

with dag:
    ingestion_and_load = PythonOperator(
        task_id='ingestion_and_load',
        python_callable=ingestion,
    )

ingestion_and_load
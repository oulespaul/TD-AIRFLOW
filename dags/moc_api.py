from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
from airflow.operators.bash import BashOperator

import os
import pytz
import json
import requests

tzInfo = pytz.timezone('Asia/Bangkok')
source_path = "/opt/airflow/dags/source/moc_api"
output_path = "/opt/airflow/dags/output/moc_api"
ingest_date = datetime.now(tz=tzInfo)

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('MOC_API',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def ingestion():
    # Get province codes
    json_file = open(f'{source_path}/province_code.json')
    province_codes = json.load(json_file)
    json_file.close()

    # Get com codes
    json_file = open(f'{source_path}/com_code.json')
    com_codes = json.load(json_file)
    json_file.close()

    for province in province_codes:
        province_code = province['CODE']
        for com in com_codes:
            com_code = com['CODE']
            file_name = f"{output_path}/{province_code}/{com_code}/2012_2022"
            dirname = os.path.dirname(file_name)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(f'{file_name}.json', 'w', encoding='utf8') as file:
                response = requests.get(
                    f"https://dataapi.moc.go.th/csi-product-indexes?com_code={com_code}&province_code={province_code}&from_year=2012&to_year=2022")
                if response.status_code != 200:
                    continue
                data = response.json()
                json.dump(data, file, ensure_ascii=False)
    print('Done')


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))

    ingest_date = datetime.now(tz=tzInfo)

    for subdir, dirs, files in os.walk(output_path):
        for file in files:
            print("file: " + file)
            if file.endswith(".gitkeep"):
                continue

            folder_name = subdir.replace(output_path, "")
            my_dir = kwargs['directory'] + "/" + \
                ingest_date.strftime("%Y%m%d") + folder_name
            print("my_dir: " + my_dir)
            hdfs.make_dir(my_dir)
            hdfs.make_dir(my_dir, permission=755)

            file_path = os.path.join(output_path, subdir, file)
            print(f"file path: {file_path}")
            with open(file_path, 'r', encoding="utf8") as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data.encode('utf-8'), overwrite=True)

                pprint("Stored! file: {}".format(file))


def store_to_hdfs_for_redundant(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host_redundant"),
                           port=Variable.get("hdfs_port_redundant"), user_name=Variable.get("hdfs_username_redundant"))

    ingest_date = datetime.now(tz=tzInfo)

    for subdir, dirs, files in os.walk(output_path):
        for file in files:
            print("file: " + file)
            if file.endswith(".gitkeep"):
                continue

            folder_name = subdir.replace(output_path, "")
            my_dir = kwargs['directory'] + "/" + \
                ingest_date.strftime("%Y%m%d") + folder_name
            print("my_dir: " + my_dir)
            hdfs.make_dir(my_dir)
            hdfs.make_dir(my_dir, permission=755)

            file_path = os.path.join(output_path, subdir, file)
            print(f"file path: {file_path}")
            with open(file_path, 'r', encoding="utf8") as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data.encode('utf-8'), overwrite=True)

                pprint("Stored! file: {}".format(file))


with dag:
    ingestion_from_api = PythonOperator(
        task_id='ingestion_from_api',
        python_callable=ingestion,
    )

    # load_to_hdfs = PythonOperator(
    #     task_id='load_to_hdfs',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_raw_zone/moc_api'},
    # )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_raw_zone/moc_api'},
    )

    # load_to_hdfs_processed = PythonOperator(
    #     task_id='load_to_hdfs_processed',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_processed_zone/moc_api'},
    # )

    load_to_hdfs_processed_for_redundant = PythonOperator(
        task_id='load_to_hdfs_processed_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_processed_zone/moc_api'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -rf /opt/airflow/dags/output/moc_api/*',
    )

ingestion_from_api >> load_to_hdfs_for_redundant >> load_to_hdfs_processed_for_redundant >> clean_up_output

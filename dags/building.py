from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
import os
import pytz
import requests

tzInfo = pytz.timezone('Asia/Bangkok')

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('BUILDING',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/ImagePool/image/Building(New)"

    file_count = 0
    for subdir, dirs, files in os.walk(path):
        pprint(f"Floder {subdir} ingesting...")
        for file in files:
            file_path = os.path.join(path, subdir, file)
            with open(file_path, 'rb') as file_data:
                my_data = file_data.read()
                hdfs.create_file(my_dir+f"/{file}", my_data, overwrite=True)
                file_count+=1

        pprint(f"Floder {subdir} ingestion done")

    pprint("Ingestion done!")
    pprint(hdfs.list_dir(my_dir))
    stamp_logging(file_count, my_dir)


def store_to_hdfs_for_redundant(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host_redundant"),
                           port=Variable.get("hdfs_port_redundant"), user_name=Variable.get("hdfs_username_redundant"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/ImagePool/image/Building(New)"

    file_count = 0
    for subdir, dirs, files in os.walk(path):
        pprint(f"Floder {subdir} ingesting...")
        for file in files:
            file_path = os.path.join(path, subdir, file)
            with open(file_path, 'rb') as file_data:
                my_data = file_data.read()
                hdfs.create_file(my_dir+f"/{file}", my_data, overwrite=True)
                file_count += 1

        pprint(f"Floder {subdir} ingestion done")

    pprint("Ingestion done!")
    pprint(hdfs.list_dir(my_dir))
    stamp_logging(file_count, my_dir)

def stamp_logging(totalFile, tgtFolder):
    ingest_date = datetime.now(tz=tzInfo)
    url = "http://192.168.45.110:3000/un-structure-report/stamp-report"
    payload = {
        "ingestionDatetime": ingest_date.strftime("%Y-%m-%d %H:%M:%S"),
        "srcFolder": "building",
        "totalSrcFile": totalFile,
        "tgtFolder": tgtFolder,
        "totalFileLoaded": totalFile,
        "status":"Success"
    }
    response = requests.post(url, json = payload)

    pprint(f"Stamp logging response: {response}")

with dag:
    # Raw Zone
    # load_to_hdfs = PythonOperator(
    #     task_id='load_to_hdfs',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_raw_zone/building'},
    # )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_raw_zone/building'},
    )

    # Processed Zone
    # load_to_hdfs_processed = PythonOperator(
    #     task_id='load_to_hdfs_processed',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_processed_zone/building'},
    # )

    load_to_hdfs_processed_for_redundant = PythonOperator(
        task_id='load_to_hdfs_processed_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_processed_zone/building'},
    )

load_to_hdfs_for_redundant >> load_to_hdfs_processed_for_redundant

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
import os
import pytz

tzInfo = pytz.timezone('Asia/Bangkok')

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('XML',
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

    path = "/opt/airflow/ImagePool/UAT XML"

    for subdir, dirs, files in os.walk(path):
        pprint(f"Floder {subdir} ingesting...")
        for file in files:
            file_path = os.path.join(path, subdir, file)
            with open(file_path, 'rb') as file_data:
                my_data = file_data.read()
                hdfs.create_file(my_dir+f"/{file}", my_data, overwrite=True)

        pprint(f"Floder {subdir} ingestion done")

    pprint("Ingestion done!")
    pprint(hdfs.list_dir(my_dir))


def store_to_hdfs_for_redundant(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host_redundant"),
                           port=Variable.get("hdfs_port_redundant"), user_name=Variable.get("hdfs_username_redundant"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/ImagePool/UAT XML"

    for subdir, dirs, files in os.walk(path):
        pprint(f"Floder {subdir} ingesting...")
        for file in files:
            file_path = os.path.join(path, subdir, file)
            with open(file_path, 'rb') as file_data:
                my_data = file_data.read()
                hdfs.create_file(my_dir+f"/{file}", my_data, overwrite=True)

        pprint(f"Floder {subdir} ingestion done")

    pprint("Ingestion done!")
    pprint(hdfs.list_dir(my_dir))


with dag:
    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/raw_zone/xml'},
    )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/raw_zone/xml'},
    )

    load_to_hdfs_processed = PythonOperator(
        task_id='load_to_hdfs_processed',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/processed_zone/xml'},
    )

    load_to_hdfs_processed_for_redundant = PythonOperator(
        task_id='load_to_hdfs_processed_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/processed_zone/xml'},
    )

load_to_hdfs >> load_to_hdfs_for_redundant >> load_to_hdfs_processed >> load_to_hdfs_processed_for_redundant

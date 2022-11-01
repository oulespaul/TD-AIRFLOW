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
output_path = "/opt/airflow/dags/output/flat_file"
ingest_date = datetime.now(tz=tzInfo)

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('FLAT_FILE',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def download_flat_file():
    url = "https://www.treasury.go.th/th/download.php?ref=oJEaLKEinJk4oaO3oJ93MRksoJIaoUEcnJM4pKOSoJI3oRkvoJSaqUEsnFM4AUNloGS3ARjkoKSaEKExnJy4KjoSo3QoSo3Q"
    res = requests.get(url, allow_redirects=True)

    with open(f"{output_path}/รายงานผลความพึงพอใจในการให้บริการ_{ingest_date.strftime('%Y%m%d')}.pdf", 'wb') as file:
        file.write(res.content)


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    os.chdir(output_path)
    for file in os.listdir():
        if file.endswith(".pdf"):
            file_path = f"{output_path}/{file}"

            with open(file_path, 'rb') as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data, overwrite=True)

                pprint("Stored! file: {}".format(file))
                pprint(hdfs.list_dir(my_dir))


def store_to_hdfs_for_redundant(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host_redundant"),
                           port=Variable.get("hdfs_port_redundant"), user_name=Variable.get("hdfs_username_redundant"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    os.chdir(output_path)
    for file in os.listdir():
        if file.endswith(".pdf"):
            file_path = f"{output_path}/{file}"

            with open(file_path, 'rb') as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data, overwrite=True)

    pprint("Stored! file: {}".format(file))
    pprint(hdfs.list_dir(my_dir))


with dag:
    ingest_flat_file = PythonOperator(
        task_id='download_flat_file',
        python_callable=download_flat_file,
    )

    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/raw_zone/flat_file'},
    )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/raw_zone/flat_file'},
    )

    load_to_hdfs_processed = PythonOperator(
        task_id='load_to_hdfs_processed',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/processed_zone/flat_file'},
    )

    load_to_hdfs_processed_for_redundant = PythonOperator(
        task_id='load_to_hdfs_processed_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/processed_zone/flat_file'},
    )

ingest_flat_file >> load_to_hdfs >> load_to_hdfs_for_redundant >> load_to_hdfs_processed >> load_to_hdfs_processed_for_redundant

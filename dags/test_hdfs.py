from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
import os

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('TEST_HDFS',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/test"

    os.chdir(path)

    for file in os.listdir():
        if file.endswith(".csv"):
            file_path = f"{path}/{file}"

            with open(file_path, 'r', encoding="utf8") as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data.encode('utf-8'), overwrite=True)

                pprint("Stored! file: {}".format(file))
                pprint(hdfs.list_dir(my_dir))


with dag:
    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/test'},
    )


load_to_hdfs
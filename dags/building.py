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

dag = DAG('BUILDING',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))
    
    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" +ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/ImagePool/image/Building(New)"

    os.chdir(path)

    for folder in os.listdir(my_dir):
        for file in folder:
            pprint(f"Floder {folder} ingesting...")
            if file.endswith(".JPG"):
                file_path = f"{path}/{file}"

                with open(file_path, 'rb') as file_data:
                    my_data = file_data.read()
                    hdfs.create_file(
                        my_dir+f"/{file}", my_data, overwrite=True)

                    pprint("Stored! file: {}".format(file))
            pprint(f"Floder {folder} ingestion done")
    
    pprint("Ingestion done!")
    pprint(hdfs.list_dir(my_dir))

with dag:
    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/raw_zone/building'},
    )


load_to_hdfs

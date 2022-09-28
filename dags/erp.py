from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
import os
import pytz
import psycopg2
import pandas as pd
from psycopg2 import Error

tzInfo = pytz.timezone('Asia/Bangkok')
output_path = "/opt/airflow/dags/output/erp"
ingest_date = datetime.now(tz=tzInfo)

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('ERP',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def ingestion():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="odoo",
                                      password="p@ssw0rd",
                                      host="192.168.45.158",
                                      port="5432",
                                      database="etp")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        # print("PostgreSQL server information")
        # print(connection.get_dsn_parameters(), "\n")

        # Executing a SQL query
        cursor.execute("SELECT * FROM account_account")
        # Fetch result
        record = cursor.fetchall()
        cols = []
        for elt in cursor.description:
            cols.append(elt[0])

        df = pd.DataFrame(data=record, columns=cols)
        df.to_csv(f'{output_path}/account_{ingest_date.strftime("%Y%m%d")}.csv')
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    os.chdir(output_path)
    for file in os.listdir():
        if file.endswith(".csv"):
            file_path = f"{output_path}/{file}"

            with open(file_path, 'r', encoding="utf8") as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data.encode('utf-8'), overwrite=True)

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
        if file.endswith(".csv"):
            file_path = f"{output_path}/{file}"

            with open(file_path, 'r', encoding="utf8") as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data.encode('utf-8'), overwrite=True)

    pprint("Stored! file: {}".format(file))
    pprint(hdfs.list_dir(my_dir))


with dag:
    ingestion_task = PythonOperator(
        task_id='ingestion',
        python_callable=ingestion,
    )

    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/raw_zone/erp'},
    )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/raw_zone/erp'},
    )

ingestion_task >> load_to_hdfs >> load_to_hdfs_for_redundant

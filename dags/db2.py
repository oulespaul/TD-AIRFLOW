from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable

import os
import pytz
import pandas as pd
import ibm_db
import ibm_db_dbi
import requests

tzInfo = pytz.timezone('Asia/Bangkok')
output_path = "/opt/airflow/dags/output/db2"
ingest_date = datetime.now(tz=tzInfo)

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('DB2',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def ingestion():
    dsn_hostname = "192.168.45.158"
    dsn_uid = "db2inst1"
    dsn_pwd = "mypassword1"
    dsn_driver = "{IBM DB2 ODBC DRIVER}"
    dsn_database = "testdb"
    dsn_port = "50000"
    dsn_protocol = "TCPIP"

    dsn = (
        "DRIVER={0};"
        "DATABASE={1};"
        "HOSTNAME={2};"
        "PORT={3};"
        "PROTOCOL={4};"
        "UID={5};"
        "PWD={6};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd)

    print(dsn)

    sql = """
    SELECT * FROM TRD.PRODUCT_CATEGORY
    """

    try:
        conn = ibm_db.connect(dsn, "", "")
        print("Connected to database: ", dsn_database, "as user: ", dsn_uid,
              "on ho                                     st: ", dsn_hostname)

        db_conn = ibm_db_dbi.Connection(conn)
        df = pd.read_sql(sql, db_conn)
        print(df)
        df.to_csv(
            f'{output_path}/product_catagory_{ingest_date.strftime("%Y%m%d")}.csv')
    except:
        print("Unable to connect: ", ibm_db.conn_errormsg())


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    file_count = 0
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
                file_count+=1
                
    stamp_logging(file_count, my_dir)


def store_to_hdfs_for_redundant(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host_redundant"),
                           port=Variable.get("hdfs_port_redundant"), user_name=Variable.get("hdfs_username_redundant"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    file_count = 0
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
                file_count+=1

    stamp_logging(file_count, my_dir)

def stamp_logging(totalFile, tgtFolder):
    url = "http://192.168.45.110:3000/un-structure-report/stamp-report"
    payload = {
        "ingestionDatetime": ingest_date.strftime("%Y-%m-%d %H:%M:%S"),
        "srcFolder": "db2",
        "totalSrcFile": totalFile,
        "tgtFolder": tgtFolder,
        "totalFileLoaded": totalFile,
        "status":"Success"
    }
    response = requests.post(url, json = payload)

    pprint(f"Stamp logging response: {response}")

with dag:
    ingestion_task = PythonOperator(
        task_id='ingestion',
        python_callable=ingestion,
    )

    # load_to_hdfs = PythonOperator(
    #     task_id='load_to_hdfs',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_raw_zone/db2'},
    # )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_raw_zone/db2'},
    )

    # load_to_hdfs_processed = PythonOperator(
    #     task_id='load_to_hdfs_processed',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_processed_zone/db2'},
    # )

    load_to_hdfs_processed_for_redundant = PythonOperator(
        task_id='load_to_hdfs_processed_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_processed_zone/db2'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/db2/*',
    )

ingestion_task >> load_to_hdfs_for_redundant >> load_to_hdfs_processed_for_redundant >> clean_up_output

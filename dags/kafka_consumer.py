from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
from confluent_kafka import Consumer, KafkaError, KafkaException

import os
import pytz
import pandas as pd

tzInfo = pytz.timezone('Asia/Bangkok')
output_path = "/opt/airflow/dags/output/db2"
ingest_date = datetime.now(tz=tzInfo)

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('KAFKA_CONSUMER',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def ingestion():
    conf = {'bootstrap.servers': "192.168.45.158:9092",
            'group.id': "ou-test-id",
            'enable.auto.commit': False,
            'auto.offset.reset': 'smallest'}

    consumer = Consumer(conf)
    topic_name = 'twitter_kafka'

    try:
        consumer.subscribe([topic_name])
        while True:
            msg = consumer.poll(timeout=1.0) # Set Timeout
            if msg is None: continue # Check condition that no have any message then skip

            if msg.error(): # Check condition if have any message then write log and end of consume
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                consumer.commit(asynchronous=False) # Commit message what consumed
                print(msg.value().decode('utf-8')) # Display message consumed
    except:
        print("Consume error")
    finally:
        consumer.close()


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
        op_kwargs={'directory': '/data/raw_zone/db2'},
    )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/raw_zone/db2'},
    )

ingestion_task >> load_to_hdfs >> load_to_hdfs_for_redundant

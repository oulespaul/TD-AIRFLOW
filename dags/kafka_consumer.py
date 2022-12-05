from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
from confluent_kafka import Consumer, KafkaError, KafkaException

import os
import pytz
import sys

tzInfo = pytz.timezone('Asia/Bangkok')
output_path = "/opt/airflow/dags/output/kafka"
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
        with open(f"{output_path}/kafka_twitter_{ingest_date.strftime('%Y%m%d')}.txt", "w", encoding="utf-8") as f:
            msg_count = 0
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # consumer.commit(asynchronous=False)
                    value = msg.value().decode('utf-8')
                    f.write(value)
                    f.write('\n')
                    print(value)
                    msg_count += 1
                    if msg_count == 50:
                        return
    finally:
        # Close down consumer to commit final offsets.
        return consumer.close()


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))

    ingest_date = datetime.now(tz=tzInfo)
    my_dir = kwargs['directory'] + "/" + ingest_date.strftime("%Y%m%d")
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    os.chdir(output_path)
    for file in os.listdir():
        if file.endswith(".txt"):
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
        if file.endswith(".txt"):
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

    # load_to_hdfs = PythonOperator(
    #     task_id='load_to_hdfs',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_raw_zone/kafka'},
    # )

    load_to_hdfs_for_redundant = PythonOperator(
        task_id='load_to_hdfs_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_raw_zone/kafka'},
    )

    # load_to_hdfs_processed = PythonOperator(
    #     task_id='load_to_hdfs_processed',
    #     python_callable=store_to_hdfs,
    #     op_kwargs={'directory': '/data/UAT_processed_zone/kafka'},
    # )

    load_to_hdfs_processed_for_redundant = PythonOperator(
        task_id='load_to_hdfs_processed_for_redundant',
        python_callable=store_to_hdfs_for_redundant,
        op_kwargs={'directory': '/data/UAT_processed_zone/kafka'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/kafka/*',
    )

ingestion_task >> load_to_hdfs_for_redundant >> load_to_hdfs_processed_for_redundant >> clean_up_output

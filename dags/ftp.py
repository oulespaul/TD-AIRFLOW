from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import os
import pysftp
import time
from tqdm import tqdm

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# TODO: Change to airflow vaiable
sftp_server = "203.154.177.2"
port = 8722
username = "tsrdupload"
password = "LT@x2565##"

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('FTP',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def upload_to_sftp(server, port, username, password, local_path, dest_path):
    try:
        with pysftp.Connection(server, port=port, username=username, password=password, cnopts=cnopts) as sftp:
            file_list = os.listdir(local_path)
            total_files = len(file_list)

            with tqdm(total=total_files, desc="Uploading", unit='folder') as pbar:
                for item in file_list:
                    local_item_path = os.path.join(local_path, item)
                    remote_item_path = os.path.join(
                        dest_path, item).replace("\\", "/")

                    if os.path.isfile(local_item_path):
                        sftp.put(local_item_path, remote_item_path)

                    elif os.path.isdir(local_item_path):
                        sftp.makedirs(remote_item_path)

                        print(f'Created remote directory: {remote_item_path}')
                        upload_to_sftp(server, port, username, password,
                                       local_item_path, remote_item_path)

                    pbar.set_postfix(file=local_item_path)
                    pbar.update(1)
                    time.sleep(0.5)

            print("Transfer success!")
    except pysftp.AuthenticationException as e:
        print("Auth failed", str(e))
    except pysftp.ConnectionException as e:
        print("Connection failed", str(e))
    except Exception as e:
        print("Something wrong", str(e))


with dag:
    upload_to_sftp = PythonOperator(
        task_id='ingestion',
        python_callable=upload_to_sftp,
    )


upload_to_sftp

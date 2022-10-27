from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz

tzInfo = pytz.timezone('Asia/Bangkok')

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('DQ_TABLE_UPDATE_ALL',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


with dag:
    run_dq_check = BashOperator(
        task_id='run_dq_check',
        bash_command='cd /opt/airflow/DataQuality && ./run_dq_table_update_all.sh ',
    )

run_dq_check

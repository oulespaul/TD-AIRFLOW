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

dag = DAG('ATLAS_PROD_UPDATE_LAST_2_DAY',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


with dag:
    run_dq_check = BashOperator(
        task_id='run_dq_check',
        bash_command='cd /opt/airflow/Atlas && ./Production_Update_Last2Day.sh ',
    )

run_dq_check

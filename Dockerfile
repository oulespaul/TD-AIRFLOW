FROM apache/airflow:2.2.0-python3.7
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
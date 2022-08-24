FROM apache/airflow:2.3.3-python3.7
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
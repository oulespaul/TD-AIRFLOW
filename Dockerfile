FROM apache/airflow:2.2.0-python3.8
USER root
RUN apt-get update && apt-get -y install libpq-dev python-dev python3-dev gcc

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
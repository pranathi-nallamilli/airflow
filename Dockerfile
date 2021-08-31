FROM apache/airflow:2.1.0

COPY ./dags/ /opt/airflow/dags
COPY requirements.txt .

RUN pip install -r requirements.txt
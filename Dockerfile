FROM apache/airflow:3.0.4

ADD requirements.txt .
RUN pip install apache-airflow==3.0.4 -r requirements.txt


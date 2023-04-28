FROM apache/airflow:2.5.1-python3.9
USER root

USER airflow

#INSTALL REQUIREMENTS
COPY requirements.txt /

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt





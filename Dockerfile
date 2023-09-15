FROM apache/airflow:2.6.3-python3.11

COPY ./requirements.txt .

# RUN pip install --no-cache-dir -r /opt/usr/airflow/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
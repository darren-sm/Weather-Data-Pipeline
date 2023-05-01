FROM apache/airflow:2.5.3-python3.10

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
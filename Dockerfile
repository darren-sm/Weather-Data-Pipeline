FROM apache/airflow:2.3.0
WORKDIR /opt/airflow

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
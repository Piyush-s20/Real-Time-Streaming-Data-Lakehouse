FROM apache/airflow:2.9.3-python3.11

# deltalake lets the DAG read the Gold Delta table (through its transaction log) directly from MinIO.
# 0.18.2 is the version pinned in Airflow 2.9.3's constraints file, so pandas/pyarrow stay untouched.
RUN pip install --no-cache-dir "apache-airflow==2.9.3" "deltalake==0.18.2" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

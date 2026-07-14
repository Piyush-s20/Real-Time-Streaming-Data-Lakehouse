from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'daily_gold_layer_reporting',
    default_args=default_args,
    description='Generates end-of-day reports from the Gold Medallion layer',
    schedule_interval=timedelta(days=1), # Runs once a day
    catchup=False,
) as dag:

    # Task 1: Check if new data exists in the Gold Layer (MinIO S3)
    # In a real environment, this would use an S3KeySensor
    check_gold_data = BashOperator(
        task_id='check_gold_data_exists',
        bash_command='echo "Checking MinIO s3a://gold/financial_metrics/ for new Parquet files..."'
    )

    # Task 2: Trigger a Spark Batch Job to generate the daily report
    generate_daily_report = BashOperator(
        task_id='generate_daily_report',
        bash_command='echo "Executing spark-submit for daily_report.py against the Gold bucket..."'
    )

    # Task 3: Send a success alert to the team
    send_alert = BashOperator(
        task_id='send_success_alert',
        bash_command='echo "Alert: Daily financial reporting complete. BI Dashboards updated."'
    )

    # Define the execution order and dependencies
    check_gold_data >> generate_daily_report >> send_alert
"""Daily report built from the Gold Delta table in MinIO.

check_gold_data_exists -> generate_daily_report -> log_report_summary

The `minio_s3` connection is an AWS connection whose endpoint_url points to MinIO.
docker-compose.yml defines it through the AIRFLOW_CONN_MINIO_S3 environment variable.
"""
import io
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

log = logging.getLogger(__name__)

MINIO_CONN_ID = 'minio_s3'
GOLD_BUCKET = 'gold'
GOLD_TABLE = 'financial_metrics'  # Delta table written by src/streaming/gold_aggregation.py

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


def generate_daily_report(ds):
    """Summarises one UTC day of Gold windows per symbol and writes it to s3a://gold/reports/date=<ds>/."""
    # Heavy imports stay inside the task so the scheduler can parse this file quickly
    import pandas as pd
    from deltalake import DeltaTable

    conn = BaseHook.get_connection(MINIO_CONN_ID)
    storage_options = {
        'AWS_ACCESS_KEY_ID': conn.login,
        'AWS_SECRET_ACCESS_KEY': conn.password,
        'AWS_ENDPOINT_URL': conn.extra_dejson['endpoint_url'],
        'AWS_REGION': conn.extra_dejson.get('region_name', 'us-east-1'),
        'AWS_ALLOW_HTTP': 'true',
    }

    # Reading through the Delta transaction log only returns committed data
    gold = DeltaTable(f's3://{GOLD_BUCKET}/{GOLD_TABLE}', storage_options=storage_options)
    windows = gold.to_pandas(columns=['window_start', 'symbol', 'moving_avg_price', 'total_volume'])

    day_start = pd.Timestamp(ds, tz='UTC')
    window_start = pd.to_datetime(windows['window_start'], utc=True)
    day = windows[(window_start >= day_start) & (window_start < day_start + pd.Timedelta(days=1))]
    if day.empty:
        raise AirflowSkipException(f'The Gold table has no windows for {ds}')

    summary = day.groupby('symbol', as_index=False).agg(
        avg_price=('moving_avg_price', 'mean'),
        total_volume=('total_volume', 'sum'),
        num_windows=('moving_avg_price', 'size'),
    )

    # Re-running the task for the same day overwrites that day's report
    key = f'reports/date={ds}/daily_summary.parquet'
    buffer = io.BytesIO()
    summary.to_parquet(buffer, index=False)
    S3Hook(aws_conn_id=MINIO_CONN_ID).load_bytes(buffer.getvalue(), key=key, bucket_name=GOLD_BUCKET, replace=True)
    log.info('Wrote %d rows to s3a://%s/%s from Gold table version %d',
             len(summary), GOLD_BUCKET, key, gold.version())

    # The return value is pushed to XCom for the summary task
    return {
        'report_path': f's3a://{GOLD_BUCKET}/{key}',
        # The report can be reproduced by time-travelling the Gold table to this version
        'gold_version': gold.version(),
        'symbols': [
            {
                'symbol': row.symbol,
                'avg_price': round(float(row.avg_price), 2),
                'total_volume': int(row.total_volume),
                'num_windows': int(row.num_windows),
            }
            for row in summary.itertuples()
        ],
    }


def log_report_summary(ds, ti):
    report = ti.xcom_pull(task_ids='generate_daily_report')
    symbols = report['symbols']
    log.info(
        'Daily Gold report for %s (Gold table version %d): %d symbols, %d windows, total volume %d -> %s',
        ds,
        report['gold_version'],
        len(symbols),
        sum(s['num_windows'] for s in symbols),
        sum(s['total_volume'] for s in symbols),
        report['report_path'],
    )
    for s in symbols:
        log.info('  %-8s avg price %9.2f | volume %9d | windows %5d',
                 s['symbol'], s['avg_price'], s['total_volume'], s['num_windows'])


# Define the DAG
with DAG(
    'daily_gold_layer_reporting',
    default_args=default_args,
    description='Generates end-of-day reports from the Gold Medallion layer',
    schedule='@daily',  # Runs once a day; each run reports on its logical date (ds)
    catchup=False,
) as dag:

    # Task 1: Wait until the Gold Delta table in MinIO has a commit and data files
    check_gold_data = S3KeySensor(
        task_id='check_gold_data_exists',
        aws_conn_id=MINIO_CONN_ID,
        bucket_name=GOLD_BUCKET,
        bucket_key=[f'{GOLD_TABLE}/_delta_log/*.json', f'{GOLD_TABLE}/part-*.parquet'],
        wildcard_match=True,
        mode='reschedule',
        poke_interval=60,
        timeout=60 * 60,
    )

    # Task 2: Aggregate the day's Gold windows per symbol and write the report back to MinIO
    generate_report = PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_daily_report,
    )

    # Task 3: Log a short summary of the report
    log_summary = PythonOperator(
        task_id='log_report_summary',
        python_callable=log_report_summary,
    )

    # Define the execution order and dependencies
    check_gold_data >> generate_report >> log_summary

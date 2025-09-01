from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/home/ubuntu/')  # This makes sure Airflow can import your ETL script
from etl_twitter_sentiment import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='twitter_sentiment_etl_dag',
    default_args=default_args,
    description='ETL for Twitter sentiment using S3 CSV',
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False
) as dag:
    etl_task = PythonOperator(
        task_id='run_twitter_etl',
        python_callable=run_twitter_etl
    )


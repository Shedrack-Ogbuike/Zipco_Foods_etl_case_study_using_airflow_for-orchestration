from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import Pytho
from Extraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 18),
    'email': 'ogbuike.shedrack@gmail.com'
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
dag = DAG(
    'zipco_foods_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Zipco Foods',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
extraction_task = PythonOperator(
    task_id='extraction_layer',
    python_callable=run_extraction
    dag=dag,)

transformation_task = PythonOperator(
    task_id='transformation_layer',
    python_callable=run_transformation,
    dag=dag,)

loading_task = PythonOperator(
    task_id='loading_layer',
    python_callable=run_loading,
    dag=dag,)

extraction_task >> transformation_task >> loading_task

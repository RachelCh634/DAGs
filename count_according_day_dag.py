from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import pandas as pd
import os

def process_excel(input_file, output_file):
    df = pd.read_excel(input_file)
    total_men = df['countOfMale'].sum()
    total_women = df['countOfFemale'].sum()
    total_vehicles = df['countOfCars'].sum()
    result_df = pd.DataFrame({
        'תאריך': [datetime.now().strftime('%Y-%m-%d')],
        'סה"כ גברים': [total_men],
        'סה"כ נשים': [total_women],
        'סה"כ רכבים': [total_vehicles]
    })

    if os.path.exists(output_file):
        existing_df = pd.read_excel(output_file)
        result_df = pd.concat([existing_df, result_df])
    
    result_df.to_excel(output_file, index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_excel_processing',
    default_args=default_args,
    description='Process Excel file and save daily totals',
    schedule_interval='20 23 * * *',
)

process_excel_task = PythonOperator(
    task_id='process_excel',
    python_callable=process_excel,
    op_args=['/home/rachel/airflow/dags/male.xlsx', '/home/rachel/airflow/dags/according_day.xlsx'],
    dag=dag,
)

process_excel_task
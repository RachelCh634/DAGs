from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.utils.dates import days_ago
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
}

def update_sums():
    input_file1_path = '/home/rachel/airflow/dags/according_day.xlsx'
    input_file2_path = '/home/rachel/airflow/dags/according_hour.xlsx'
    output_file_path = '/home/rachel/airflow/dags/output.xlsx'

    # קריאת הקבצים
    df1 = pd.read_excel(input_file1_path)
    df2 = pd.read_excel(input_file2_path)

    # סיכום העמודות
    summed_data = {
        'countOfCars': [df1['countOfCars'].sum() + df2['countOfCars'].sum()],
        'countOfFemale': [df1['countOfFemale'].sum() + df2['countOfFemale'].sum()],
        'countOfMale': [df1['countOfMale'].sum() + df2['countOfMale'].sum()]
    }

    # יצירת DataFrame עם הסכומים
    result_df = pd.DataFrame(summed_data)

    # כתיבת התוצאות לקובץ אקסל נוסף (או עדכון הקובץ אם הוא כבר קיים)
    if os.path.exists(output_file_path):
        existing_df = pd.read_excel(output_file_path)
        result_df = existing_df.add(result_df, fill_value=0)
        result_df.to_excel(output_file_path, index=False)
    else:
        result_df.to_excel(output_file_path, index=False)

dag = DAG(
    'update_sums_dag',
    default_args=default_args,
    description='Update sums every minute from two Excel files',
    schedule_interval='*/1 * * * *',
)

t1 = PythonOperator(
    task_id='update_sums_task',
    python_callable=update_sums,
    dag=dag,
)

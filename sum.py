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
    input_file_path = '/home/rachel/airflow/dags/hours.xlsx'
    output_file_path = '/home/rachel/airflow/dags/output.xlsx'
    
    df = pd.read_excel(input_file_path)
    last_row = df.iloc[-1] 
    
    new_data = {
        'countOfCars': last_row['countOfCars'],
        'countOfFemale': last_row['countOfFemale'],
        'countOfMale': last_row['countOfMale']
    }

    if os.path.exists(output_file_path):
        existing_df = pd.read_excel(output_file_path)
        existing_df['countOfCars'][0] += new_data['countOfCars']
        existing_df['countOfFemale'][0] += new_data['countOfFemale']
        existing_df['countOfMale'][0] += new_data['countOfMale']
        existing_df.to_excel(output_file_path, index=False)
    else:
        result_df = pd.DataFrame([new_data])
        result_df.to_excel(output_file_path, index=False)

dag = DAG(
    'update_sum_dag',
    default_args=default_args,
    description='Update sums every hour from the last row of the hours Excel file',
    schedule_interval='0 * * * *', 
)
t1 = PythonOperator(
    task_id='update_sums_task',
    python_callable=update_sums,
    dag=dag,
)

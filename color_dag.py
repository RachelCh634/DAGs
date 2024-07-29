from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
import pandas as pd

def update_excel():
    # הגדרת הנתיב לקובץ האקסל
    file_path = "/home/rachel/airflow/dags/colors.xlsx"

    # בדיקה אם הקובץ קיים
    if not os.path.exists(file_path):
        # יצירת DataFrame חדש עם העמודות והערכים ההתחלתיים
        data = {
            "צבע": ["אדום", "כחול", "צהוב", "ירוק", "שחור"],
            "גבר": [0] * 5,
            "אישה": [0] * 5,
            "רכב": [0] * 5
        }
        excel_df = pd.DataFrame(data)
        excel_df.to_excel(file_path, index=False)
    else:
        # קריאת קובץ האקסל הקיים
        excel_df = pd.read_excel(file_path)

    # יצירת חיבור ל-Spark
    spark = SparkSession.builder \
        .config("spark.jars", "file:///mnt/c/spark-3.5.1-bin-hadoop3/jars/postgresql-42.6.2.jar")\
        .appName("PostgreSQL Example") \
        .getOrCreate()

    # פרטי החיבור ל-PostgreSQL
    db_user = "your_username"
    db_password = "your_password"
    jdbc_url = "jdbc:postgresql://localhost:5432/your_database"

    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    # קריאת טבלה בשם 'people' ו-'cars' מ-PostgreSQL
    people_df = spark.read.jdbc(url=jdbc_url, table="people", properties=properties)
    cars_df = spark.read.jdbc(url=jdbc_url, table="cars", properties=properties)

    # המרה של ה-DataFrame של Spark ל-Pandas DataFrame
    people_pd = people_df.toPandas()
    cars_pd = cars_df.toPandas()

    # הגדרת ה-Index ל-DataFrame כך שיהיה לפי צבע
    excel_df.set_index("צבע", inplace=True)

    # עדכון הכמויות לפי הנתונים מ-people
    for _, row in people_pd.iterrows():
        gender = row['gender']  # החליפי לשם העמודה האמיתית של המין בטבלת people
        shirt_color = row['shirt_color']  # החליפי לשם העמודה האמיתית של צבע החולצה בטבלת people

        if gender in excel_df.columns and shirt_color in excel_df.index:
            excel_df.at[shirt_color, gender] += 1

    # עדכון הכמויות לפי הנתונים מ-cars
    for _, row in cars_pd.iterrows():
        shirt_color = row['color']  # החליפי לשם העמודה האמיתית של צבע החולצה בטבלת cars

        if 'רכב' in excel_df.columns and shirt_color in excel_df.index:
            excel_df.at[shirt_color, 'רכב'] += 1

    # שמירת הקובץ המעודכן
    excel_df.to_excel(file_path, index=True)

# הגדרת ה-DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'update_excel_dag',
    default_args=default_args,
    description='A simple DAG to update an Excel file every minute',
    schedule_interval='*/1 * * * *',  # כל דקה
)

# הגדרת הטאסק
update_excel_task = PythonOperator(
    task_id='update_excel',
    python_callable=update_excel,
    dag=dag,
)

update_excel_task

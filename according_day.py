from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

jdbc_url = Variable.get("jdbc_url")
db_user = Variable.get("db_user")
db_password = Variable.get("db_password")

dag = DAG(
    'daily_count_dag',
    default_args=default_args,
    description='A DAG to update Excel file daily at 23:30',
    schedule_interval='30 23 * * *',  # פועל בכל יום ב-23:30
    start_date=datetime(2023, 7, 1),
    catchup=False,
)

def get_male_count():
    try:
        spark = SparkSession.builder \
            .config("spark.jars", "file:///mnt/c/spark-3.5.1-bin-hadoop3/jars/postgresql-42.6.2.jar")\
            .appName("PostgreSQL Example") \
            .getOrCreate()

        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        ref_var_value = -1

        sql_command = f"CALL get_male_count_according_day({ref_var_value})"

        connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            jdbc_url, properties['user'], properties['password']
        )
        statement = connection.createStatement()
        result_set = statement.executeQuery(sql_command)

        result = []
        while result_set.next():
            result.append(result_set.getInt(1))

    except Exception as e:
        print(f"Error: {e}")
        result = None

    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
        if 'spark' in locals():
            spark.stop()

    return result[0] if result else 0

def get_cars_count():
    try:
        spark = SparkSession.builder \
            .config("spark.jars", "file:///mnt/c/spark-3.5.1-bin-hadoop3/jars/postgresql-42.6.2.jar")\
            .appName("PostgreSQL Example") \
            .getOrCreate()

        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        ref_var_value = -1

        sql_command = f"CALL get_count_according_day_car({ref_var_value})"

        connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            jdbc_url, properties['user'], properties['password']
        )
        statement = connection.createStatement()
        result_set = statement.executeQuery(sql_command)

        result = []
        while result_set.next():
            result.append(result_set.getInt(1))

    except Exception as e:
        print(f"Error: {e}")
        result = None

    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
        if 'spark' in locals():
            spark.stop()

    return result[0] if result else 0

def get_female_count():
    try:
        spark = SparkSession.builder \
            .config("spark.jars", "file:///mnt/c/spark-3.5.1-bin-hadoop3/jars/postgresql-42.6.2.jar")\
            .appName("PostgreSQL Example") \
            .getOrCreate()

        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        ref_var_value = -1

        sql_command = f"CALL get_female_count_according_day({ref_var_value})"

        connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            jdbc_url, properties['user'], properties['password']
        )
        statement = connection.createStatement()
        result_set = statement.executeQuery(sql_command)

        result = []
        while result_set.next():
            result.append(result_set.getInt(1))

    except Exception as e:
        print(f"Error: {e}")
        result = None

    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
        if 'spark' in locals():
            spark.stop()

    return result[0] if result else 0

def update_excel():
    excel_file = '/home/rachel/airflow/dags/daily_counts.xlsx'
    current_time = datetime.now().replace(second=0, microsecond=0)
    current_date_str = current_time.strftime('%Y-%m-%d')
    cars_count = get_cars_count()
    female_count = get_female_count()
    male_count = get_male_count()

    new_data = pd.DataFrame({
        'date': [current_date_str],
        'countOfCars': [cars_count],
        'countOfFemale': [female_count],
        'countOfMale': [male_count],
    })

    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        df = pd.concat([df, new_data], ignore_index=True)
    except FileNotFoundError:
        df = new_data

    df.to_excel(excel_file, index=False, engine='openpyxl')

update_excel_task = PythonOperator(
    task_id='update_excel',
    python_callable=update_excel,
    dag=dag,
)

update_excel_task

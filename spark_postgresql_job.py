from pyspark.sql import SparkSession

def run_spark_postgresql_job():
    spark = SparkSession.builder \
        .config("spark.jars", "C:\\spark-3.5.1-bin-hadoop3\\jars\\postgresql-42.6.2.jar") \
        .appName("PostgreSQL Example") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://34.134.106.171:5432/postgres"
    properties = {
        "user": "postgres",
        "password": "practicumgpc2024",
        "driver": "org.postgresql.Driver"
    }
    print("hello")
    ref_var_value = -1

    try:
        sql_command = f"CALL get_male_count({ref_var_value})"

        connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbc_url, properties['user'], properties['password'])
        statement = connection.createStatement()
        result_set = statement.executeQuery(sql_command)
        result = []
        while result_set.next():
            result.append(result_set.getInt(1))

        if result:
            print(f"Result: {result[0]}")
        else:
            print("No result returned")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if connection:
            connection.close()
        spark.stop()

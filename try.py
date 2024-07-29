from pyspark.sql import SparkSession
import pandas as pd

# יצירת SparkSession
spark = SparkSession.builder \
    .config("spark.jars", r"C:\\spark-3.5.1-bin-hadoop3\\jars\\postgresql-42.6.2.jar") \
    .appName("PostgreSQL Example") \
    .getOrCreate()

# פרטי החיבור ל-DB
jdbc_url = "jdbc:postgresql://34.134.106.171:5432/postgres?socketTimeout=30"
properties = {
    "user": "postgres",
    "password": "practicumgpc2024",
    "driver": "org.postgresql.Driver"
}

# קריאת נתונים מה-DB
df = spark.read.jdbc(url=jdbc_url, table="cars", properties=properties)

# המרת הנתונים ל-DataFrame של Pandas
pandas_df = df.toPandas()

# הדפסת התוצאות כטבלה
print(pandas_df)

# שמירת התוצאות כקובץ CSV (אופציונלי)
pandas_df.to_csv('output.csv', index=False)

print("hello")
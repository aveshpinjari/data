import findspark
findspark.init("C:/spark")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

# ==============================
# STEP 1: Start Spark
# ==============================
spark = SparkSession.builder \
    .appName("Dynamic_ETL") \
    .enableHiveSupport() \
    .getOrCreate()

# ==============================
# STEP 3: Load source CSV data
# ==============================
source_csv_path = "actual.csv"
data_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(source_csv_path)
hive_table = "ingestion_metadata.metadata"
data_df.write.mode("append").insertInto(hive_table)

# ==============================
# STEP 6: Write to MySQL
# ==============================
mysql_url = "jdbc:mysql://10.21.18.51:3306/ingestion_metadata"
mysql_properties = {
    "user": "gold_metadata",
    "password": "Tecblic@987",
    "driver": "com.mysql.cj.jdbc.Driver"
}

mysql_table = "metadata"
data_df.write.jdbc(url=mysql_url, table=mysql_table, mode="append", properties=mysql_properties)

# ==============================
# STEP 7: Stop Spark
# ==============================
spark.stop()
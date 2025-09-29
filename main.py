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
# STEP 2: Load your metadata/schema CSV
# ==============================
schema_csv_path = "C:/path/to/your/schema.csv"
schema_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(schema_csv_path)

# ==============================
# STEP 3: Load source CSV data
# ==============================
source_csv_path = "C:/path/to/your/data.csv"
data_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(source_csv_path)

# ==============================
# STEP 4: Apply column mappings
# ==============================
for row in schema_df.collect():
    target_col = row['TargetColumn']
    source_table = row['Source Table']  # in case of joins in future
    source_col = row['Source Column']
    default_val = row['Default Condition']

    if source_col in data_df.columns:
        # Direct mapping from source CSV
        data_df = data_df.withColumnRenamed(source_col, target_col)
    else:
        # Column missing in source, use default value
        data_df = data_df.withColumn(target_col, lit(default_val))

# Keep only the target columns
target_columns = [row['TargetColumn'] for row in schema_df.collect()]
data_df = data_df.select(*target_columns)

# ==============================
# STEP 5: Write to Hive
# ==============================
hive_table = "your_hive_db.your_table"
data_df.write.mode("append").insertInto(hive_table)

# ==============================
# STEP 6: Write to MySQL
# ==============================
mysql_url = "jdbc:mysql://localhost:3306/your_db"
mysql_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

mysql_table = "your_table"
data_df.write.jdbc(url=mysql_url, table=mysql_table, mode="append", properties=mysql_properties)

# ==============================
# STEP 7: Stop Spark
# ==============================
spark.stop()

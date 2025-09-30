from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import pyspark.sql
from pyspark.sql.functions import col, max as spark_max, lit, current_timestamp, date_format, when
from pathlib import Path
import shutil
# ✅ Create Spark session
spark = SparkSession.builder \
    .appName("Dynamic_ETL") \
    .enableHiveSupport() \
    .config("spark.jars", "/data/jars/mysql-connector-j-8.0.31.jar") \
    .getOrCreate()

# ✅ MySQL connection properties
jdbc_url = "jdbc:mysql://10.21.18.51:3306/ingestion_metadata"
properties = {
    "user": "bronze_metadata",
    "password": "Tecblic@987",
    "driver": "com.mysql.cj.jdbc.Driver"
}
table_name="source_metadata"

# ✅ Read table into DataFrame
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# 🔹 Constants
JAR_PATHS = [
    "/data/jars/mysql-connector-j-8.0.31.jar",
     
]

METADATA_CSV_PATH = r"/home/tecblic/Desktop/Dhruv prajapati/Pyspark/metadata/metadata.csv"
METADATA_PARQUET_PATH = r"/home/tecblic/Desktop/Dhruv prajapati/Pyspark/metadata/metadata.parquet"

# 🔹 Spark initialization
def init_spark():
    existing_jars = [jar for jar in JAR_PATHS if Path(jar).exists()]
    if not existing_jars:
        print("❌ No database JAR files found!")
    spark = (
        SparkSession.builder
        .appName("Multi_DB_ETL")
        .config("spark.jars", ",".join(existing_jars))
        .getOrCreate()
    )
    print(f"✅ SparkSession initialized with {len(existing_jars)} database drivers")
    return spark

# 🔹 Extract table name from CSV filename
def extract_table_name_from_csv(filename):
    """
    Extract table name from CSV filename patterns like:
    HFCKEN_acc_01_gl_accounting_detail_09072025195852.csv -> gl_accounting_detail
    HFCKEN_acc_02_gl_exception_09072025195934.csv -> gl_exception
    HFCKEN_aut_01_authorization_log_09072025224837.csv -> authorization_log
    HFCKEN_aut_02_auth_activity_analysis_09072025200010.csv -> auth_activity_analysis
    """
    try:
        # Remove .csv extension
        name_without_ext = filename.replace('.csv', '')
        
        # Pattern: HFCKEN_xxx_xx_table_name_timestamp
        # We need to extract everything between the last underscore (before timestamp) and the prefix
        
        # Split by underscore
        parts = name_without_ext.split('_')
        
        if len(parts) < 4:
            # Fallback: use filename without extension
            print(f"   ⚠️  Could not parse table name from {filename}, using filename as table name")
            return name_without_ext
        
        # Remove timestamp (last part - should be digits)
        if parts[-1].isdigit():
            parts = parts[:-1]
        
        # Remove prefix parts (HFCKEN_acc_01 or HFCKEN_aut_01, etc.)
        # Usually first 3 parts are prefix
        if len(parts) > 3:
            table_name_parts = parts[3:]  # Take everything after the first 3 parts
            table_name = '_'.join(table_name_parts)
        else:
            # Fallback
            table_name = '_'.join(parts)
        
        print(f"   🏷️  Extracted table name: '{table_name}' from '{filename}'")
        return table_name
        
    except Exception as e:
        print(f"   ❌ Error extracting table name from {filename}: {e}")
        # Fallback: use filename without extension and clean it
        clean_name = filename.replace('.csv', '').replace('-', '_').replace(' ', '_')
        return clean_name

# 🔹 Load metadata
def load_metadata(spark,df):
    try:
        print("🔄 Loading metadata from CSV and converting to Parquet...")
        
        # Read CSV with explicit schema
        # df = spark.read.csv(METADATA_CSV_PATH, header=True, inferSchema=True)
        
        # Ensure parquet directory exists and clean it
        parquet_dir = Path(METADATA_PARQUET_PATH)
        if parquet_dir.exists():
            shutil.rmtree(str(parquet_dir))
              GNU nano 7.2                                                                  read_data.py *                                                                          from pathlib import Path
import shutil
# ✅ Create Spark session
spark = SparkSession.builder \
    .appName("Dynamic_ETL") \
    .enableHiveSupport() \
    .config("spark.jars", "/data/jars/mysql-connector-j-8.0.31.jar") \
    .getOrCreate()

# ✅ MySQL connection properties
jdbc_url = "jdbc:mysql://10.21.18.51:3306/ingestion_metadata"
properties = {
    "user": "bronze_metadata",
    "password": "Tecblic@987",
    "driver": "com.mysql.cj.jdbc.Driver"
}
table_name="source_metadata"

# ✅ Read table into DataFrame
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# 🔹 Constants
JAR_PATHS = [
    "/home/tecblic/Downloads/postgresql-42.6.0(1).jar",  # PostgreSQL
    "/path/to/mysql-connector-java.jar",             # MySQL
    "/path/to/ojdbc8.jar",                           # Oracle
    "/path/to/sqljdbc42.jar"                         # SQL Server
]

METADATA_CSV_PATH =
        # Write to parquet with coalesce to avoid multiple files
        df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(METADATA_PARQUET_PATH)
        
        # Read back with schema to ensure consistency
        metadata_df = spark.read.parquet(METADATA_PARQUET_PATH)
        
        # More flexible active flag filtering - handle TRUE, True, true, 1, etc.
        # Also include rows where active_flag is null/empty but source_type is not null
        active_df = metadata_df.filter(
            (col("active_flag") == True) | 
            (col("active_flag") == "TRUE") | 
            (col("active_flag") == "True") | 
            (col("active_flag") == "true") |
            # Include CSV sources even if active_flag is empty/null
            ((col("source_type") == "csv") & (col("input_path").isNotNull()))
        )
        
        print(f"✅ Loaded {active_df.count()} active metadata records")
        
        # Debug: Show which records are considered active
        if active_df.count() > 0:
            print("🔍 Active records:")
            active_df.select("id", "source_type", "source_name", "input_path", "active_flag").show(truncate=False)
        
        return active_df
    
    except Exception as e:
        print(f"❌ Failed to load metadata: {e}")
        return None

# 🔹 Update metadata with proper file handling
def update_metadata(source_identifier, df,spark, max_val=None,status="Success"):
    try:
        # Check if parquet file exists
        if not Path(METADATA_PARQUET_PATH).exists():
            print(f"   ❌ Metadata parquet file not found: {METADATA_PARQUET_PATH}")
            return False
        
        # Read parquet with explicit error handling
        try:
            df = spark.read.parquet(METADATA_PARQUET_PATH)
        except Exception as read_error:
            print(f"   ❌ Failed to read parquet file: {read_error}")
            # Try to recreate from CSV
            print("   🔄 Attempting to recreate parquet from CSV...")
            csv_df = df
            csv_df.coalesce(1).write.mode("overwrite").parquet(METADATA_PARQUET_PATH)
            df = spark.read.parquet(METADATA_PARQUET_PATH)
        
        # For CSV sources, use input_path as identifier, for DB sources use source_table_name
        condition = (
            (col("input_path") == source_identifier) | 
            (col("source_name") == source_identifier)
        )
        
        # Check if source exists
        if df.filter(condition).count() == 0:
            print(f"   ❌ Source {source_identifier} not found in metadata")
            return False
        
        # Current timestamp and date
        now_ts, now_date = current_timestamp(), date_format(current_timestamp(), "yyyy/MM/dd")
        
        # Update columns
        updated_df = df.withColumn(
            "last_extracted_value",
            when(condition, lit(str(max_val) if max_val else "")).otherwise(col("last_extracted_value"))
        ).withColumn(
            "updated_at",
            when(condition, now_ts).otherwise(col("updated_at"))
        ).withColumn(
            "last_run_date",
            when(condition, now_date).otherwise(col("last_run_date"))
        ).withColumn(
            "last_run_status",
            when(condition, lit(status)).otherwise(col("last_run_status"))
        )
        
        # Create temporary path for atomic write
        temp_path = f"{METADATA_PARQUET_PATH}_temp"
        temp_path_obj = Path(temp_path)
        
        # Clean temp directory if exists
        if Path(temp_path).exists():
            shutil.rmtree(temp_path)
            
        # Write to temporary location first
        updated_df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(temp_path)
        
        # Atomic move (rename temp to final)
        final_path_obj = Path(METADATA_PARQUET_PATH)
        if final_path_obj.exists():
            shutil.rmtree(METADATA_PARQUET_PATH)
            
        temp_path_obj.rename(final_path_obj)
        print(f"   ✅ Metadata updated for {source_identifier} (status: {status})")
        return True
    
    except Exception as e:
        print(f"   ❌ Failed to update metadata for {source_identifier}: {e}")
        return False

# 🔹 Process CSV sources from folder
def process_csv_source(df, spark, row):
    try:
        input_path = row["input_path"]
        output_path = Path(row["output_path"])
        
        # Generate a unique identifier for CSV source (using input_path)
        source_identifier = input_path
        
        # Check if input_path is a file or directory
        input_path_obj = Path(input_path)
        
        if not input_path_obj.exists():
            print(f"   ❌ Input path not found: {input_path}")
            update_metadata(source_identifier,df, spark, status="Failed")
            return False

        csv_files = []
        
        # If it's a directory, get all CSV files from it
        if input_path_obj.is_dir():
            csv_files = list(input_path_obj.glob("*.csv"))
            print(f"   📁 Found {len(csv_files)} CSV files in directory: {input_path}")
        elif input_path_obj.is_file() and input_path.lower().endswith('.csv'):
            csv_files = [input_path_obj]
            print(f"   📄 Processing single CSV file: {input_path}")
        else:
            print(f"   ❌ Input path is neither a CSV file nor a directory: {input_path}")
            update_metadata(source_identifier,df, spark, status="Failed")
            return False
        
        if not csv_files:
            print(f"   ❌ No CSV files found in: {input_path}")
            update_metadata(source_identifier,df, spark, status="No Data")
            return True

        total_processed = 0
        successful_processed = 0

        # Process each CSV file
        for csv_file in csv_files:
            try:
                print(f"\n   📥 Processing CSV file: {csv_file.name}")
                
                # Extract table name from filename
                table_name = extract_table_name_from_csv(csv_file.name)
                
                # Read CSV
                csv_df = spark.read.csv(str(csv_file), header=True, inferSchema=True)
                
                # Count records
                record_count = csv_df.count()
                print(f"   📊 Records found in {csv_file.name}: {record_count}")
                
                if record_count == 0:
                    print(f"   ⚠️  No records found in CSV file: {csv_file.name}")
                    continue

                # Create output directory with extracted table name
                final_output_path = output_path / "Source" / table_name
                final_output_path.mkdir(parents=True, exist_ok=True)
                
                parquet_file = final_output_path / "data.parquet"
                
                # Remove existing parquet file if it exists
                if parquet_file.exists():
                    shutil.rmtree(str(parquet_file))

                # Write to parquet
                csv_df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(str(parquet_file))
                print(f"   ✅ CSV converted to Parquet: {parquet_file}")

                successful_processed += 1
                total_processed += 1

            except Exception as file_error:
                print(f"   ❌ Error processing CSV file {csv_file.name}: {file_error}")
                total_processed += 1
                continue

        # Update metadata based on overall success
        if successful_processed == 0:
            update_metadata(source_identifier,df, spark, status="Failed")
            return False
        elif successful_processed == total_processed:
            update_metadata(source_identifier,df, spark, max_val=successful_processed, status="Success")
            print(f"   🎯 Successfully processed all {successful_processed} CSV files")
            return True
        else:
            update_metadata(source_identifier,df, spark, max_val=successful_processed, status="Partial Success")
            print(f"   ⚠️  Processed {successful_processed}/{total_processed} CSV files successfully")
            return True
    
    except Exception as e:
        print(f"   ❌ Error processing CSV source {input_path}: {e}")
        update_metadata(source_identifier,df, spark, status="Failed")
        return False

# 🔹 Extract & store for database + CSV
def extract_and_store(df ,spark, row):
    try:
        source_type = row["source_type"].lower()
        
        # Handle identifier for both CSV and database sources
        if source_type == "csv":
            source_identifier = row["input_path"]
            # For CSV sources, we'll process multiple files and extract names
            display_name = f"CSV_FOLDER_{Path(row['input_path']).name}"
        else:
            source_identifier = row["source_name"]
            display_name = row["source_name"]
        
        print(f"\n📂 Processing: {display_name} ({source_type})")
        update_metadata(source_identifier,df, spark, status="Running")

        if source_type == "csv":
            return process_csv_source(df,spark, row)
        else:
            jdbc_url = row["jdbc_url"]
            db_props = {"user": row["user_name"], "password": row["password"], "driver": row["driver_class"]}
            load_type = str(row["load_type"]).lower()
            output_path = Path(row["output_path"]) / "Source" / display_name
            incremental_column, last_value, schema_name = row["incremental_column"], row["last_extracted_value"], row["schema_name"]

            # Build query
            if load_type == "incremental" and incremental_column and last_value:
                query = f"(SELECT * FROM {schema_name}.{display_name} WHERE {incremental_column} > '{last_value}') as tmp" if schema_name else f"(SELECT * FROM {display_name} WHERE {incremental_column} > '{last_value}') as tmp"
                print(f"   🔎 Incremental load: {incremental_column} > {last_value}")
            else:
                query = f"(SELECT * FROM {schema_name}.{display_name}) as tmp" if schema_name else f"(SELECT * FROM {display_name}) as tmp"
                print("   📥 Full load")

            # Read from JDBC
            src_df = spark.read.jdbc(jdbc_url, query, properties=db_props)
            count = src_df.count()
            print(f"   📊 Records found: {count}")
            
            if count == 0:
                update_metadata(source_identifier,df, spark, status="No Data")
                return True

            # Create output directory
            output_path.mkdir(parents=True, exist_ok=True)
            parquet_file = output_path / "data.parquet"
            
            # Remove existing parquet file if it exists
            if parquet_file.exists():
                shutil.rmtree(str(parquet_file))
                
            # Write parquet
            src_df.coalesce(1).write.mode("overwrite").parquet(str(parquet_file))
            print(f"   ✅ Data written to {parquet_file}")

            # Update metadata with max incremental column (if applicable)
            if load_type == "incremental" and incremental_column in src_df.columns:
                try:
                    max_val = src_df.agg(spark_max(col(incremental_column))).collect()[0][0]
                    update_metadata(source_identifier,df, spark, max_val=max_val, status="Success")
                except Exception as max_error:
                    print(f"   ⚠️  Could not get max value for {incremental_column}: {max_error}")
                    update_metadata(source_identifier,df, spark, status="Success")
            else:
                update_metadata(source_identifier,df, spark, status="Success")
                
            return True
        
    except Exception as e:
        source_identifier = row.get("input_path", row.get("source_name", "Unknown"))
        print(f"   ❌ Error processing {source_identifier}: {e}")
        update_metadata(source_identifier,df, spark, status="Failed")
        return False

# 🔹 Main pipeline
def main():
    print("🚀 Starting Multi-Database ETL Pipeline...")
    spark = init_spark()
    df=None
    try:
        metadata = load_metadata(spark,df)
        if not metadata:
            print("❌ No active metadata found. Exiting...")
            return
        
        success_count, total_count = 0, metadata.count()
        
        print(f"\n📋 Processing {total_count} sources...")
        for row in metadata.collect():
            if extract_and_store(df,spark, row.asDict()):
                success_count += 1
                
        print(f"\n🎯 Pipeline Complete: {success_count}/{total_count} sources processed successfully")
    
    finally:
        spark.stop()
        print("🛑 SparkSession stopped")

if __name__ == "__main__":
    main()


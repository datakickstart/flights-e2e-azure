# Databricks notebook source
# MAGIC %md
# MAGIC # Stream processing Raw and Bronze steps with Liquid Clustering
# MAGIC Notebook to demonstrate main streaming pipeline to a liquid cluster table.
# MAGIC Instructions:
# MAGIC Set run_raw to "true" if the raw table needs data appended from the static databricks-datasets source.
# MAGIC Set run_bronze to "true" if the bronze table should be incrementally loaded. This should typically be true unless you are doing a special run.
# MAGIC Set tail_num_prefix to a string like "A", "B", etc to provide a modified field when reloading raw data.
# MAGIC
# MAGIC raw_checkpoint_version can be incremented to forget history and reload data to increase data size.
# MAGIC bronze_checkpoint_version can be incremented to forget history and reload ata to increase data size.  
# MAGIC
# MAGIC ### Args:
# MAGIC ```
# MAGIC   run_bronze = true/false. 
# MAGIC   run_raw = true/false. 
# MAGIC   tail_num_prefix = < A|B|C|etc >
# MAGIC ```
# MAGIC

# COMMAND ----------

dbutils.widgets.dropdown('demo_options', 'uniform', ['uniform', 'liquid'])
dbutils.widgets.text('catalog', 'main')
dbutils.widgets.text('schema', 'flights_dev')

dbutils.widgets.dropdown('run_raw', 'true', ['true', 'false'])
dbutils.widgets.dropdown('run_bronze', 'true', ['true', 'false'])
dbutils.widgets.text('tail_num_prefix', '')

# COMMAND ----------

demo_type = dbutils.widgets.get('demo_options')
catalog = dbutils.widgets.get('catalog')
database = dbutils.widgets.get('schema')

run_raw = dbutils.widgets.get('run_raw')
tail_num_prefix = dbutils.widgets.get('tail_num_prefix')
run_bronze = dbutils.widgets.get('run_bronze')

TABLE_NAME = f"{catalog}.{database}.flights_raw"
BRONZE_TABLE_NAME = f"{catalog}.{database}.flights_bronze_{demo_type}3"

raw_checkpoint_version = "1.3"
bronze_checkpoint_version = "1.3"
silver_checkpoint_version = "1.3"

# COMMAND ----------

# DBTITLE 1,Get schema and max_year
from pyspark.sql.functions import col, lit, concat, trim, concat_ws

df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/databricks-datasets/airlines/part-00000").limit(100)

schema = df.schema

# COMMAND ----------

print(schema.simpleString())

# COMMAND ----------

# DBTITLE 1,PySpark Streaming Read
if run_raw == 'true':
    # Create a PySpark streaming read using autoloader

    QUERY_NAME = "airlines_ingest"

    # Modify maxBytesPerTrigger to work with larger batchers (faster full load) or smaller batches (slower full load)
    streaming_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.includeExistingFiles", "true") \
        .schema(schema) \
        .option("cloudFiles.maxBytesPerTrigger", "512m") \
        .load("/databricks-datasets/airlines")
        # .option("cloudFiles.maxFilesPerTrigger", 1)

    if tail_num_prefix.lstrip() != '':
       streaming_df = streaming_df.withColumn('TailNum', concat_ws('-',lit(tail_num_prefix), col('TailNum'))) 


# COMMAND ----------

## RESET RAW CHECKPOINT by deleting directory
# dbutils.fs.rm(f"/Volumes/{catalog}/{database}/flights_checkpoints/{demo_type}_checkpoint{raw_checkpoint_version}", True)

# COMMAND ----------

# spark.sql(f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")

# COMMAND ----------

if run_raw == 'true':
    QUERY_NAME = "airlines_ingest_raw"
    query = (
      streaming_df
      .selectExpr("*",  "CAST(current_timestamp() AS TIMESTAMP) as created_timestamp_default", "CAST(current_timestamp() AS TIMESTAMP_NTZ) as created_timestamp_ntz", "CAST(current_timestamp() AS TIMESTAMP_LTZ) as created_timestamp_ltz")
      .writeStream
        .queryName(QUERY_NAME)
        .option("checkpointLocation",f"/Volumes/{catalog}/{database}/flights_checkpoints/{demo_type}_checkpoint{raw_checkpoint_version}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(TABLE_NAME)
   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Raw to Bronze

# COMMAND ----------

# Create a PySpark streaming read using autoloader
from pyspark.sql.functions import col

if run_bronze == 'true':
    # Get Schema
    schema = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/databricks-datasets/airlines/part-00000").limit(100).schema

    QUERY_NAME = f"airlines_ingest_{demo_type}_bronze"

    # Modify maxBytesPerTrigger to work with larger batchers (faster full load) or smaller batches (slower full load)
    streaming_df = spark.readStream.format("delta") \
        .schema(schema) \
        .option("maxBytesPerTrigger", "512m") \
        .table(TABLE_NAME)

    # .option("maxFilesPerTrigger", 1) \

# COMMAND ----------

from pyspark.sql.functions import struct, array, to_json, col, replace, lit, expr, current_timestamp, lpad, when, right, left, substr, to_timestamp
from pyspark.sql import functions as F

if run_bronze == 'true':
  # spark.conf.set('spark.sql.parquet.compression.codec', 'zstd')

  if demo_type == "liquid":
    spark.sql(f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME} (`timestamp` timestamp, event_date date, UniqueCarrier string, DepTime string, FlightNum int, times struct<CRSArrTime:string,ArrTime:string,DepTime:string,TailNum:string, timeArray:array<string>>, payload string, created_timestamp timestamp) TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'=true, 'delta.checkpoint.writeStatsAsStruct'=true, 'schema.autoMerge.enabled'= true, 'delta.deletedFileRetentionDuration'='3 days', 'delta.logRetentionDuration'='3 days', 'delta.targetFileSize'='128000000', 'compression'='zstd') CLUSTER BY (timestamp, UniqueCarrier, FlightNum)")
   
    # 'delta.targetFileSize'='1GB',
    # event_date date GENERATED ALWAYS AS (cast(`timestamp` as date))
  
  elif demo_type == "uniform":
    spark.sql(f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME} (`timestamp` timestamp, event_date date, UniqueCarrier string, DepTime string, FlightNum int, times struct<CRSArrTime:string,ArrTime:string,DepTime:string,TailNum:string, timeArray:array<string>>, payload string, created_timestamp timestamp, created_timestamp_default timestamp, created_timestamp_ntz TIMESTAMP_NTZ, created_timestamp_ltz TIMESTAMP) TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true','delta.universalFormat.enabledFormats' = 'iceberg', 'delta.enableDeletionVectors' = 'false', 'delta.enableChangeDataFeed' = true, 'delta.autoOptimize.optimizeWrite'=true, 'delta.autoOptimize.autoCompact'=true, 'schema.autoMerge.enabled'= true, 'delta.feature.timestampNtz' = 'supported', 'compression'='zstd')")

    # Optional TBLPROPERTIES to experiment with
    # 'delta.deletedFileRetentionDuration'='3 days', 'delta.logRetentionDuration'='3 days', 'delta.targetFileSize'='128000000'

  query = (
    streaming_df
      .withColumn("CRSArrTime", replace("CRSArrTime", lit("NA")).cast("string"))
      .withColumn("DepTime", lpad("DepTime",4, "0"))
      .withColumn("date_str", expr("concat(cast(Year as string),'-',lpad(cast(Month as string),2,'0'),'-',lpad(cast(DayOfMonth as string),2,'0'))"))
      .withColumn("timestamp_str", 
                  concat(left("CRSDepTime",lit('2')), lit(':'), right("CRSDepTime",lit('2'))))
      .withColumn("timestamp", expr("try_to_timestamp(concat(date_str, ' ', timestamp_str))"))
      .withColumn("event_date", col("timestamp").cast("date"))
      .withColumn("created_timestamp", current_timestamp())
      .withColumn("times", struct("CRSArrTime", "ArrTime", "DepTime", array("CRSArrTime", "ArrTime", "DepTime").alias("timeArray")))
      .select("timestamp","event_date", "UniqueCarrier", "DepTime", "FlightNum", "times",
              to_json(struct("*")).alias("payload"), 
              "created_timestamp",
              "created_timestamp_default",
              "created_timestamp_ntz",
              "created_timestamp_ltz")
      #struct("*").alias("payload_struct"), 
    .writeStream
      .queryName(QUERY_NAME)
.option("checkpointLocation",f"/Volumes/{catalog}/{database}/flights_checkpoints/{demo_type}_checkpoint_bronze{bronze_checkpoint_version}")
      .option("mergeSchema", "true")
      # .trigger(processingTime='20 seconds')
      .trigger(availableNow=True)
      .toTable(BRONZE_TABLE_NAME)
  )

# COMMAND ----------

display(spark.sql(f"SELECT count(1), max(created_timestamp_ltz) t_tz, max(created_timestamp_ntz) t_ntz from {BRONZE_TABLE_NAME}"))
# 882745645	2025-01-09T06:01:01.000+00:00	2025-01-08T22:01:01.000


# COMMAND ----------

# TMP
BRONZE_TABLE_NAME2 = f"{catalog}.{database}.flights_bronze_{demo_type}2"
QUERY_NAME2 = f"airlines_ingest_{demo_type}_bronze2"

(streaming_df
      .withColumn("CRSArrTime", replace("CRSArrTime", lit("NA")).cast("string"))
      .withColumn("DepTime", lpad("DepTime",4, "0"))
      .withColumn("date_str", expr("concat(cast(Year as string),'-',lpad(cast(Month as string),2,'0'),'-',lpad(cast(DayOfMonth as string),2,'0'))"))
      .withColumn("timestamp_str", 
                  concat(left("CRSDepTime",lit('2')), lit(':'), right("CRSDepTime",lit('2'))))
      .withColumn("timestamp", expr("cast(concat(date_str, ' ', timestamp_str) as timestamp_ntz)"))
      .withColumn("event_date", col("timestamp").cast("date"))
      .withColumn("created_timestamp", current_timestamp())
      .withColumn("times", struct("CRSArrTime", "ArrTime", "DepTime", array("CRSArrTime", "ArrTime", "DepTime").alias("timeArray")))
      .select("timestamp","event_date", "UniqueCarrier", "DepTime", "FlightNum", "times",
              to_json(struct("*")).alias("payload"), 
              "created_timestamp")
      #struct("*").alias("payload_struct"), 
    .writeStream
      .queryName(QUERY_NAME2)
.option("checkpointLocation",f"/Volumes/{catalog}/{database}/flights_checkpoints/{demo_type}_checkpoint_bronze{bronze_checkpoint_version}_2")
      .option("mergeSchema", "true")
      # .trigger(processingTime='20 seconds')
      .trigger(availableNow=True)
      .toTable(BRONZE_TABLE_NAME2)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance and Metadata info (Optional to run)
# MAGIC Check out [Delta File Checks/FileMetadata_and_DeltaLog_testing](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/4224924687404886/command/4224924687406416) notebook for things attempted to evaluate table layout and improve performance.

# COMMAND ----------

# spark.conf.set('custom_vars.table', BRONZE_TABLE_NAME)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE WIDGET TEXT demo_type_name DEFAULT "uniform";
# MAGIC -- DECLARE demo_type STRING DEFAULT 'uniform';
# MAGIC -- SET demo_type = :demo_type_name;
# MAGIC -- REMOVE WIDGET demo_type_name;

# COMMAND ----------

# %sql DESCRIBE EXTENDED ${custom_vars.table}

# COMMAND ----------

# %sql VACUUM ${custom_vars.table} RETAIN 72 HOURS;

# COMMAND ----------

# DBTITLE 1,Optimize Custom Vars Table
# %sql
# OPTIMIZE ${custom_vars.table};

# COMMAND ----------

# DBTITLE 1,Describe Bronze Flights History
# %sql
# DESCRIBE HISTORY ${custom_vars.table}

# COMMAND ----------

# DBTITLE 1,Operation Metrics SELECT Query
# %sql
# Select operationMetrics.numAddedFiles, operationMetrics.numOutputBytes/1024/1024, *
# from 
# (DESCRIBE HISTORY ${custom_vars.table})

# COMMAND ----------

# DBTITLE 1,Unique Carriers and Flight Numbers Count
# MAGIC %sql
# MAGIC -- SELECT SUM(n)
# MAGIC -- FROM
# MAGIC -- (
# MAGIC --   SELECT UniqueCarrier, FlightNum, 1 as n
# MAGIC --   FROM ${custom_vars.table}
# MAGIC --   GROUP BY UniqueCarrier, FlightNum
# MAGIC -- )
# MAGIC
# MAGIC -- SELECT COUNT(DISTINCT UniqueCarrier) carries, COUNT(DISTINCT FlightNum) flights
# MAGIC -- FROM ${custom_vars.table}

# COMMAND ----------

# DBTITLE 1,File Count and Size Analysis
# %sql
# select approx_count_distinct(_metadata.file_name) file_count, avg(_metadata.file_size)/1024/1024 file_size_mb, any_value(_metadata.file_name) random_filename
# from ${custom_vars.table}

# COMMAND ----------

# DBTITLE 1,Flights File Size
# %sql
# select _metadata.file_name, _metadata.file_size/1024/1024 file_size_mb
# from ${custom_vars.table}

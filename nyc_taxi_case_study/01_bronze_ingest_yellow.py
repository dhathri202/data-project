# Databricks notebook source
# MAGIC %fs ls 'dbfs:/FileStore/dhathri/nyc'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/dhathri/nyc/yellow_data/'

# COMMAND ----------

# MAGIC %md
# MAGIC use catalog hive_metastore

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/dhathri/nyc/yellow_data/checkpoint', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REPLACE TEMPORARY VIEW yellowtrip_bronze_temp AS (
# MAGIC   SELECT * FROM yellowtrip_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

checkpoint_path = "dbfs:/FileStore/dhathri/checkpoints/yellow_bronze"
schema_path = "dbfs:/FileStore/dhathri/schemas/yellow_bronze"

# COMMAND ----------

df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaHints", "trip_distance float")
    .option("cloudFiles.schemaLocation", schema_path)
    .load("dbfs:/FileStore/dhathri/nyc/yellow_data/"))

df_stream.createOrReplaceTempView("yellowtrip_bronze_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yellowtrip_bronze_temp limit 10

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

(spark.table("yellowtrip_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_path)
      .outputMode("append")
      .table("yellowtrip_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yellowtrip_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yellowtrip_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended yellowtrip_bronze
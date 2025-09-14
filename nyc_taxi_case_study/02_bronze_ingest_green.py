# Databricks notebook source
# MAGIC %fs ls 'dbfs:/FileStore/dhathri/nyc/green_data/'

# COMMAND ----------

green_checkpoint_path = "dbfs:/FileStore/dhathri/checkpoints/green_bronze"
green_schema_path = "dbfs:/FileStore/dhathri/schemas/green_bronze"


# COMMAND ----------

df_green_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaHints", "trip_distance float")
    .option("cloudFiles.schemaLocation", green_schema_path)
    .option("header", "true")
    .load("dbfs:/FileStore/dhathri/nyc/green_data/"))

# COMMAND ----------

df_green_stream.createOrReplaceTempView("greentrip_bronze_temp")

# COMMAND ----------

# MAGIC %sql select * from greentrip_bronze_temp limit 5

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

(spark.table("greentrip_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", green_checkpoint_path)
      .outputMode("append")
      .table("greentrip_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from greentrip_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC injest greentrip_june.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from greentrip_bronze
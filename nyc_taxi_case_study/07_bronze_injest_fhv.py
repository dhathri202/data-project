# Databricks notebook source
# MAGIC %fs ls 'dbfs:/FileStore/dhathri/nyc/fhv/fhv_csv'

# COMMAND ----------

fhv_checkpoint_path = "dbfs:/FileStore/dhathri/checkpoints/fhv_bronze"
fhv_schema_path = "dbfs:/FileStore/dhathri/schemas/fhv_bronze"

# COMMAND ----------

df_fhv_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaHints", """
        dispatching_base_num string,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        PULocationID int,
        DOLocationID int,
        SR_Flag string
    """)
    .option("cloudFiles.schemaLocation", fhv_schema_path)
    .option("header", "true")
    .load("dbfs:/FileStore/dhathri/nyc/fhv/fhv_csv")
)

# COMMAND ----------

df_fhv_stream.createOrReplaceTempView("fhv_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fhv_temp_view limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

(spark.table("fhv_temp_view")
      .writeStream
      .format("delta")
      .option("checkpointLocation", fhv_checkpoint_path)
      .outputMode("append")
      .table("fhv_trip_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fhv_trip_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC injest one more fhv_trip_csv file & check count

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/dhathri/nyc/fhv/fhv_csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fhv_trip_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fhv_trip_bronze limit 5
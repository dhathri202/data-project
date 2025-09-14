# Databricks notebook source
# MAGIC %md
# MAGIC Rename the green_bronze columns & yellow-bronze columns to a common column name.

# COMMAND ----------

# MAGIC %md
# MAGIC DROP VIEW IF EXISTS combined_trip_data;
# MAGIC DROP VIEW IF EXISTS combined_trip_with_zones;

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

(spark.readStream
  .table("yellowtrip_bronze")
  .createOrReplaceTempView("yellowtrip_bronze_tmp"))

(spark.readStream
  .table("greentrip_bronze")
  .createOrReplaceTempView("greentrip_bronze_tmp"))

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE global TEMP VIEW combined_trip_data AS
# MAGIC SELECT
# MAGIC     VendorID,
# MAGIC     tpep_pickup_datetime AS pickup_datetime,
# MAGIC     tpep_dropoff_datetime AS dropoff_datetime,
# MAGIC     passenger_count,
# MAGIC     trip_distance,
# MAGIC     RatecodeID,
# MAGIC     store_and_fwd_flag,
# MAGIC     PULocationID,
# MAGIC     DOLocationID,
# MAGIC     payment_type,
# MAGIC     fare_amount,
# MAGIC     extra,
# MAGIC     mta_tax,
# MAGIC     tip_amount,
# MAGIC     tolls_amount,
# MAGIC     improvement_surcharge,
# MAGIC     total_amount,
# MAGIC     congestion_surcharge,
# MAGIC     'yellow' as trip_type
# MAGIC FROM yellowtrip_bronze_tmp
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     VendorID,
# MAGIC     lpep_pickup_datetime AS pickup_datetime,
# MAGIC     lpep_dropoff_datetime AS dropoff_datetime,
# MAGIC     passenger_count,
# MAGIC     trip_distance,
# MAGIC     RatecodeID,
# MAGIC     store_and_fwd_flag,
# MAGIC     PULocationID,
# MAGIC     DOLocationID,
# MAGIC     payment_type,
# MAGIC     fare_amount,
# MAGIC     extra,
# MAGIC     mta_tax,
# MAGIC     tip_amount,
# MAGIC     tolls_amount,
# MAGIC     improvement_surcharge,
# MAGIC     total_amount,
# MAGIC     congestion_surcharge,
# MAGIC     'green' as trip_type
# MAGIC FROM greentrip_bronze_tmp;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended combined_trip_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_type, COUNT(*) 
# MAGIC FROM combined_trip_data 
# MAGIC GROUP BY trip_type
# MAGIC ORDER BY COUNT(*) DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from combined_trip_data limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC Now filter and join with taxi_zone

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REPLACE TEMP VIEW combined_trip_with_zones AS
# MAGIC SELECT
# MAGIC     c.VendorID,
# MAGIC     c.DOLocationID,
# MAGIC     CAST(c.passenger_count AS INT) AS passenger_count,
# MAGIC     c.trip_distance,
# MAGIC     CAST(c.total_amount AS DOUBLE) AS total_amount,
# MAGIC     t.borough,
# MAGIC     t.zone,
# MAGIC     t.service_zone
# MAGIC FROM combined_trip_data c
# MAGIC JOIN taxi_zone_view t
# MAGIC     ON c.DOLocationID = t.LocationID
# MAGIC WHERE c.pickup_datetime != c.dropoff_datetime
# MAGIC   AND CAST(c.passenger_count AS INT) != 0
# MAGIC   AND c.trip_distance != 0.0
# MAGIC   AND CAST(c.total_amount AS DOUBLE) != 0.0;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW combined_trip_with_zones AS
# MAGIC SELECT
# MAGIC     c.VendorID,
# MAGIC     c.PULocationID,
# MAGIC     CAST(c.pickup_datetime AS TIMESTAMP) AS pickup_datetime,
# MAGIC     CAST(c.dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
# MAGIC     pz.borough AS pickup_borough,
# MAGIC     pz.zone AS pickup_zone,
# MAGIC     pz.service_zone AS pickup_service_zone,
# MAGIC     c.DOLocationID,
# MAGIC     dz.borough AS dropoff_borough,
# MAGIC     dz.zone AS dropoff_zone,
# MAGIC     dz.service_zone AS dropoff_service_zone,
# MAGIC     CAST(c.passenger_count AS INT) AS passenger_count,
# MAGIC     c.trip_distance,
# MAGIC     CAST(c.total_amount AS DOUBLE) AS total_amount,
# MAGIC     c.trip_type
# MAGIC FROM combined_trip_data c
# MAGIC JOIN global_temp.taxi_zone_view pz ON c.PULocationID = pz.LocationID
# MAGIC JOIN global_temp.taxi_zone_view dz ON c.DOLocationID = dz.LocationID
# MAGIC WHERE
# MAGIC     CAST(c.pickup_datetime AS TIMESTAMP) != CAST(c.dropoff_datetime AS TIMESTAMP)
# MAGIC     AND CAST(c.passenger_count AS INT) != 0
# MAGIC     AND c.trip_distance != 0.0
# MAGIC     AND CAST(c.total_amount AS DOUBLE) != 0.0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_type FROM global_temp.combined_trip_with_zones LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_type, COUNT(*) 
# MAGIC FROM global_temp.combined_trip_with_zones 
# MAGIC GROUP BY trip_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.combined_trip_with_zones limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC describe global_temp.combined_trip_with_zones

# COMMAND ----------

# MAGIC %md
# MAGIC convert this to silver table-write to table

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/dhathri/checkpoints/combined_trip_zones_silver", recurse=True)

# COMMAND ----------

checkpoint_path = "dbfs:/FileStore/dhathri/checkpoints/combined_trip_zones_silver"

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/dhathri/checkpoints/")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists combined_trip_zones_silver
# MAGIC

# COMMAND ----------

(spark.table("global_temp.combined_trip_with_zones")
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .outputMode("append")
  .table("combined_trip_zones_silver"))


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from combined_trip_zones_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select trip_type from combined_trip_zones_silver limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from combined_trip_zones_silver limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT
# MAGIC     VendorID,
# MAGIC     CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
# MAGIC     CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
# MAGIC     CAST(passenger_count AS INT) AS passenger_count,
# MAGIC     trip_distance,
# MAGIC     RatecodeID,
# MAGIC     PULocationID,
# MAGIC     DOLocationID,
# MAGIC     payment_type,
# MAGIC     fare_amount,
# MAGIC     tip_amount,
# MAGIC     total_amount
# MAGIC FROM combined_trip_data c 
# MAGIC join taxi_zone_view t
# MAGIC WHERE passenger_count != 0
# MAGIC   AND trip_distance != 0.0
# MAGIC   AND total_amount != 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC join on both pickup and drop off location id- to make it consisting of both pu & do data.

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore
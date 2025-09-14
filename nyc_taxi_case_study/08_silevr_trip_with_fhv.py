# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fhv_base_json_view limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fhv_trip_bronze limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC # Steps:
# MAGIC 1. combine fhv_trip_bronze & taxizone => **fhv_trip_zone**
# MAGIC 2. fhv_trip_zone + fhv_json => **fhv_combined**
# MAGIC 3. combine fhv_combined & combined_trip_zones_silver => **all_trips_silver**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1:
# MAGIC **combine fhv_trip_bronze & taxizone => fhv_trip_zone**

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

(spark.readStream
  .table("fhv_trip_bronze")
  .createOrReplaceTempView("fhv_trip_bronze_tmp"))

# COMMAND ----------

(spark.readStream
  .table("combined_trip_zones_silver")
  .createOrReplaceTempView("combined_trip_zones_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view fhv_trip_with_zones as select
# MAGIC   f.dispatching_base_num ,
# MAGIC   f.pickup_datetime,
# MAGIC   f.dropoff_datetime,
# MAGIC   f.PULocationID,
# MAGIC   pu.Borough AS pickup_borough,
# MAGIC   pu.Zone AS pickup_zone,
# MAGIC   pu.service_zone AS pickup_service_zone,
# MAGIC   f.DOLocationID,
# MAGIC   do.Borough AS dropoff_borough,
# MAGIC   do.Zone AS dropoff_zone,
# MAGIC   do.service_zone AS dropoff_service_zone,
# MAGIC   NULL AS VendorID,
# MAGIC   NULL AS passenger_count,
# MAGIC   NULL AS trip_distance,
# MAGIC   NULL AS total_amount,
# MAGIC   'fhv' AS trip_type
# MAGIC FROM fhv_trip_bronze_tmp f
# MAGIC LEFT JOIN global_temp.taxi_zone_view pu ON f.PULocationID = pu.LocationID
# MAGIC LEFT JOIN global_temp.taxi_zone_view do ON f.DOLocationID = do.LocationID
# MAGIC WHERE f.PULocationID IS NOT NULL AND f.DOLocationID IS NOT NULL
# MAGIC   AND f.pickup_datetime != f.dropoff_datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fhv_trip_with_zones limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LocationID
# MAGIC FROM global_temp.taxi_zone_view
# MAGIC WHERE LocationID IN (264, 265);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LocationID, borough, zone, service_zone
# MAGIC FROM global_temp.taxi_zone_view
# MAGIC WHERE LocationID IN (264, 265);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2:
# MAGIC **fhv_trip_zone + fhv_json => fhv_combined**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW fhv_combined AS
# MAGIC SELECT
# MAGIC     f.*,
# MAGIC     j.entity_name AS base_name,
# MAGIC     j.base_type,
# MAGIC     j.license_number,
# MAGIC     j.shl_endorsed,
# MAGIC     j.telephone_number
# MAGIC FROM fhv_trip_with_zones f
# MAGIC LEFT JOIN global_temp.fhv_base_json_view j
# MAGIC   ON f.dispatching_base_num = j.license_number;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fhv_combined limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW fhv_aligned AS
# MAGIC SELECT
# MAGIC     dispatching_base_num,
# MAGIC     pickup_datetime,
# MAGIC     dropoff_datetime,
# MAGIC     PULocationID,
# MAGIC     pickup_borough,
# MAGIC     pickup_zone,
# MAGIC     pickup_service_zone,
# MAGIC     DOLocationID,
# MAGIC     dropoff_borough,
# MAGIC     dropoff_zone,
# MAGIC     dropoff_service_zone,
# MAGIC     VendorID,
# MAGIC     passenger_count,
# MAGIC     trip_distance,
# MAGIC     total_amount,
# MAGIC     trip_type,
# MAGIC     base_name,
# MAGIC     base_type,
# MAGIC     license_number,
# MAGIC     shl_endorsed,
# MAGIC     telephone_number
# MAGIC FROM fhv_combined;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW silver_aligned AS
# MAGIC SELECT
# MAGIC     NULL AS dispatching_base_num,
# MAGIC     pickup_datetime,
# MAGIC     dropoff_datetime,
# MAGIC     PULocationID,
# MAGIC     pickup_borough,
# MAGIC     pickup_zone,
# MAGIC     pickup_service_zone,
# MAGIC     DOLocationID,
# MAGIC     dropoff_borough,
# MAGIC     dropoff_zone,
# MAGIC     dropoff_service_zone,
# MAGIC     VendorID,
# MAGIC     passenger_count,
# MAGIC     trip_distance,
# MAGIC     total_amount,
# MAGIC     trip_type,
# MAGIC     NULL AS base_name,
# MAGIC     NULL AS base_type,
# MAGIC     NULL AS license_number,
# MAGIC     NULL AS shl_endorsed,
# MAGIC     NULL AS telephone_number
# MAGIC FROM combined_trip_zones_silver_tmp
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW all_trips_combined AS
# MAGIC SELECT * FROM fhv_aligned
# MAGIC UNION ALL
# MAGIC SELECT * FROM silver_aligned;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3:
# MAGIC **combine fhv_combined & combined_trip_zones_silver => all_trips_silver**

# COMMAND ----------

checkpoint_path="dbfs:/FileStore/dhathri/checkpoints/all_trips_silver"

# COMMAND ----------

(spark.table("all_trips_combined")
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .outputMode("append")
  .table("all_trips_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from all_trips_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from all_trips_silver limit 5 offset 100000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_type, COUNT(*) FROM all_trips_silver GROUP BY trip_type;
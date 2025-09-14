# Databricks notebook source
# MAGIC %md
# MAGIC # Note:
# MAGIC 1. added a column **trip_type-->'yellow'** for yellow trip & similarly **'green'** for green trip.
# MAGIC 2. silver table name has been changed from **combined_trip_silver** to **combined_trip_zones_silver**

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

(spark.readStream
  .table("combined_trip_silver")
  .createOrReplaceTempView("combined_trip_silver_temp"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 1:
# MAGIC **Creating an aggregate gold table of Total Count of Passengers for each pickup_zone, each vendorid.**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pickup_zone_passenger_count_view AS (
# MAGIC   SELECT pickup_zone, VendorID, sum(passenger_count) as total_passenger_count
# MAGIC   FROM combined_trip_silver_temp
# MAGIC   GROUP BY pickup_zone, VendorID)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe pickup_zone_passenger_count_view

# COMMAND ----------

# MAGIC %md
# MAGIC using availableNow=true, to run this streaming query once to process all existing data currently available and then stop.

# COMMAND ----------

checkpoint_path = "dbfs:/FileStore/dhathri/checkpoints/pickup_zone_passenger_count_gold"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS zone_passenger_count_gold;

# COMMAND ----------

(spark.table("pickup_zone_passenger_count_view")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .trigger(availableNow= True)
      .option("checkpointLocation", checkpoint_path)
      .table("pickup_zone_passenger_count_gold"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pickup_zone_passenger_count_gold limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pickup_zone_passenger_count_gold

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 2: 
# MAGIC **Total Fare amount for each pickup_borough and each vendor**

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW total_fare_by_pickup_borough_vendor AS
SELECT
  pickup_borough,
  VendorID,
  SUM(total_amount) AS total_fare_amount
FROM combined_trip_silver_temp
GROUP BY pickup_borough, VendorID
""")

# COMMAND ----------

checkpoint_path = "dbfs:/FileStore/dhathri/checkpoints/total_fare_gold_checkpoint"

# COMMAND ----------

(spark.table("total_fare_by_pickup_borough_vendor")
  .writeStream
  .format("delta")
  .outputMode("complete")
  .trigger(availableNow=True)
  .option("checkpointLocation", checkpoint_path)
  .table("total_fare_by_pickup_borough_vendor_gold"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from total_fare_by_pickup_borough_vendor_gold

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 3:
# MAGIC **Finding the average distance for each pickup borough and vendor.**

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view avg_distance_by_borough_vendor_view as (
# MAGIC   SELECT VendorID, avg(trip_distance) as avg_distance, pickup_borough
# MAGIC   FROM combined_trip_silver_temp
# MAGIC   GROUP BY VendorID, pickup_borough)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/dhathri/checkpoints/avg_distance_gold", recurse=True)

# COMMAND ----------

checkpoint_path="dbfs:/FileStore/dhathri/checkpoints/avg_distance_borough_vendor_gold"

# COMMAND ----------

# MAGIC %sql use catalog hive_metastore

# COMMAND ----------

(spark.table("avg_distance_by_borough_vendor_view")
  .writeStream
  .format("delta")
  .outputMode("complete")
  .trigger(availableNow=True)
  .option("checkpointLocation", checkpoint_path)
  .table("avg_distance_borough_vendor_gold"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from avg_distance_borough_vendor_gold limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC **further tables**
# MAGIC What is the average trip distance for each borough and vendor?
# MAGIC
# MAGIC What are the peak pickup hours based on total trip counts?
# MAGIC
# MAGIC Which are the top 10 pickup zones by total fare collected?
# MAGIC
# MAGIC How many trips occur per day of week in each borough?
# MAGIC
# MAGIC What is the average tip amount by payment type and vendor?
# MAGIC
# MAGIC What is the average fare per passenger for each zone?
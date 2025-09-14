# Databricks notebook source
taxi_zone_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("dbfs:/FileStore/dhathri/nyc/taxizone/TaxiZones.csv"))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists taxi_zone

# COMMAND ----------

# make taxi_zone_df as global
taxi_zone_df.createOrReplaceGlobalTempView("taxi_zone_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.taxi_zone_view limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_zone_view where Borough = 'Bronx' or Borough='Brooklyn'
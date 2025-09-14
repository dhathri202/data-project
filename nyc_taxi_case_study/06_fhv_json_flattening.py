# Databricks notebook source
# MAGIC %md
# MAGIC - convert json file to tabular format
# MAGIC - so that it is compatible to merge with other csv files
# MAGIC

# COMMAND ----------

fhv_json_df=spark.read.option("multiline", "true").json("dbfs:/FileStore/dhathri/nyc/fhv/FhvBases.json")

# COMMAND ----------

fhv_json_df.printSchema()


# COMMAND ----------

fhv_json_df.show()

# COMMAND ----------

display(fhv_json_df.limit(20))

# COMMAND ----------

from pyspark.sql.functions import col

fhv_clean_df = fhv_json_df.select(
    col("Address.Building").alias("building"),
    col("Address.Street").alias("street"),
    col("Address.City").alias("city"),
    col("Address.Postcode").alias("postcode"),
    col("Address.State").alias("state"),
    col("GeoLocation.Latitude").alias("latitude"),
    col("GeoLocation.Longitude").alias("longitude"),
    col("Entity Name").alias("entity_name"),
    col("License Number").alias("license_number"),
    col("SHL Endorsed").alias("shl_endorsed"),
    col("Telephone Number").alias("telephone_number"),
    col("Type of Base").alias("base_type"),
    col("Date").alias("date"),
    col("Time").alias("time")
)

# COMMAND ----------

fhv_clean_df.toDF(*[
    c.strip().lower().replace(" ", "_").replace("/", "_") for c in fhv_clean_df.columns
])

# COMMAND ----------

fhv_clean_df.createOrReplaceGlobalTempView("fhv_base_json_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fhv_base_json_view limit 20;
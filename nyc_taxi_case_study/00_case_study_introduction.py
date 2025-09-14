# Databricks notebook source
# MAGIC %md
# MAGIC ### Background

# COMMAND ----------

# MAGIC %md
# MAGIC New York City uses two main types of taxis:
# MAGIC - Yellow Taxis (Medallion) - Can pick up passengers anywhere in NYC.
# MAGIC - Green Taxis (Boro/Street Hail Livery)- Only work outside downtown Manhattan and airports.
# MAGIC - Apart from these, there are now many For-Hire Vehicles (FHVs) like:
# MAGIC - Uber, Lyft, Black cars, Limos, etc.
# MAGIC All of these vehicles are monitored and licensed by the NYC Taxi and Limousine Commission (TLC).
# MAGIC The system was fine until app-based ride services (FHVs) exploded in number.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenges

# COMMAND ----------

# MAGIC %md
# MAGIC - The arrival of Uber/Lyft-like services increased data volume massively.
# MAGIC - There are many types of FHVs, each working in a different way.
# MAGIC - The data comes in different file formats (CSV, TSV, JSON).
# MAGIC - The number of sources (over 750 bases and 100k vehicles) is huge.
# MAGIC - Real-time tracking and streaming became necessary due to high frequency of trips.
# MAGIC - _Need: to manage, clean, combine, and analyze all this messy and high-volume data._
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business need

# COMMAND ----------

# MAGIC %md
# MAGIC - Build a central platform to store and manage all taxi and FHV data.
# MAGIC - The system should work for batch and real-time streaming data.
# MAGIC - The data should be transformed into meaningful insights like: Revenue by taxi type or location, Total trips per region, Trip trends over time.
# MAGIC - The data should also be kept long-term for: Legal/regulatory use, Marketing and promotions, Safety and insurance needs.

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/dhathri/nyc'
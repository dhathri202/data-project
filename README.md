# data-project
Unified Data Engineering project on NYC Taxi &amp; FHV datasets using Databricks, Auto Loader, and Spark SQL. Includes ingestion, enrichment, and aggregation pipelines with bronze, silver, and gold tables.
# NYC Taxi Data Project (Databricks)

## Project Overview
Case study project built in **Databricks** to create a **unified data engineering solution** for NYC Yellow Taxi, Green Taxi, and FHV datasets.  
The project demonstrates ingestion, enrichment, and aggregation pipelines using **Auto Loader, Spark SQL, Delta Lake**, and the **Bronze → Silver → Gold** architecture.

## Datasets
- Yellow Taxi: Multiple CSV files
- Green Taxi: Multiple CSV files
- FHV Tripdata: Multiple CSV files
- Taxi Zone Lookup: Static CSV (batch processing)
- FHV Bases: Static JSON (heterogeneous, flattened and merged)

## Approach
- Stream ingestion of CSVs using **Auto Loader**
- Batch processing for static files (CSV/JSON)
- JSON flattening and merging with trip data
- Applied filtering, merges, and aggregations
- Organized data into **Bronze → Silver → Gold** tables

## Outputs
- 2–3 curated Silver tables
- Multiple Gold tables with aggregates and insights

## Tech Stack
- Databricks (PySpark, Spark SQL, dbutils)
- Delta Lake
- Auto Loader (stream processing)
- Magic commands (`%sql`, `%python`, `%fs`)

## Author
Built as a **self-learning case study** in Databricks. Most work done using SQL/Spark SQL with some Python and Databricks utilities.

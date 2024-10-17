# Databricks notebook source
# DBTITLE 1,Querying the files
# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# DBTITLE 1,CTAS
# MAGIC %sql
# MAGIC CREATE table customers as 
# MAGIC select *,current_timestamp() as ingestion_date from json.`/Volumes/ny_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/ny_databricks/default/raw/sales.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table product as 
# MAGIC select *,current_timestamp() as ingestion_date from json.`/Volumes/ny_databricks/default/raw/products.json`

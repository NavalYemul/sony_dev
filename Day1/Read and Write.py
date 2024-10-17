# Databricks notebook source
- pyspark df
- Spark SQL

E
data format(csv, table, json, parquet, detla, avro, orc)
(ADLS, Databases, DW, S3)


T 


L (parquet, DELTA)

# COMMAND ----------

csv (1gb)
97% ( 60- 80%)
parquet (200-300mb)

# COMMAND ----------

df=spark.read.csv("/Volumes/ny_databricks/default/raw/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

df.display()

# COMMAND ----------

df_customer=spark.read.json("/Volumes/ny_databricks/default/raw/customers.json")

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer.write.saveAsTable("customer")

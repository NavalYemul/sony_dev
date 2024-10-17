# Databricks notebook source
# MAGIC %md
# MAGIC ##ETL

# COMMAND ----------

# MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/Day1/includes

# COMMAND ----------

# DBTITLE 1,Read
df_sales=spark.read.csv(f"{input_path}order_dates.csv",header=True,inferSchema=True)

# COMMAND ----------

#calling function
df1=add_ingestion(df_sales)

# COMMAND ----------

# DBTITLE 1,Write
df1.write.mode("overwrite").saveAsTable("order_dates")

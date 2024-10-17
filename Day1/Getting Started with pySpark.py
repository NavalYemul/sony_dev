# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Core.
# MAGIC
# MAGIC RDD
# MAGIC DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame: Structured API

# COMMAND ----------

spark

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema=["id", "name", "age"]
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df=spark.createDataFrame(data)

# COMMAND ----------

df.display()

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

DataFrame Functions
.select
.alias 
.withColumnRenamed
.withColumnsRenamed
.withColumn

Functions
col

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df1=df.select("id","age")

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df1.display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("current_date",current_date()).display()

# COMMAND ----------

df.withColumn("age",current_date()).display()

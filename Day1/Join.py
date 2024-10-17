# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customer=spark.table("customers")

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer.where("customer_id>2 or customer_city ='New Michaelview'").display()
#df_customer.where(col("customer_id")==2).display()

# COMMAND ----------

df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined=df_sales.join(df_customer,df_sales["customer_id"]==df_customer["customer_id"],"inner")

# COMMAND ----------

df_joined=df_sales.join(df_customer,"customer_id","inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.count()

# COMMAND ----------

df_joined.groupBy(df_customer["customer_id"]).count().orderBy(df_customer["customer_id"]).display()

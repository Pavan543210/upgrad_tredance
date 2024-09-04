# Databricks notebook source
# MAGIC %run /Workspace/Users/pavan_1724384783110@npupgradassessment.onmicrosoft.com/day8/includes

# COMMAND ----------

df=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable("bronze.products_bronze")

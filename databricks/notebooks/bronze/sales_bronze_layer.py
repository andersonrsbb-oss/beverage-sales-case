# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze Sales

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df_sales = spark.read.parquet('/mnt/adlsdataengineeringprd/beverage-datalake/raw/sales')

df_sales = df_sales.withColumn('bronze_ingestion_timestamp', current_timestamp())

df_sales.write.mode('overwrite').format('delta').saveAsTable('hive_metastore.beverage_bronze.brz_tab_sales')

# COMMAND ----------

df_channel_group = spark.read.parquet('/mnt/adlsdataengineeringprd/beverage-datalake/raw/channel_group')

df_channel_group = df_channel_group.withColumn('bronze_ingestion_timestamp', current_timestamp())

df_channel_group.write.mode('overwrite').format('delta').saveAsTable('catalog_beverage.beverage_bronze.brz_tab_channel_group')
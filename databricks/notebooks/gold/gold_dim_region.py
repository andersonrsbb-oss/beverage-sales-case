# Databricks notebook source
# MAGIC %md
# MAGIC ##Dim Region

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df_sales = spark.table("hive_metastore.beverage_silver.slv_tab_sales")

dim_region = df_sales.select("ds_bottler_org_lvl_c_region").distinct().withColumnRenamed("ds_bottler_org_lvl_c_region","ds_region")

dim_region = dim_region.withColumn("sk_region", monotonically_increasing_id())

dim_region = (
  dim_region
  .select(
    'sk_region',
    'ds_region'
  )
)
dim_region.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable('hive_metastore.beverage_gold.gld_tab_dim_region')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.beverage_gold.gld_tab_dim_region
# Databricks notebook source
# MAGIC %md
# MAGIC ##Dim Brand Flavor

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df_sales = spark.table("hive_metastore.beverage_silver.slv_tab_sales")

dim_brand_flavor = (
    df_sales.select("nm_brand", "cd_brand_flavor")
    .distinct()
    .withColumn("sk_brand", monotonically_increasing_id())
    .select("sk_brand", "cd_brand_flavor", "nm_brand")
)
dim_brand_flavor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hive_metastore.beverage_gold.gld_tab_dim_brand_flavor")
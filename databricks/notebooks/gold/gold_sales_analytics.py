# Databricks notebook source
# MAGIC %md
# MAGIC ##Report Sales Analytics

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

df_fact_sales = spark.read.table('hive_metastore.beverage_gold.gld_tab_fact_sales')
df_dim_date = spark.read.table('hive_metastore.beverage_gold.gld_tab_dim_date')
df_dim_region = spark.read.table('hive_metastore.beverage_gold.gld_tab_dim_region')
df_dim_brand_flavor = spark.read.table('hive_metastore.beverage_gold.gld_tab_dim_brand_flavor')

# COMMAND ----------

rep_sales = (
    df_fact_sales
    .join(df_dim_brand_flavor, df_fact_sales.sk_brand == df_dim_brand_flavor.sk_brand, "left")
    .join(df_dim_date, df_fact_sales.cd_date == df_dim_date.cd_date, "left")
    .join(df_dim_region, df_fact_sales.sk_region == df_dim_region.sk_region, "left")
)

# rep_sales.display()
rep_sales = (
    rep_sales
    .groupBy(
        'ds_date',
        'ds_year_month',
        'nm_brand',
        'ds_region',
        'ds_channel_group',
        'ds_trade_channel_desc',
        'ds_trade_group',
        'ds_trade_type'
    )
    .agg(f.sum('price_volume').alias('price_volume'))
)

rep_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable('hive_metastore.beverage_gold.gld_tab_rep_sales_analytics')

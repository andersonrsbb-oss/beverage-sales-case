# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

df_sales = spark.read.table('hive_metastore.beverage_silver.slv_tab_sales')
df_channel = spark.read.table('hive_metastore.beverage_silver.slv_tab_channel_group')
df_dim_date = spark.read.table('hive_metastore.beverage_gold.gld_tab_dim_date')
df_dim_region = spark.read.table('hive_metastore.beverage_gold.gld_tab_dim_region')
df_dim_brand_flavor = spark.read.table('hive_metastore.beverage_gold.gld_tab_dim_brand_flavor')

# COMMAND ----------

fact_sales = (
    df_sales
    .join(df_channel, df_sales.ds_trade_channel_desc == df_channel.ds_trade_channel_desc, "left")
    .join(df_dim_brand_flavor, df_sales.nm_brand == df_dim_brand_flavor.nm_brand, "left")
    .join(df_dim_date, df_sales.ds_date == df_dim_date.ds_date, "left")
    .join(df_dim_region, df_sales.ds_bottler_org_lvl_c_region == df_dim_region.ds_region, "left")
    .drop(df_channel.ds_trade_channel_desc)
)

fact_sales = (
    fact_sales
    .groupBy(
        'cd_date',
        'sk_brand',
        'sk_region',
        'ds_channel_group',
        'ds_trade_channel_desc',
        'ds_trade_group',
        'ds_trade_type',
        'cd_category_pack',
        'ds_category_pack',
        'ds_tsr_pckg_nm'
    )
    .agg(f.sum('price_volume').alias('price_volume'))
)

fact_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable('hive_metastore.beverage_gold.gld_tab_fact_sales')
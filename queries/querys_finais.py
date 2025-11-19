# Databricks notebook source
# MAGIC %md
# MAGIC #Query Finais

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 - Top 3 Trade Group for Region

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

df_sales_analytics = spark.read.table('hive_metastore.beverage_gold.gld_tab_rep_sales_analytics')

# COMMAND ----------

window_spec = Window.partitionBy('ds_region').orderBy(f.sum('price_volume').desc())

df_agg = (
    df_sales_analytics
    .groupBy('ds_region', 'ds_trade_group')
    .agg(f.sum('price_volume').alias('total_price'))
)

df_ranked = df_agg.withColumn(
    'row',
    f.row_number().over(
        Window.partitionBy('ds_region').orderBy(f.col('total_price').desc())
    )
)

df_top3 = df_ranked.filter(f.col('row') <= 3).select(
    'ds_region', 'ds_trade_group', 'total_price'
)

display(df_top3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 - Sales Brand per Month

# COMMAND ----------

df_agg_brand = (
    df_sales_analytics
    .groupBy('nm_brand', 'ds_year_month')
    .agg(f.sum('price_volume').alias('total_price'))
)

display(df_agg_brand)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 - Lowest Brand in sales per Region

# COMMAND ----------

window_spec = Window.partitionBy('ds_region').orderBy(f.sum('price_volume').desc())

df_agg_lowest = (
    df_sales_analytics
    .groupBy('ds_region', 'nm_brand')
    .agg(f.sum('price_volume').alias('total_price'))
)

df_ranked_lowest = df_agg_lowest.withColumn(
    'row',
    f.row_number().over(
        Window.partitionBy('ds_region').orderBy(f.col('total_price').asc())
    )
)

df_lowest = df_ranked_lowest.filter(f.col('row') == 1).select(
    'ds_region', 'nm_brand', 'total_price'
)

df_lowest.display()
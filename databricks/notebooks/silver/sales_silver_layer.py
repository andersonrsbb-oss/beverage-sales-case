# Databricks notebook source
# MAGIC %md
# MAGIC ##Import

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carregamento da Tabela Bronze

# COMMAND ----------

df = spark.table("hive_metastore.beverage_bronze.brz_tab_sales")

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renomeação e Conversão de Tipos das Colunas

# COMMAND ----------

df_renamed = (
    #rename and force type
    df.select(
        f.to_date(f.col("date"), "M/d/yyyy").alias("ds_date"),
        f.col("ce_brand_flvr").cast('int').alias("cd_brand_flavor"),
        f.col("brand_nm").alias("nm_brand"),
        f.col("btlr_org_lvl_c_desc").alias("ds_bottler_org_lvl_c_region"),
        f.col("chnl_group").alias("ds_channel_group"),
        f.col("trade_chnl_desc").alias("ds_trade_channel_desc"),
        f.col("pkg_cat").alias("cd_category_pack"),
        f.col("pkg_cat_desc").alias("ds_category_pack"),
        f.col("tsr_pckg_nm").alias("ds_tsr_pckg_nm"),
        f.col("$_volume").cast("decimal(10,2)").alias("price_volume"),
        f.col("year").alias("ds_year"),
        f.col("month").alias("ds_month"),
        f.col("period").alias("ds_period"),
        f.col("bronze_ingestion_timestamp").alias("dt_bronze_ingestion_timestamp"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Limpeza de Caracteres Especiais

# COMMAND ----------

df_cleaned = (
    # remove special characters and extra spaces
    df_renamed.select(
        [
            f.regexp_replace(
                f.regexp_replace(f.col(c), r"[^a-zA-Z0-9\s]", ""), r"\s+", " "
            ).alias(c) if t == "string" else f.col(c)
            for c, t in df_renamed.dtypes
        ]
    )
)
df_cleaned.count()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

window_spec = Window.partitionBy(
    "ds_date", "cd_brand_flavor", "nm_brand", "ds_bottler_org_lvl_c_region",
    "ds_channel_group", "ds_trade_channel_desc", "cd_category_pack",
    "ds_category_pack", "ds_tsr_pckg_nm", "price_volume", "ds_year",
    "ds_month", "ds_period"
).orderBy(f.lit(1))

df_dedup = df_cleaned.withColumn(
    "row_num", f.row_number().over(window_spec)
).filter(f.col("row_num") == 1).drop("row_num")

# df_dedup.display()

# COMMAND ----------

df_sales_final = (
    df_dedup
    .select(
        "ds_date",
        "cd_brand_flavor",
        "nm_brand",
        "ds_bottler_org_lvl_c_region",
        "ds_channel_group",
        "ds_trade_channel_desc",
        "cd_category_pack",
        "ds_category_pack",
        "ds_tsr_pckg_nm",
        "price_volume",
        "ds_year",
        "ds_month",
        "ds_period",
        "dt_bronze_ingestion_timestamp"
    )
)

# COMMAND ----------

df_sales_final.write.mode("overwrite").saveAsTable("hive_metastore.beverage_silver.slv_tab_sales")
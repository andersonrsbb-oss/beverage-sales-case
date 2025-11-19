# Databricks notebook source
# MAGIC %md
# MAGIC ##Imports

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carregamento da Tabela Bronze

# COMMAND ----------

df = spark.table("hive_metastore.beverage_bronze.brz_tab_channel_group")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renomeação e Conversão de Tipos das Colunas

# COMMAND ----------

df_renamed = (
    #rename and force type
    df.select(
        f.col("trade_chnl_desc").alias("ds_trade_channel_desc"),
        f.col("trade_group_desc").alias("ds_trade_group"),
        f.col("trade_type_desc").alias("ds_trade_type"),
        f.col("bronze_ingestion_timestamp").alias("dt_bronze_ingestion_timestamp"),
    )
)
display(df_renamed)

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
df_cleaned.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Remove duplicatas

# COMMAND ----------

df_dedup = df_cleaned.dropDuplicates()

df_dedup.display()

# COMMAND ----------

df_channel_group_final = (
    df_dedup
    .select(
        "ds_trade_channel_desc",
        "ds_trade_group",
        "ds_trade_type",
        "dt_bronze_ingestion_timestamp"
    )
)

# COMMAND ----------

df_channel_group_final.write.mode("overwrite").saveAsTable("hive_metastore.beverage_silver.slv_tab_channel_group")
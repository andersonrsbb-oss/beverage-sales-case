# Databricks notebook source
# MAGIC %md
# MAGIC ##Dim Date

# COMMAND ----------

from pyspark.sql.functions import sequence, explode, to_date, col, year, month, dayofmonth, dayofweek, date_format, weekofyear, concat, lpad
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

# Defina o intervalo de datas desejado
start_date = "2005-01-01"
end_date = "2007-12-31"

# Crie um DataFrame com todas as datas no intervalo
df_dim_date = (
    spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
    .select(explode(sequence(to_date(col("start_date")), to_date(col("end_date")))).alias("ds_date"))
    .withColumn("nr_year", year(col("ds_date")))
    .withColumn("nr_month", month(col("ds_date")))
    .withColumn("nr_day", dayofmonth(col("ds_date")))
    .withColumn("nr_day_of_week", dayofweek(col("ds_date")))
    .withColumn("nr_week_of_year", weekofyear(col("ds_date")))
    .withColumn("nm_month_name", date_format(col("ds_date"), "MMMM"))
    .withColumn("nm_day_name", date_format(col("ds_date"), "EEEE"))
)

df_dim_date = (
    df_dim_date.withColumn("cd_date", row_number().over(Window.orderBy("ds_date")))
    .withColumn("ds_year_month", concat(col("nr_year"), lpad(col("nr_month"), 2, "0")))
)

df_dim_date = (
    df_dim_date.select(
        "cd_date",
        "ds_date",
        "nr_year",
        "nr_month",
        "nr_day",
        "nr_day_of_week",
        "nr_week_of_year",
        "nm_month_name",
        "nm_day_name",
        "ds_year_month"       
    )
)
df_dim_date.write.format("delta").mode("overwrite").saveAsTable("hive_metastore.beverage_gold.gld_tab_dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.beverage_gold.gld_tab_dim_date
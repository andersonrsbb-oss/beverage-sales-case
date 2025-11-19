# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Database

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.default;
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS beverage_bronze
# MAGIC LOCATION '/mnt/adlsdataengineeringprd/bronze/beverage_bronze';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS beverage_silver
# MAGIC LOCATION '/mnt/adlsdataengineeringprd/silver/beverage_silver';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS beverage_gold
# MAGIC LOCATION '/mnt/adlsdataengineeringprd/gold/beverage_gold'
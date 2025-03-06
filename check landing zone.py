# Databricks notebook source
dbutils.fs.ls('abfss://aize-container@adlsaize.dfs.core.windows.net/')

# COMMAND ----------

dbutils.fs.ls('abfss://aize-container@adlsaize.dfs.core.windows.net/landing-zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dbr_aize.dbr01_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dbr_aize.dbr01_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dbr_aize.dbr01_gold
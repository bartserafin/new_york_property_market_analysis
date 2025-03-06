# Databricks notebook source
landing_zone = "abfss://aize-container@adlsaize.dfs.core.windows.net/landing-zone"

# COMMAND ----------

# Load Airbnb CSV file into a df
airbnb_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{landing_zone}/Airbnb_data_New_York.csv")

# Save as Delta Table in Bronze Layer
airbnb_df.write.format("delta").mode("overwrite").save("abfss://aize-container@adlsaize.dfs.core.windows.net/bronze/airbnb_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_bronze.airbnb
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/bronze/airbnb_data";

# COMMAND ----------

# Load Property Sales CSV file into df
property_sales_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{landing_zone}/Property_sales_data_New_York.csv")

clean_columns = [col.strip().replace(" ", "_") if col else "Unnamed_Column" for col in property_sales_df.columns]
property_sales_df = property_sales_df.toDF(*clean_columns)

# save to bronze layer
property_sales_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save("abfss://aize-container@adlsaize.dfs.core.windows.net/bronze/property_sales_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_bronze.property_sales
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/bronze/property_sales_data";

# COMMAND ----------

# load weather excel into df
weather_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(f"{landing_zone}/Weather Data.xlsx")

clean_columns = [col.strip().replace(" ", "_") if col else "Unnamed_Column" for col in weather_df.columns]
weather_df = weather_df.toDF(*clean_columns)

# save to bronze layer
weather_df.write.format("delta").mode("overwrite").save("abfss://aize-container@adlsaize.dfs.core.windows.net/bronze/weather_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_bronze.weather
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/bronze/weather_data";
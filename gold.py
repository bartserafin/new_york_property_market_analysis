# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Read silver layer tables
df_airbnb = spark.read.format("delta").table("dbr01_silver.airbnb")
df_property_sales = spark.read.format("delta").table("dbr01_silver.property_sales")
df_weather = spark.read.format("delta").table("dbr01_silver.weather")

# COMMAND ----------

# aggregate property sales by borough

# average sale price per borough
avg_price_df = df_property_sales.groupBy("borough_name").agg(round(avg("sale_price"), 2).alias("avg_property_price"))
avg_price_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Key Metrics for Real Estate Investment Analysis
# MAGIC
# MAGIC ## 1. Yearly Revenue
# MAGIC $$ \text{Estimated Yearly Revenue} = \text{Average Rental Price} \times \text{Average 365 Availability} \times 0.7 $$
# MAGIC (Assuming 70% occupancy)
# MAGIC
# MAGIC
# MAGIC ## 2. Return of Investment % (ROI)
# MAGIC $$ \text{ROI} = \frac{\text{Estimated Yearly Revenue}}{\text{Average Sale Price}} \times 100 $$
# MAGIC - Below 3%: Low yield
# MAGIC - 3-5%: Moderate yield
# MAGIC - 5-7%: Good yield
# MAGIC - Above 7%: High yield

# COMMAND ----------

# aggregate airbnb by borough
OCCUPANCY_RATE = 0.8

# average nightly price per borough
avg_airbnb_price_df = df_airbnb.withColumnRenamed("neighbourhood_group", "borough_name").groupBy("borough_name")\
    .agg(round(avg(col("availability_365")), 2).alias("avg_availability_365"), round(avg("price"), 2).alias("avg_nightly_price"))


# Estimate annual revenue per borough (avg nightly price * average availability * OCCUPANCY_RATE)
annual_revenue_df = avg_airbnb_price_df.withColumn("estimated_annual_revenue", round(col("avg_nightly_price") * col("avg_availability_365") * OCCUPANCY_RATE, 2))

# Calculate ROI (Annual Revenue / Average Property Price) * 100
roi_df = annual_revenue_df.join(avg_price_df, annual_revenue_df.borough_name == avg_price_df.borough_name, "inner")\
    .withColumn("ROI", round((col("estimated_annual_revenue") / col("avg_property_price")) * 100, 2))\
    .select(annual_revenue_df.borough_name, "avg_property_price", "estimated_annual_revenue", "ROI")


roi_df.show()

roi_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://aize-container@adlsaize.dfs.core.windows.net/gold/property_revenue")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_gold.property_revenue
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/gold/property_revenue"
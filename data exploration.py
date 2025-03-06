# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Key Metrics for Real Estate Investment Analysis
# MAGIC
# MAGIC ## 1. Rental Yield
# MAGIC $$ \text{Rental Yield} = \frac{\text{Estimated Yearly Revenue}}{\text{Average Sale Price}} \times 100 $$
# MAGIC
# MAGIC Where:
# MAGIC $$ \text{Estimated Yearly Revenue} = \text{Average Rental Price} \times 365 \times 0.7 $$
# MAGIC (Assuming 70% occupancy)
# MAGIC - Below 3%: Low yield
# MAGIC - 3-5%: Moderate yield
# MAGIC - 5-7%: Good yield
# MAGIC - Above 7%: High yield
# MAGIC
# MAGIC ## 2. Price-to-Rent Ratio
# MAGIC $$ \text{Price-to-Rent Ratio} = \frac{\text{Average Sale Price}}{\text{Average Rental Price} \times 12} $$
# MAGIC
# MAGIC ## 3. Occupancy Rate
# MAGIC $$ \text{Occupancy Rate} = \frac{365 - \text{Average Availability}}{365} \times 100 $$
# MAGIC
# MAGIC ## 4. Price Difference Percentage
# MAGIC $$ \text{Price Difference Percentage} = \frac{\text{Sale Price} - \text{Average Sale Price}}{\text{Average Sale Price}} \times 100 $$
# MAGIC
# MAGIC ## 5. Property Score
# MAGIC $$ \text{Property Score} = \text{Rental Yield} + \frac{100}{\text{Price-to-Rent Ratio}} + \frac{\text{Occupancy Rate}}{2} - |\text{Price Difference Percentage}| $$
# MAGIC
# MAGIC ## 6. Airbnb Score
# MAGIC $$ \text{Airbnb Score} = \text{Rental Yield} + \frac{\text{Occupancy Rate}}{2} + \frac{\text{Number of Reviews}}{10} - |\text{Price Difference Percentage}| $$
# MAGIC
# MAGIC ## 7. Investment Score
# MAGIC $$ \text{Investment Score} = \text{Rental Yield} + \frac{100}{\text{Price-to-Rent Ratio}} + \frac{\text{Occupancy Rate}}{2} $$
# MAGIC

# COMMAND ----------

# Read gold layer tables
df_rentals = spark.read.format("delta").table("dbr01_gold.rentals")
df_neighborhood_stats = spark.read.format("delta").table("dbr01_gold.neighborhood_stats")
df_property_stats = spark.read.format("delta").table("dbr01_gold.property_stats")
df_weather_trends = spark.read.format("delta").table("dbr01_gold.weather_trends")
df_weather = spark.read.format("delta").table("dbr01_silver.weather")

# COMMAND ----------

# 1. Identify High-Demand Neighborhoods for Short-Term Rentals
high_demand_neighborhoods = df_neighborhood_stats.orderBy(desc("listing_count"), desc("avg_rental_price")) \
    .select("neighbourhood", "neighbourhood_group", "listing_count", "avg_rental_price", "avg_reviews", "avg_availability") \
    .limit(10)

high_demand_neighborhoods.show()

# COMMAND ----------

high_demand_neighborhoods.select("neighbourhood", "neighbourhood_group", "listing_count", "avg_rental_price", "avg_reviews", "avg_availability").write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://aize-container@adlsaize.dfs.core.windows.net/gold/high_demand_neighborhoods")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_gold.high_demand_neighborhoods
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/gold/high_demand_neighborhoods"

# COMMAND ----------

# 2. Evaluate Property Prices vs. Rental Income Potential
# Assuming 70% occupancy
rental_yield = df_neighborhood_stats.join(df_property_stats, 
    (df_neighborhood_stats.neighbourhood == df_property_stats.NEIGHBORHOOD) &
    (df_neighborhood_stats.neighbourhood_group == df_property_stats.BOROUGH_NAME)) \
    .withColumn("estimated_yearly_revenue", col("avg_rental_price") * 365 * 0.7) \
    .withColumn("rental_yield", col("estimated_yearly_revenue") / col("avg_sale_price") * 100) \
    .select("neighbourhood", "neighbourhood_group", "avg_rental_price", "avg_sale_price", "rental_yield") \
    .orderBy(desc("rental_yield"))

rental_yield.show()

# COMMAND ----------

rental_yield.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://aize-container@adlsaize.dfs.core.windows.net/gold/rental_yield")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_gold.rental_yield
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/gold/rental_yield"

# COMMAND ----------

# 3. Assess Seasonal Trends Using Weather Data
seasonal_trends = df_rentals.join(df_weather, df_rentals.SALE_DATE.cast("date") == df_weather.Date) \
    .groupBy("season", month("SALE_DATE").alias("month")) \
    .agg(
        avg("price").alias("avg_rental_price"),
        avg("Mean_Temperature").alias("avg_temperature"),
        count("*").alias("rental_count")
    ) \
    .orderBy("month")

seasonal_trends.show()

# COMMAND ----------

seasonal_trends.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://aize-container@adlsaize.dfs.core.windows.net/gold/seasonal_trends")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_gold.seasonal_trends
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/gold/seasonal_trends"

# COMMAND ----------

# Additional analysis: Property types in high-demand areas
property_types = df_rentals.filter(col("neighbourhood").isin([row.neighbourhood for row in high_demand_neighborhoods.collect()])) \
    .groupBy("neighbourhood", "BUILDING_CLASS_CATEGORY") \
    .count() \
    .orderBy(desc("count"))

property_types.show()


# COMMAND ----------

print("Top Neighborhoods for Investment:")
high_demand_neighborhoods.show(5)
print("\nNeighborhoods with Highest Rental Yield:")
rental_yield.show(5)


# COMMAND ----------

print("Recommended Property Types in High-Demand Areas:")
property_types.show(10)


# COMMAND ----------

print("Seasonal Trends:")
seasonal_trends.orderBy(desc("rental_count")).show()

# Identify off-season months
off_season = seasonal_trends.orderBy("rental_count").limit(3)
print("Off-season months (lowest rental count):")
off_season.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Best Areas roperties to invest in

# COMMAND ----------

# Calculate investment metrics
investment_opportunities = df_property_stats.join(
    df_neighborhood_stats,
    (df_property_stats.NEIGHBORHOOD == df_neighborhood_stats.neighbourhood) &
    (df_property_stats.BOROUGH_NAME == df_neighborhood_stats.neighbourhood_group),
    "inner"
).withColumn(
    "rental_yield",
    (col("avg_rental_price") * 365 * 0.7) / col("avg_sale_price") * 100  # Assuming 70% occupancy
).withColumn(
    "price_to_rent_ratio",
    col("avg_sale_price") / (col("avg_rental_price") * 12)
).withColumn(
    "occupancy_rate",
    (365 - col("avg_availability")) / 365 * 100
).select(
    "NEIGHBORHOOD",
    "BOROUGH_NAME",
    "avg_sale_price",
    "avg_rental_price",
    "rental_yield",
    "price_to_rent_ratio",
    "occupancy_rate",
    "listing_count",
    "sales_count"
)

# Best areas for property investment (based on rental yield)
print("Top 10 Neighborhoods for Property Investment:")
investment_opportunities.orderBy(desc("rental_yield")).show(10)

# Best areas for Airbnb rentals (based on occupancy rate and listing count)
print("Top 10 Neighborhoods for Airbnb Rentals:")
investment_opportunities.orderBy(desc("occupancy_rate"), desc("listing_count")).show(10)

# Overall best investment opportunities (considering multiple factors)
print("Top 10 Overall Investment Opportunities:")
investment_opportunities.withColumn(
    "investment_score",
    col("rental_yield") + (100 / col("price_to_rent_ratio")) + (col("occupancy_rate") / 2)
).orderBy(desc("investment_score")).show(10)


# COMMAND ----------

investment_opportunities.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://aize-container@adlsaize.dfs.core.windows.net/gold/investment_opportunities")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_gold.investment_opportunities
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/gold/investment_opportunities"

# COMMAND ----------

from pyspark.sql.window import Window

df_property_sales = spark.read.format("delta").table("dbr_aize.dbr01_silver.property_sales")
df_airbnb = spark.read.format("delta").table("dbr_aize.dbr01_silver.airbnb")

property_sales = df_property_sales.alias("ps")
inv_opportunities = investment_opportunities.alias("io")

# Join property sales with investment opportunities
property_investment_opportunities = property_sales.join(
    inv_opportunities,
    (col("ps.NEIGHBORHOOD") == col("io.NEIGHBORHOOD")) &
    (col("ps.BOROUGH_NAME") == col("io.BOROUGH_NAME")),
    "inner"
)

# Calculate property-specific metrics
property_scores = property_investment_opportunities.withColumn(
    "price_difference_percentage", 
    (col("ps.SALE_PRICE") - col("io.avg_sale_price")) / col("io.avg_sale_price") * 100
).withColumn(
    "property_score",
    col("io.rental_yield") + 
    (100 / col("io.price_to_rent_ratio")) + 
    (col("io.occupancy_rate") / 2) -
    abs(col("price_difference_percentage"))  # Penalize properties too far from average price
)

# Rank properties within each neighborhood
window_spec = Window.partitionBy("ps.NEIGHBORHOOD").orderBy(desc("property_score"))
ranked_properties = property_scores.withColumn("rank", rank().over(window_spec))

# Select top properties
top_properties = ranked_properties.filter(col("rank") <= 3).orderBy(desc("property_score")).limit(10)

print("Top 10 Properties for Investment:")
top_properties.select(
    "ps.ADDRESS", "ps.NEIGHBORHOOD", "ps.BOROUGH_NAME", "ps.SALE_PRICE", "ps.BUILDING_CLASS_CATEGORY",
    "io.rental_yield", "io.price_to_rent_ratio", "io.occupancy_rate", "property_score"
).show(truncate=False)

# For Airbnb listings
airbnb = df_airbnb.alias("ab")
airbnb_investment_opportunities = airbnb.join(
    inv_opportunities,
    (col("ab.neighbourhood") == col("io.NEIGHBORHOOD")) &
    (col("ab.neighbourhood_group") == col("io.BOROUGH_NAME")),
    "inner"
)

# Calculate Airbnb-specific metrics
airbnb_scores = airbnb_investment_opportunities.withColumn(
    "price_difference_percentage", 
    (col("ab.price") - col("io.avg_rental_price")) / col("io.avg_rental_price") * 100
).withColumn(
    "airbnb_score",
    col("io.rental_yield") + 
    (col("io.occupancy_rate") / 2) +
    (col("ab.number_of_reviews") / 10) -  # Consider review count as a proxy for popularity
    abs(col("price_difference_percentage"))  # Penalize listings too far from average price
)

# Rank Airbnb listings within each neighborhood
airbnb_window_spec = Window.partitionBy("ab.neighbourhood").orderBy(desc("airbnb_score"))
ranked_airbnb = airbnb_scores.withColumn("rank", rank().over(airbnb_window_spec))

# Select top Airbnb listings
top_airbnb = ranked_airbnb.filter(col("rank") <= 3).orderBy(desc("airbnb_score")).limit(10)

print("Top 10 Airbnb Listings for Investment:")
top_airbnb.select(
    "ab.name", "ab.neighbourhood", "ab.neighbourhood_group", "ab.price", "ab.room_type",
    "io.rental_yield", "io.occupancy_rate", "ab.number_of_reviews", "airbnb_score"
).show(truncate=False)


# COMMAND ----------

top_properties.select(
    "ps.ADDRESS", "ps.NEIGHBORHOOD", "ps.BOROUGH_NAME", "ps.SALE_PRICE", "ps.BUILDING_CLASS_CATEGORY",
    "io.rental_yield", "io.price_to_rent_ratio", "io.occupancy_rate", "property_score")\
    .write.format("delta").mode("overwrite").save("abfss://aize-container@adlsaize.dfs.core.windows.net/gold/top_properties")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_gold.top_properties
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/gold/top_properties"

# COMMAND ----------

top_airbnb.write.format("delta").mode("overwrite").save("abfss://aize-container@adlsaize.dfs.core.windows.net/gold/top_airbnb")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_gold.top_airbnb
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/gold/top_airbnb"
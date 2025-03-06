# Databricks notebook source
from pyspark.sql.functions import *

# Silver schema paths
silver_airbnb_path = "abfss://aize-container@adlsaize.dfs.core.windows.net/silver/airbnb_data"
silver_property_sales_path = "abfss://aize-container@adlsaize.dfs.core.windows.net/silver/property_sales_data"
silver_weather_path = "abfss://aize-container@adlsaize.dfs.core.windows.net/silver/weather_data"


# COMMAND ----------

# Clean and transform Airbnb data
df_airbnb = spark.read.format("delta").table("dbr_aize.dbr01_bronze.airbnb")

df_airbnb = df_airbnb.dropDuplicates()

# Drop rows with missing values in key columns (price, availability, location)
df_airbnb = df_airbnb.dropna(subset=["price", "neighbourhood_group", "availability_365"])

# cast data type
df_airbnb = df_airbnb\
    .withColumn("price", col("price").cast("integer")) \
    .withColumn("minimum_nights", col("minimum_nights").cast("integer"))\
    .withColumn("number_of_reviews", col("number_of_reviews").cast("integer"))\
    .withColumn("availability_365", col("availability_365").cast("integer"))\
    .withColumn("reviews_per_month", col("reviews_per_month").cast("integer"))\
    .withColumn("longitude", col("longitude").cast("float"))\
    .withColumn("latitude", col("latitude").cast("float"))

# Filter anomalies, data out of range
df_airbnb_cleaned = df_airbnb.filter(
    (col("price") > 20) &
    (col("minimum_nights") >= 1) &
    (col("number_of_reviews") > 0) &
    (col("availability_365") > 30) &
    (col("availability_365") <= 365)
)

# Lowercase all column names and assign back
df_lowercase = df_airbnb_cleaned.select([col(c).alias(c.lower()) for c in df_airbnb_cleaned.columns])

# Write the cleaned and transformed data
df_lowercase.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_airbnb_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_silver.airbnb
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/silver/airbnb_data"

# COMMAND ----------

# Clean and transform Property Sales data
df_property_sales = spark.read.format("delta").table("dbr_aize.dbr01_bronze.property_sales")



# Remove duplicates
df_property_sales = df_property_sales.dropDuplicates()

# Drop rows with missing important values (e.g., price, borough)
df_property_sales = df_property_sales.dropna(subset=["BOROUGH", "SALE_PRICE"])

# cast data type
df_property_sales = df_property_sales\
    .withColumn("SALE_PRICE", col("SALE_PRICE").cast("integer"))\
    .withColumn("LAND_SQUARE_FEET", col("LAND_SQUARE_FEET").cast("integer"))\
    .withColumn("GROSS_SQUARE_FEET", col("GROSS_SQUARE_FEET").cast("integer"))\
    .withColumn("YEAR_BUILT", col("YEAR_BUILT").cast("integer"))\

df_property_sales_cleaned = df_property_sales.filter(
    (col("SALE_PRICE") > 10000) &
    (col("GROSS_SQUARE_FEET") > 0) &
    (col("LAND_SQUARE_FEET") > 0) &
    (col("YEAR_BUILT") >= 1800) &
    (col("YEAR_BUILT") <= year(current_date()))
)

# Map borough numbers to names
df_property_sales_cleaned = df_property_sales_cleaned.withColumn(
    "BOROUGH_NAME",
    when(col("BOROUGH") == 1, "Manhattan")
     .when(col("BOROUGH") == 2, "Bronx")
     .when(col("BOROUGH") == 3, "Brooklyn")
     .when(col("BOROUGH") == 4, "Queens")
     .when(col("BOROUGH") == 5, "Staten Island")
     .otherwise("Unknown")  # Handle unexpected values
    )\
    .withColumn("SALE_DATE", to_date(col("SALE_DATE")))\
    .withColumn("NEIGHBORHOOD", upper(col("NEIGHBORHOOD")))\
    .drop("_c0")

# lowercase all column names
df_property_sales_cleaned = df_property_sales_cleaned.select([col(c).alias(c.lower()) for c in df_property_sales_cleaned.columns])
df_property_sales_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_property_sales_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_silver.property_sales
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/silver/property_sales_data"

# COMMAND ----------

from pyspark.sql.window import Window

# Clean and transform Weather data (parse dates, filter invalid rows)
df_weather = spark.read.format("delta").table("dbr_aize.dbr01_bronze.weather")

# Remove duplicates
df_weather = df_weather.dropDuplicates()

# Define a window specification to order rows
window_spec = Window.orderBy("location")

# Add row number to each row
df_with_index = df_weather.withColumn("row_number", row_number().over(window_spec))

# Filter out first 8 rows because they are metadata for weather
df_filtered = df_with_index.filter(col("row_number") > 9).drop("row_number")

# rename key columns
df_weather = df_filtered.withColumnRenamed("location", "Date") \
                       .withColumnRenamed("New_York1", "Maximum_Temperature") \
                       .withColumnRenamed("New_York2", "Minimum_Temperature") \
                       .withColumnRenamed("New_York3", "Mean_Temperature") \

# Convert Date Column to Proper Format
df_weather = df_weather.withColumn("Date", to_timestamp(col("Date"), "yyyy-MM-dd'T'HH:mm:ss"))
df_weather = df_weather.withColumn("Date", to_date(col("Date")))

df_weather = df_weather.withColumn("Maximum_Temperature", col("Maximum_Temperature").cast("float"))\
                        .withColumn("Minimum_Temperature", col("Minimum_Temperature").cast("float"))

# Handle Missing Values
df_weather_cleaned = df_weather.filter(col("Date").isNotNull())

df_weather_cleaned = df_weather_cleaned.filter(
    (col("Maximum_Temperature") > 0) &
    (col("Maximum_Temperature") < 50) &
    (col("Minimum_Temperature") > -20) &
    (col("Minimum_Temperature") < 50) &
    (col("Mean_Temperature") > -20) &
    (col("Mean_Temperature") < 50) &
    (col("Minimum_Temperature") <= col("Maximum_Temperature")) &
    (col("Mean_Temperature") >= col("Minimum_Temperature")) &
    (col("Mean_Temperature") <= col("Maximum_Temperature"))
)
df_weather_cleaned = df_weather_cleaned\
    .withColumn("season",
        when((month("date").isin(12, 1, 2)), "Winter")
            .when((month("date").isin(3, 4, 5)), "Spring")
            .when((month("date").isin(6, 7, 8)), "Summer")
            .when((month("date").isin(9, 10, 11)), "Autumn")
        )

# lowercase all column names
df_weather_cleaned = df_weather_cleaned.select([col(c).alias(c.lower()) for c in df_weather_cleaned.columns])

df_weather_cleaned.write.format("delta").mode("overwrite").save(silver_weather_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr01_silver.weather
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://aize-container@adlsaize.dfs.core.windows.net/silver/weather_data"
# Databricks notebook source
# MAGIC %md
# MAGIC # Combined Transforms
# MAGIC ### Delta -> Delta : Gold

# COMMAND ----------

abs_df = spark.read.load('/mnt/domain_data/silver/combined/delta')
domain_df = spark.read.load('/mnt/domain_data/silver/domain_sales_listings')

abs_df.createOrReplaceTempView('abs_vw')
domain_df.createOrReplaceTempView('domain_vw')

# COMMAND ----------

display(abs_df)

# COMMAND ----------

display(domain_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.listing_count
# MAGIC LOCATION '/mnt/domain_data/gold/listing_count/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   UPPER(State) AS State, 
# MAGIC   postcode as Postcode, 
# MAGIC   count(*) AS Listing_count
# MAGIC FROM domain_vw
# MAGIC GROUP BY state, postcode;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.agencies
# MAGIC LOCATION '/mnt/domain_data/gold/agencies/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   agencyname,
# MAGIC   agencyProfilePageUrl,
# MAGIC   UPPER(State) AS State,
# MAGIC   count(Identifier) AS Property_count
# MAGIC FROM domain_vw
# MAGIC WHERE agencyname IS NOT NULL
# MAGIC GROUP BY
# MAGIC   agencyname,
# MAGIC   agencyProfilePageUrl,
# MAGIC   State
# MAGIC ORDER BY State, agencyname

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.status_results
# MAGIC LOCATION '/mnt/domain_data/gold/status_results/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   UPPER(state) AS State, 
# MAGIC   postcode AS Postcode, 
# MAGIC   suburb AS Suburb, 
# MAGIC   result AS Result, 
# MAGIC   count(Identifier) AS Count
# MAGIC FROM domain_vw
# MAGIC GROUP BY State, Postcode, Suburb, Result
# MAGIC ORDER BY State, Postcode, Suburb, Result;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.average_price
# MAGIC LOCATION '/mnt/domain_data/gold/average_price/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC     UPPER(state) as State, 
# MAGIC     suburb as Suburb, 
# MAGIC     propertytype as PropertyType, 
# MAGIC     ROUND(AVG(price), 0) AveragePrice,
# MAGIC     COUNT(*) AS Count
# MAGIC FROM domain_vw
# MAGIC     WHERE result NOT IN ('AUWD', 'AUVB', 'PTSW', 'PTLA', 'AUPI', 'AUHB')
# MAGIC     AND price IS NOT NULL
# MAGIC     AND state IS NOT NULL
# MAGIC     GROUP BY state, suburb, propertytype
# MAGIC     ORDER BY state, suburb, propertytype;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.property_performance
# MAGIC LOCATION '/mnt/domain_data/gold/property_performance/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC     UPPER(state) AS State, 
# MAGIC     propertytype AS PropertyType, 
# MAGIC     bedrooms AS Bedrooms, 
# MAGIC     ROUND(AVG(price), 0) AS AveragePrice, 
# MAGIC     ROUND(STDDEV(price), 0) AS STDPrice, 
# MAGIC     COUNT(*) AS Count
# MAGIC FROM domain_vw
# MAGIC     WHERE result NOT IN ('AUWD', 'AUVB', 'PTSW', 'PTLA', 'AUPI', 'AUHB')
# MAGIC     AND price IS NOT NULL
# MAGIC     AND state IS NOT NULL
# MAGIC     GROUP BY state, propertytype, bedrooms
# MAGIC     ORDER BY state, propertytype, bedrooms DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.property_details
# MAGIC LOCATION '/mnt/domain_data/gold/property_details/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   id AS PropertyID,
# MAGIC   propertyType AS PropertyType,
# MAGIC   bedrooms AS Bedroom,
# MAGIC   bathrooms AS Bathroom,
# MAGIC   carspaces AS Carspaces,
# MAGIC   streetNumber AS StreetNumber,
# MAGIC   unitNumber AS UnitNumber,
# MAGIC   streetType AS StreetType,
# MAGIC   suburb AS Suburb,
# MAGIC   city AS City,
# MAGIC   postcode AS Postcode,
# MAGIC   UPPER(state) AS State,
# MAGIC   latitude AS Latitude,
# MAGIC   longitude AS Longitude,
# MAGIC   result AS Result,
# MAGIC   propertyDetailsURL AS DomainURL
# MAGIC FROM domain_vw;

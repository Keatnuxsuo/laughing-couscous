# Databricks notebook source
# MAGIC %md
# MAGIC # Domain Extract Loader
# MAGIC ### Delta -> Delta : Silver

# COMMAND ----------

# Includes
from pyspark.sql.functions import lit, col
from pyspark.sql import functions
from delta.tables import DeltaTable

# COMMAND ----------

# Loading bronze data
bronze = '/mnt/domain_data/bronze/domain_sales_listings'
df_bronze = spark.read.load(bronze)

# COMMAND ----------

# Loading silver data
silver = '/mnt/domain_data/silver/domain_sales_listings'
if DeltaTable.isDeltaTable(spark, silver):
    df_silver = spark.read.load(silver)
else:
    df_silver = None

# COMMAND ----------

if df_bronze is not None:
    df_bronze.createOrReplaceTempView('domain_sales_listings__bronze')
    
if df_silver is not None:
    df_silver.createOrReplaceTempView('domain_sales_listings__silver')

# COMMAND ----------

if df_silver is not None:
    for field in df_silver.schema.fields:
        print(field.name + " -> " + str(field.dataType))

# COMMAND ----------

# Getting the current bronze week 
if 'week_of_year' in df_bronze.columns:
    bronze__max_week = df_bronze.agg({'week_of_year': 'max'}).collect()[0][0]
else:
    bronze__max_week = 0
    
bronze__max_week

# COMMAND ----------

# Getting the current silver week 
if df_silver is not None and 'week_of_year' in df_silver.columns:
    silver__max_week = df_silver.agg({'week_of_year': 'max'}).collect()[0][0]
else:
    silver__max_week = 0
    
spark.conf.set('helper_vars.max_week', silver__max_week)

# COMMAND ----------

df_bronze_target = df_bronze.filter(df_bronze.week_of_year >= lit(silver__max_week))
df_bronze_target.createOrReplaceTempView('domain_sales_listings__bronze_target')
display(df_bronze_target['identifier', 'week_of_year'])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First run will create new table
# MAGIC -- May help to create table with explicit schema at start of the notebook and then just leave the merge statement to do its work
# MAGIC CREATE TABLE IF NOT EXISTS 
# MAGIC   silver.domain_sales_listings
# MAGIC (
# MAGIC    identifier STRING,
# MAGIC    agencyId LONG,
# MAGIC    agencyName STRING,
# MAGIC    agencyProfilePageUrl STRING,
# MAGIC    agent STRING,
# MAGIC    bathrooms LONG,
# MAGIC    bedrooms LONG,
# MAGIC    carspaces LONG,
# MAGIC    city STRING,
# MAGIC    execution_time STRING,
# MAGIC    id LONG,
# MAGIC    postcode STRING,
# MAGIC    price LONG,
# MAGIC    propertyDetailsUrl STRING,
# MAGIC    propertyType STRING,
# MAGIC    result STRING,
# MAGIC    state STRING,
# MAGIC    streetName STRING,
# MAGIC    streetNumber STRING,
# MAGIC    streetType STRING,
# MAGIC    suburb STRING,
# MAGIC    unitNumber STRING,
# MAGIC    latitude DOUBLE,
# MAGIC    longitude DOUBLE,
# MAGIC    week_of_year INTEGER
# MAGIC )
# MAGIC LOCATION 
# MAGIC   '/mnt/domain_data/silver/domain_sales_listings';

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO 
# MAGIC   silver.domain_sales_listings TARGET
# MAGIC USING 
# MAGIC   domain_sales_listings__bronze_target SOURCE
# MAGIC ON 
# MAGIC   TARGET.identifier = SOURCE.identifier 
# MAGIC   AND SOURCE.week_of_year = TARGET.week_of_year
# MAGIC   AND TARGET.result = SOURCE.result
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET * -- equivalent of Python whenMatchedUpdateAll
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *; -- equivalent of Python whenNotMatchedInsertAll

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.domain_sales_listings;

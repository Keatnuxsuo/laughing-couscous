# Databricks notebook source
# MAGIC %md
# MAGIC ### Preconditions
# MAGIC S3 Bucket mounted to /mnt/domain_data
# MAGIC ### Input
# MAGIC Json lines file from S3 bucket in AWS<br>
# MAGIC Existing records from silver.domain_sales_listings
# MAGIC ### Output
# MAGIC Delta table in the silver schema

# COMMAND ----------

# MAGIC %run ../Includes/functions

# COMMAND ----------

MOUNT_POINT = '/mnt/domain_data'

# COMMAND ----------

#list out S3 objects
#jsonl is the output from airbyte. DB read jsonl the same way as json
listings_path = f'{MOUNT_POINT}/bronze/domain/sales_listings/domain_sales_listings_stream/'
dbutils.fs.ls(listings_path)

# COMMAND ----------

df = spark.read.json(listings_path)
display(df.take(5))

# COMMAND ----------

df.count()

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from df
# MAGIC where _airbyte_data.price is not null

# COMMAND ----------

spark.udf.register('last_sunday', get_last_sunday)

# COMMAND ----------

source_df = spark.sql("""
    with source as (
    select _airbyte_data.*
    from df
    where _airbyte_data.price is not null)
    select sha2(concat_ws('||', propertyDetailsUrl, result, listingWeekEnding), 256) as listing_sk,
           *
    from (
    select distinct
      id::string
      , propertyDetailsUrl::string
      , propertyType::string
      , result::string
      , last_sunday(to_date(execution_time))::date as listingWeekEnding
      , unitNumber::string
      , streetNumber::string
      , streetName::string
      , streetType::string
      , suburb::string
      , upper(state) as state
      , postcode::string
      , price::int
      , geoLocation.latitude::float as latitude
      , geolocation.longitude::float as longitude
      , unix_timestamp() as insert_datetime
    from source) t 
""")

# COMMAND ----------

source_df.count()

# COMMAND ----------

display(source_df.take(5))

# COMMAND ----------

source_df.createOrReplaceTempView('source')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from source
# MAGIC limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Target Listings or Create Table

# COMMAND ----------

if 'domain_sales_listings' not in get_schema_tables('silver'):
    create_domain_silver()
    target_df = spark.table('silver.domain_sales_listings')
    target_df.createOrReplaceTempView('target')
else:
    target_df = spark.table('silver.domain_sales_listings')
    target_df.createOrReplaceTempView('target')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge Latest Data into Target (silver.domain_sales_listings)

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into target
# MAGIC using source
# MAGIC on target.listing_sk = source.listing_sk
# MAGIC when matched then
# MAGIC update set 
# MAGIC       target.id = source.id,
# MAGIC       target.propertyType = source.propertyType,
# MAGIC       target.listingWeekEnding = source.listingWeekEnding,
# MAGIC       target.unitNumber = source.unitNumber,
# MAGIC       target.streetNumber = source.streetNumber,
# MAGIC       target.streetName = source.streetName,
# MAGIC       target.streetType = source.streetType,
# MAGIC       target.suburb = source.suburb,
# MAGIC       target.state = source.state,
# MAGIC       target.postcode = source.postcode,
# MAGIC       target.price = source.price,
# MAGIC       target.latitude = source.latitude,
# MAGIC       target.longitude = source.longitude,
# MAGIC       target.insert_datetime = source.insert_datetime
# MAGIC when not matched then
# MAGIC insert
# MAGIC (
# MAGIC       listing_sk,
# MAGIC       id,
# MAGIC       propertyDetailsUrl,
# MAGIC       propertyType,
# MAGIC       result,
# MAGIC       listingWeekEnding,
# MAGIC       unitNumber,
# MAGIC       streetNumber,
# MAGIC       streetName,
# MAGIC       streetType,
# MAGIC       suburb,
# MAGIC       state,
# MAGIC       postcode,
# MAGIC       price,
# MAGIC       latitude,
# MAGIC       longitude,
# MAGIC       insert_datetime
# MAGIC )
# MAGIC values
# MAGIC (
# MAGIC       source.listing_sk,
# MAGIC       source.id,
# MAGIC       source.propertyDetailsUrl,
# MAGIC       source.propertyType,
# MAGIC       source.result,
# MAGIC       source.listingWeekEnding,
# MAGIC       source.unitNumber,
# MAGIC       source.streetNumber,
# MAGIC       source.streetName,
# MAGIC       source.streetType,
# MAGIC       source.suburb,
# MAGIC       source.state,
# MAGIC       source.postcode,
# MAGIC       source.price,
# MAGIC       source.latitude,
# MAGIC       source.longitude,
# MAGIC       source.insert_datetime
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Current Version of Target to S3

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.domain_sales_listings
# MAGIC select *
# MAGIC from target

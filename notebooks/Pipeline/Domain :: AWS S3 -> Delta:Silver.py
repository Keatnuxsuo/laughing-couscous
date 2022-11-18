# Databricks notebook source
import urllib

ACCESS_KEY = 'AKIA4Q3QTMVFBT227RNX'
SECRET_KEY = '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'
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
AWS_S3_BUCKET = 'domain-dwh/'
MOUNT_NAME = '/mnt/domain_data'

# COMMAND ----------

# SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

# SOURCE_URL

# COMMAND ----------

#mount s3
if not (any(mount.mountPoint == MOUNT_NAME for mount in dbutils.fs.mounts())):
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
else:
    print("Drive already mounted.")

#unmount
#dbutils.fs.unmount('/mnt/domain_data')

# COMMAND ----------

#list out S3 objects
#jsonl is the output from airbyte. DB read jsonl the same way as json
listings = '/mnt/domain_data/bronze/domain/sales_listings/domain_sales_listings_stream/'
dbutils.fs.ls(listings)

# COMMAND ----------

df = spark.read.json(listings)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# create watermark path
# check the watermark path if doesn't exist then set watermark date = funciton(executionweek)
# only process this notebook when date is greater than watermark date
# if exists, read the file and set the watermark date to the to maxdate 

# COMMAND ----------

# Aggregation function e.g. avg, sum, max, min, count, rank
# Grouping i.e. group by
# Window function e.g. partition by
# Calculation e.g. column_A + column_B
# Data type casting
# Filtering e.g. where, having
# Sorting
# Joins/merges
# Unions
# Renaming e.g. select col_a as my_col_a

# COMMAND ----------

df2 = df.select("_airbyte_data.*")
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col

df3 = (df2
             .withColumn('latitude', col("geoLocation.latitude"))
             .withColumn('longitude', col("geoLocation.longitude"))
             .drop('geoLocation')
)
#df3 = (df3.withColumn(col('execution_time')), col('execution_time')) # NICK
display(df3)

# COMMAND ----------

#remove Null record on postcode column

from pyspark.sql import functions
#df3.where(functions.isnull(functions.col("postcode"))).count()
df3 = df3.filter(df3.postcode.isNotNull())

# COMMAND ----------

#check number of rows after removing
df3.count()

# COMMAND ----------

#Cast execution date to listing week in a year
from pyspark.sql.functions import weekofyear
df3 = df3.withColumn('week_of_year',weekofyear(df3.execution_time))

display(df3)

# COMMAND ----------


# transformation 3 - if its successfully wirtten to silver, then write Watermark in Bronze layer -> extract the date from the file path (maxdate) -> wana to execute the file that > more than maxdate
df_final.createOrReplaceTempView('domain_sales_listings_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.domain_sales_listings
# MAGIC LOCATION '/mnt/domain_data/bronze/domain_sales_listings'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM
# MAGIC   domain_sales_listings_cleaned;

# COMMAND ----------



# COMMAND ----------

#write the df_final to silver layer -> '/mnt/domain_data/silver/domain_sales_listings'
#maerge into and use propertyDetailsUrl + results as a key

# COMMAND ----------

# %sql

# CREATE OR REPLACE TABLE silver.domain_sales_listings
# LOCATION '/mnt/domain_data/silver/domain_sales_listings'
# AS
# SELECT
#   *
# FROM
#   domain_sales_listings_cleaned 
# WHERE
#   listingDate in ('{0}')

# COMMAND ----------

# What Alex does in production
#write data into the location he wants but this doesn't write into metadatastore
#df_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save('/mnt/domain_data/silver/domain_sales_listings')
#so need to still use sql to write to the metadatastore location
# %sql

# CREATE TABLE IF NOT EXISTS silver.domain_sales_listings
# USING DELTA
# LOCATION '/mnt/domain_data_silver_domain_sales_listings'

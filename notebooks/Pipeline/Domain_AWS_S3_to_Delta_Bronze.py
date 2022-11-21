# Databricks notebook source
# MAGIC %md
# MAGIC # Domain Extract Loader
# MAGIC ### AWS S3 -> Delta : Bronze

# COMMAND ----------

import urllib
from pyspark.sql.functions import col
from pyspark.sql import functions
from pyspark.sql.functions import weekofyear

# COMMAND ----------

# Credentials and drive mounting
ACCESS_KEY = dbutils.secrets.get(scope='DOMAIN_DWH', key='AWS_DOMAIN_S3_ID')
SECRET_KEY = dbutils.secrets.get(scope='DOMAIN_DWH', key='AWS_DOMAIN_S3_KEY')

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
AWS_S3_BUCKET = 'domain-dwh/'
MOUNT_NAME = '/mnt/domain_data'

SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

# Mount S3
if not (any(mount.mountPoint == MOUNT_NAME for mount in dbutils.fs.mounts())):
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
else:
    print("Drive already mounted.")

# Unmount S3
# dbutils.fs.unmount(MOUNT_NAME)

# COMMAND ----------

# List out S3 objects
listings = '/mnt/domain_data/bronze/domain/sales_listings/domain_sales_listings_stream/'
dbutils.fs.ls(listings)

# COMMAND ----------

# Loading data and selecting payload
df_raw = spark.read.json(listings)
df_airbyte_data = df_raw.select("_airbyte_data.*")
df_airbyte_data.count()

# COMMAND ----------

# Flattening Geolocation JSON
df = (df_airbyte_data
             .withColumn('latitude', col("geoLocation.latitude"))
             .withColumn('longitude', col("geoLocation.longitude"))
             .drop('geoLocation')
)

#remove Null record on postcode column
df = df.filter(df.postcode.isNotNull())

#check number of rows after removing
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

# Cast execution date to week of the year
df = df.withColumn('week_of_year',weekofyear(df.execution_time))
df = df.dropDuplicates(subset=['propertyDetailsUrl', 'result', 'week_of_year'])
df.createOrReplaceTempView('domain_sales_listings')
display(df['propertyDetailsURL', 'week_of_year'])

# COMMAND ----------

display(df)

# COMMAND ----------

for field in df.schema.fields:
    print(field.name + " -> " + str(field.dataType))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM
# MAGIC   domain_sales_listings;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.domain_sales_listings
# MAGIC LOCATION '/mnt/domain_data/bronze/domain_sales_listings'
# MAGIC AS
# MAGIC SELECT CONCAT(propertyDetailsUrl, '_', result) as identifier, *
# MAGIC FROM domain_sales_listings;

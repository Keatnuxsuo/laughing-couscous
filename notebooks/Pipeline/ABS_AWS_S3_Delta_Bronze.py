# Databricks notebook source
# MAGIC %md
# MAGIC # ABS Extract Loader
# MAGIC ### AWS S3 -> Delta : Bronze

# COMMAND ----------

# Includes
import urllib
import json

# COMMAND ----------

# Credentials and Drive mounting
ACCESS_KEY = dbutils.secrets.get(scope='DOMAIN_DWH', key='AWS_DOMAIN_S3_ID')
SECRET_KEY = dbutils.secrets.get(scope='DOMAIN_DWH', key='AWS_DOMAIN_S3_KEY')

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
AWS_S3_BUCKET = 'domain-dwh/'
MOUNT_NAME = '/mnt/domain_data'

SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

if not (any(mount.mountPoint == MOUNT_NAME for mount in dbutils.fs.mounts())):
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
else:
    print("Drive already mounted.")

# COMMAND ----------

# Defining main Extraction function

def extract(target):
  src_data = extract_01(target)
  keys, values, dimensions = extract_02(src_data)
  df = extract_03(keys, values, dimensions, target)
  
  return df

# COMMAND ----------

# Compartmentalised E & L functions

def extract_01(target):
  df = spark.read.text(f'dbfs:/mnt/domain_data/bronze/abs/{target}/abs_data/*.jsonl')
  data = df.collect()
  src_data = json.loads(data[0]['value'])
  return src_data

def extract_02(data):
  dims = {}
  
  obs_keys = [k for k in data['_airbyte_data']['data']['dataSets'][0]['observations'].keys()]
  obs_vals = [v for v in data['_airbyte_data']['data']['dataSets'][0]['observations'].values()]

  for o in data['_airbyte_data']['data']['structure']['dimensions']['observation']:
      dims[o['id']] = [k['name'] for k in o['values']]
      
  return obs_keys, obs_vals, dims

def extract_03(keys, values, dimensions, target):
  dim1 = 'STATE'
  dim2 = 'REGION'  
  
  widget_dim_order = {}
  output = []
  
  for idx, k in enumerate(dimensions.keys()):
      if k == dim1:
          widget_dim_order[dim1] = idx
      elif k == dim2:
          widget_dim_order[dim2] = idx

  for idx, obs in enumerate(keys):
      _dim_idx = obs.split(':')
      _dim1 = dimensions[dim1][int(_dim_idx[widget_dim_order[dim1]])]
      _dim2 = dimensions[dim2][int(_dim_idx[widget_dim_order[dim2]])]
      _metric = values[idx][0]

      output.append([
          target,
          _dim1,
          _dim2,
          _metric
      ])
      
  df = spark.createDataFrame(output)
  cols = [
      'Metric',
      'State',
      'Postcode',
      'Value'
  ]
  for old, new in zip(df.columns, cols):
      df = df.withColumnRenamed(old, new)
  return df

# COMMAND ----------

# Extracting and defining the Dataframes
hhld_size_df = extract('hhld_size')
med_age_df = extract('med_age_persons')
med_income_df = extract('med_ttl_fam_income_weekly')
mortgage_pymt_df = extract('med_mortgage_repymt_mthly')
rent_pymt_df = extract('med_rent_weekly')

# COMMAND ----------

display(hhld_size_df)

# COMMAND ----------

display(med_age_df)

# COMMAND ----------

display(med_income_df)

# COMMAND ----------

display(mortgage_pymt_df)

# COMMAND ----------

display(rent_pymt_df)

# COMMAND ----------

hhld_size_df.createOrReplaceTempView('hhld_size_vw')
med_age_df.createOrReplaceTempView('med_age_vw')
med_income_df.createOrReplaceTempView('med_income_vw')
mortgage_pymt_df.createOrReplaceTempView('mortgage_pymt_vw')
rent_pymt_df.createOrReplaceTempView('rent_pymt_vw')

# COMMAND ----------

# MAGIC %md Loading Dataframes to Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.hhld_size
# MAGIC LOCATION '/mnt/abs_data/hhld_size/delta'
# MAGIC AS
# MAGIC SELECT sha2(concat_ws('||', array(Metric, State, Postcode, Value)), 256) as hhld_size_key, 
# MAGIC        Metric::string,
# MAGIC        State::string,
# MAGIC        Postcode::string,
# MAGIC        Value::bigint
# MAGIC FROM hhld_size_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.med_age
# MAGIC LOCATION '/mnt/domain_data/bronze/abs/med_age_persons/delta'
# MAGIC AS
# MAGIC SELECT sha2(concat_ws('||', array(Metric, State, Postcode, Value)), 256) as med_age_key, 
# MAGIC        Metric::string,
# MAGIC        State::string,
# MAGIC        Postcode::string,
# MAGIC        Value::bigint
# MAGIC FROM med_age_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.med_income
# MAGIC LOCATION '/mnt/domain_data/bronze/abs/med_ttl_fam_income_weekly/delta'
# MAGIC AS
# MAGIC SELECT sha2(concat_ws('||', array(Metric, State, Postcode, Value)), 256) as med_income_key, 
# MAGIC        Metric::string,
# MAGIC        State::string,
# MAGIC        Postcode::string,
# MAGIC        Value::bigint
# MAGIC FROM med_income_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.mortgage_pymt
# MAGIC LOCATION '/mnt/domain_data/bronze/abs/med_mortgage_repymt_mthly/delta'
# MAGIC AS
# MAGIC SELECT sha2(concat_ws('||', array(Metric, State, Postcode, Value)), 256) as mortgage_pymt_key, 
# MAGIC        Metric::string,
# MAGIC        State::string,
# MAGIC        Postcode::string,
# MAGIC        Value::bigint
# MAGIC FROM mortgage_pymt_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.rent_pymt
# MAGIC LOCATION '/mnt/domain_data/bronze/abs/med_rent_weekly/delta'
# MAGIC AS
# MAGIC SELECT sha2(concat_ws('||', array(Metric, State, Postcode, Value)), 256) as rent_pymt_key, 
# MAGIC        Metric::string,
# MAGIC        State::string,
# MAGIC        Postcode::string,
# MAGIC        Value::bigint
# MAGIC FROM rent_pymt_vw

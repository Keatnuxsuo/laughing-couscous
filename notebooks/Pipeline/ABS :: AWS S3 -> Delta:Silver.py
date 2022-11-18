# Databricks notebook source
# MAGIC %md
# MAGIC ### Settings
# MAGIC dataset = the bucket name in AWS for the source data<br>
# MAGIC dimension_1 = choice of STATE or REGION to extract these values from the observation key (colon-delimited)<br>
# MAGIC dimension_2 = choice of STATE or REGION to extract these values from the observation key (colon-delimited)
# MAGIC ### Input
# MAGIC Json lines file from S3 bucket in AWS
# MAGIC ### Output
# MAGIC Delta table in the silver schema

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN dataset DEFAULT "" CHOICES SELECT * FROM (VALUES (""), ("hhld_size"), ("med_age"), ("med_income"), ("mortgage_pymt"), ("rent_pymt"));
# MAGIC CREATE WIDGET DROPDOWN dimension_1 DEFAULT "STATE" CHOICES SELECT * FROM (VALUES (""), ("STATE"), ("REGION"));
# MAGIC CREATE WIDGET DROPDOWN dimension_2 DEFAULT "REGION" CHOICES SELECT * FROM (VALUES (""), ("STATE"), ("REGION"));

# COMMAND ----------

import urllib
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dataframe from drop zone

# COMMAND ----------

# MAGIC %md
# MAGIC Note... code reads in data to a dataframe as text, then collects to driver and re-creates a Python dictionary<br>
# MAGIC Reason is that trying to navigate object using spark.read.json cannot handle case of multiple keys within the observations object - I doubt it is a limitation of PySpark, I just can't get it to work using a dataframe!

# COMMAND ----------

dataset = dbutils.widgets.get('dataset')
df = spark.read.text(f'dbfs:/mnt/abs_data/{dataset}/abs_data/*.jsonl')
data = df.collect()
src_data = json.loads(data[0]['value'])

# COMMAND ----------

src_data

# COMMAND ----------

# MAGIC %md
# MAGIC Collect observation keys and values.<br>
# MAGIC #### Keys
# MAGIC The key looks like 0:0:1:0:0 (and may have additional colon separated values depending on the metric and how the API query was structured)<br>
# MAGIC The reason for the structure is that each value represents an index against the different dimensions that the API returns<br>
# MAGIC In the case of rent_pymt, the dimensions are MEDAVG, REGION, REGION_TYPE, STATE, TIME_PERIOD.  We are only interested in the REGION and STATE dimensions.
# MAGIC #### Values
# MAGIC The values are the observations associated with the metric, in the case of rent_pymt, the values are the median weekly rent payable in the region.

# COMMAND ----------

obs_keys = [k for k in src_data['_airbyte_data']['data']['dataSets'][0]['observations'].keys()]
obs_vals = [v for v in src_data['_airbyte_data']['data']['dataSets'][0]['observations'].values()]

# COMMAND ----------

obs_keys[:10]

# COMMAND ----------



# COMMAND ----------

obs_vals[:10]

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dictionary of dimensions.<br>
# MAGIC Purpose is to then identify where in the dimension list, the widget values are located (i.e. their index value)

# COMMAND ----------

dims = {}
for o in src_data['_airbyte_data']['data']['structure']['dimensions']['observation']:
    dims[o['id']] = [k['name'] for k in o['values']]

# COMMAND ----------

dims.keys()

# COMMAND ----------

# MAGIC %md
# MAGIC Find the index values in the dimension list of the selected dimensions in the widgets - dimension_1 and dimension_2

# COMMAND ----------

widget_dim_order = {}
for idx, k in enumerate(dims.keys()):
    dim1 = dbutils.widgets.get('dimension_1')
    dim2 = dbutils.widgets.get('dimension_2')
    if k == dim1:
        widget_dim_order[dim1] = idx
    elif k == dim2:
        widget_dim_order[dim2] = idx

# COMMAND ----------

widget_dim_order

# COMMAND ----------

# MAGIC %md
# MAGIC Replace the observation key with the associated dimension value obtained above.<br>
# MAGIC Package values together in a list of lists to then create a dataframe for export.

# COMMAND ----------

output = []
for idx, obs in enumerate(obs_keys):
    _dim_idx = obs.split(':')
    _dim1 = dims[dbutils.widgets.get('dimension_1')][int(_dim_idx[widget_dim_order[dbutils.widgets.get('dimension_1')]])]
    _dim2 = dims[dbutils.widgets.get('dimension_2')][int(_dim_idx[widget_dim_order[dbutils.widgets.get('dimension_2')]])]
    _metric = obs_vals[idx][0]
    output.append([
        dbutils.widgets.get('dataset').replace('-', '_'),
        _dim1,
        _dim2,
        _metric
    ])

# COMMAND ----------

output[:10]

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dataframe of the extracted values

# COMMAND ----------

out_df = spark.createDataFrame(output)
cols = [
    'metric_name',
    dbutils.widgets.get('dimension_1').lower(),
    dbutils.widgets.get('dimension_2').lower(),
    'metric_value'
]
for old, new in zip(out_df.columns, cols): # Required to handle case where dataframe is created with no defined schema
    out_df = out_df.withColumnRenamed(old, new)

# COMMAND ----------

display(out_df)

# COMMAND ----------

out_df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %md
# MAGIC Convert output dataframe to a delta table and assign datatypes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.${dataset} 
# MAGIC LOCATION '/mnt/abs_data/${dataset}/delta'
# MAGIC AS
# MAGIC SELECT sha2(concat_ws('||', array(metric_name, ${dimension_1}, ${dimension_2}, metric_value)), 256) as ${dataset}_key, 
# MAGIC        metric_name::string,
# MAGIC        region::string,
# MAGIC        state::string,
# MAGIC        metric_value::bigint
# MAGIC FROM df

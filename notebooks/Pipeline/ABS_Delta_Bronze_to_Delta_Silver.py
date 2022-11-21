# Databricks notebook source
# MAGIC %md
# MAGIC # ABS Extract Loader
# MAGIC ### Delta -> Delta : Silver

# COMMAND ----------

#hhld_size_df = spark.read.load('/mnt/abs_data/hhld_size/delta')
med_age_df = spark.read.load('/mnt/domain_data/bronze/abs/med_age_persons/delta')
med_income_df = spark.read.load('/mnt/domain_data/bronze/abs/med_ttl_fam_income_weekly/delta')
mortgage_pymt_df = spark.read.load('/mnt/domain_data/bronze/abs/med_mortgage_repymt_mthly/delta')
rent_pymt_df = spark.read.load('/mnt/domain_data/bronze/abs/med_rent_weekly/delta')

# COMMAND ----------

#hhld_size_df.createOrReplaceTempView('hhld_size__bronze')
med_age_df.createOrReplaceTempView('med_age__bronze')
med_income_df.createOrReplaceTempView('med_income__bronze')
mortgage_pymt_df.createOrReplaceTempView('mortgage_pymt__bronze')
rent_pymt_df.createOrReplaceTempView('rent_pymt__bronze')

# COMMAND ----------

# CREATE OR REPLACE TABLE silver.household_size
# LOCATION '/mnt/abs_data/household_size/delta'
# AS
# SELECT 
#   State,
#   CASE State 
#     WHEN 'New South Wales' THEN 'NSW' 
#     WHEN 'Victoria' THEN 'VIC'
#     WHEN 'Tasmania' THEN 'TAS'
#     WHEN 'Australian Capital Territory' THEN 'ACT'
#     WHEN 'South Australia' THEN 'SA'
#     WHEN 'Western Australia' THEN 'WA'
#     WHEN 'Queensland' THEN 'QLD'
#     ELSE 'OTH'
#     END AS State_Abbr,
#   Postcode as Postcode,
#   Value as Household_size
#   FROM hhld_size__bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.median_age
# MAGIC LOCATION '/mnt/domain_data/silver/median_age/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   State,
# MAGIC   CASE State 
# MAGIC     WHEN 'New South Wales' THEN 'NSW' 
# MAGIC     WHEN 'Victoria' THEN 'VIC'
# MAGIC     WHEN 'Tasmania' THEN 'TAS'
# MAGIC     WHEN 'Australian Capital Territory' THEN 'ACT'
# MAGIC     WHEN 'South Australia' THEN 'SA'
# MAGIC     WHEN 'Western Australia' THEN 'WA'
# MAGIC     WHEN 'Queensland' THEN 'QLD'
# MAGIC     ELSE 'OTH'
# MAGIC     END AS State_Abbr,
# MAGIC   Postcode as Postcode,
# MAGIC   Value as Median_age
# MAGIC   FROM med_age__bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.median_income
# MAGIC LOCATION '/mnt/domain_data/silver/median_income/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   State,
# MAGIC   CASE State 
# MAGIC     WHEN 'New South Wales' THEN 'NSW' 
# MAGIC     WHEN 'Victoria' THEN 'VIC'
# MAGIC     WHEN 'Tasmania' THEN 'TAS'
# MAGIC     WHEN 'Australian Capital Territory' THEN 'ACT'
# MAGIC     WHEN 'South Australia' THEN 'SA'
# MAGIC     WHEN 'Western Australia' THEN 'WA'
# MAGIC     WHEN 'Queensland' THEN 'QLD'
# MAGIC     ELSE 'OTH'
# MAGIC     END AS State_Abbr,
# MAGIC   Postcode as Postcode,
# MAGIC   Value as Median_income
# MAGIC   FROM med_income__bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.mortgage_payment
# MAGIC LOCATION '/mnt/domain_data/silver/mortgage_payment/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   State,
# MAGIC   CASE State 
# MAGIC     WHEN 'New South Wales' THEN 'NSW' 
# MAGIC     WHEN 'Victoria' THEN 'VIC'
# MAGIC     WHEN 'Tasmania' THEN 'TAS'
# MAGIC     WHEN 'Australian Capital Territory' THEN 'ACT'
# MAGIC     WHEN 'South Australia' THEN 'SA'
# MAGIC     WHEN 'Western Australia' THEN 'WA'
# MAGIC     WHEN 'Queensland' THEN 'QLD'
# MAGIC     ELSE 'OTH'
# MAGIC     END AS State_Abbr,
# MAGIC   Postcode as Postcode,
# MAGIC   Value as Mortgage_payment
# MAGIC   FROM mortgage_pymt__bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.rent_payment
# MAGIC LOCATION '/mnt/domain_data/silver/rent_payment/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   State,
# MAGIC   CASE State 
# MAGIC     WHEN 'New South Wales' THEN 'NSW' 
# MAGIC     WHEN 'Victoria' THEN 'VIC'
# MAGIC     WHEN 'Tasmania' THEN 'TAS'
# MAGIC     WHEN 'Australian Capital Territory' THEN 'ACT'
# MAGIC     WHEN 'South Australia' THEN 'SA'
# MAGIC     WHEN 'Western Australia' THEN 'WA'
# MAGIC     WHEN 'Queensland' THEN 'QLD'
# MAGIC     ELSE 'OTH'
# MAGIC     END AS State_Abbr,
# MAGIC   Postcode as Postcode,
# MAGIC   Value as Rent_payment
# MAGIC   FROM rent_pymt__bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.abs_combined
# MAGIC LOCATION '/mnt/domain_data/silver/combined/delta'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   a.State as State,
# MAGIC   CASE a.State 
# MAGIC     WHEN 'New South Wales' THEN 'NSW' 
# MAGIC     WHEN 'Victoria' THEN 'VIC'
# MAGIC     WHEN 'Tasmania' THEN 'TAS'
# MAGIC     WHEN 'Australian Capital Territory' THEN 'ACT'
# MAGIC     WHEN 'South Australia' THEN 'SA'
# MAGIC     WHEN 'Western Australia' THEN 'WA'
# MAGIC     WHEN 'Queensland' THEN 'QLD'
# MAGIC     ELSE 'OTH'
# MAGIC     END AS State_Abbr,
# MAGIC   a.Postcode as Postcode,
# MAGIC   a.Value as Median_Age,
# MAGIC   c.Value as Median_Income,
# MAGIC   d.value as Mortgage_Payment,
# MAGIC   e.value as Rent_Payment
# MAGIC FROM med_age__bronze a
# MAGIC FULL OUTER JOIN med_income__bronze c ON a.Postcode = c.Postcode
# MAGIC FULL OUTER JOIN mortgage_pymt__bronze d ON a.Postcode = d.Postcode
# MAGIC FULL OUTER JOIN rent_pymt__bronze e ON a.Postcode = e.Postcode
# MAGIC WHERE a.med_age_key IS NOT NULL;

# Databricks notebook source
def get_schema_tables(schema):
    data = spark.sql(f"""
        show tables from {schema}
    """)
    return [i['tableName'] for i in data.collect()]

# COMMAND ----------

def create_domain_silver():
    spark.sql("""
        CREATE OR REPLACE TABLE silver.domain_sales_listings (
            listing_sk string,
            id string,
            propertyDetailsUrl string,
            propertyType string,
            result string,
            listingWeekEnding date,
            unitNumber string,
            streetNumber string,
            streetName string,
            streetType string,
            suburb string,
            state string,
            postcode string,
            price int,
            latitude float,
            longitude float,
            insert_datetime timestamp
        )
    """)

# COMMAND ----------

def get_last_sunday(date):
    import datetime
    from dateutil.relativedelta import relativedelta

    week = date.isocalendar().week - 1
    year = date.isocalendar().year
    date = datetime.date(year, 1, 1) + relativedelta(weeks=+week)
    if date.weekday() == 6:
        return datetime.datetime.strftime(date, '%Y-%m-%d')
    else:
        diff = 6 - date.weekday()
        return datetime.datetime.strftime(date + datetime.timedelta(days = diff), '%Y-%m-%d')
    print(date.weekday())

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC ## Transforming data and running data quality tests with PyDeequ

# COMMAND ----------

import great_expectations as ge
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import (
    countDistinct,
    col,
    isnull,
    to_date,
    to_timestamp,
    date_format,
)

# COMMAND ----------

# read dataframe(s)
df = spark.read.table("openaq_barcelona_bronze")
display(df.describe())

# converting timestamps to dates and times

df = (
    df.withColumn("date_utc", to_date("date_utc"))
    .withColumn("time_utc", date_format("date_utc", "HH:mm:ss"))
    .withColumn("date_local", to_date("date_local"))
    .withColumn("time_local", date_format("date_local", "HH:mm:ss"))
)

display(df)
df.createOrReplaceTempView("my_view")
spark.sql("SELECT DISTINCT parameter from my_view").show()

# COMMAND ----------

# Creating GE objects
df_ge = ge.dataset.SparkDFDataset(df)

# Define some expectations
df_ge.expect_column_values_to_be_in_set("parameter", ["so2", "co", "o3", "no2", "pm25"])

df_ge.expe

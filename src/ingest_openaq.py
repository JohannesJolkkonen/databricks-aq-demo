# Databricks notebook source
# MAGIC %md
# MAGIC ## Get air quality data from OpenAQ API

# COMMAND ----------

import requests
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    BooleanType,
)

# COMMAND ----------

# OpenAQ REST API endpoint
start_date = "2023-01-01"
end_date = "2023-06-28"
base_url = "https://api.openaq.org/v2/measurements"
page = 1
json_data_list = []

while True:
    print(f"fetching page {page}")
    params = {
        "limit": 1000,
        "counztry": "ES",
        "city": "Barcelona",
        "date_from": start_date,
        "date_to": end_date,
        "page": page,
    }
    response = requests.get(base_url, params=params)
    json_data = response.json()
    results = json_data.get("results", [])

    if not results:
        break

    json_data_list.extend(results)
    page += 1


json_data = json.dumps({"results": json_data_list})

df = spark.read.json(spark.sparkContext.parallelize([json_data]))

df = df.select(explode("results").alias("results"))

df = df.select(
    col("results.locationId").alias("locationId"),
    col("results.location").alias("location"),
    col("results.parameter").alias("parameter"),
    col("results.value").alias("value"),
    col("results.date.utc").alias("date_utc"),
    col("results.date.local").alias("date_local"),
    col("results.unit").alias("unit"),
    col("results.coordinates.latitude").alias("latitude"),
    col("results.coordinates.longitude").alias("longitude"),
    col("results.country").alias("country"),
    col("results.city").alias("city"),
    col("results.isMobile").alias("isMobile"),
    col("results.isAnalysis").alias("isAnalysis"),
    col("results.entity").alias("entity"),
    col("results.sensorType").alias("sensorType"),
)
display(df)

# Store DF into delta table
df.write.mode("overwrite").saveAsTable("openaq_barcelona_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC ## Get air quality data from OpenAQ API
# MAGIC CREATE   TABLE   air_quality   (
# MAGIC   city_id   INT,
# MAGIC   date   DATE,
# MAGIC   pollutant   VARCHAR(50),
# MAGIC   concentration   FLOAT,
# MAGIC   PRIMARY KEY   (city_id,   date,   pollutant)
# MAGIC );
# MAGIC
# MAGIC INSERT   INTO   air_quality   (city_id,   date,   pollutant,   concentration)
# MAGIC VALUES
# MAGIC (1,   '2023-07-01',   'PM2.5',   10.5),
# MAGIC (1,   '2023-07-01',   'PM10',   20.3),
# MAGIC (2,   '2023-07-01',   'PM2.5',   8.2),
# MAGIC (2,   '2023-07-01',   'PM10',   15.1),
# MAGIC (3,   '2023-07-01',   'PM2.5',   12.7),
# MAGIC (3,   '2023-07-01',   'PM10',   18.6);
# MAGIC
# MAGIC CREATE   TABLE   population   (
# MAGIC   city_id   INT,
# MAGIC   year   INT,
# MAGIC   population   INT,
# MAGIC   PRIMARY KEY   (city_id,   year)
# MAGIC );
# MAGIC
# MAGIC INSERT   INTO   population   (city_id,   year,   population)
# MAGIC VALUES
# MAGIC (1,   2023,   1000000),
# MAGIC (2,   2023,   750000),
# MAGIC (3,   2023,   500000);
# MAGIC
# MAGIC SELECT   a.city_id,   a.date,   a.pollutant,   a.concentration,   p.population
# MAGIC FROM   air_quality   a
# MAGIC JOIN   population   p   ON   a.city_id   =   p.city_id
# MAGIC WHERE   a.date   =   '2023-07-01'
# MAGIC ORDER   BY   a.city_id;

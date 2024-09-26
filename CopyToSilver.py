# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession


# COMMAND ----------

spark = SparkSession.builder.appName("CopyToSilverAsDelta").getOrCreate()

# COMMAND ----------

input_file_name = dbutils.widgets.get('input_file_name')


# COMMAND ----------

df = spark.read.parquet(
    f"/Volumes/main/default/bronze_vol/{input_file_name}",
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True
)

# COMMAND ----------

output_file_name = "/Volumes/main/default/silver_vol/staging_parquet"

df.write.format('parquet') \
    .option("path", output_file_name) \
    .mode("overwrite") \
    .save()
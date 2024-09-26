# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, to_date, round, format_number, date_format, lit, year, month, dayofmonth, monotonically_increasing_id, cast,split
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql import DataFrame

# COMMAND ----------

spark = SparkSession.builder \
    .appName("TESTING") \
    .getOrCreate()

# COMMAND ----------

currency = spark.sql("SELECT * FROM tables.gold.currency_dim")
calendar = spark.sql("SELECT * FROM tables.gold.calendar_dim")
counter_party = spark.sql("SELECT * FROM tables.gold.counter_party_dim")
transactions = spark.sql("SELECT * FROM tables.gold.transactions")
category = spark.sql("SELECT * FROM tables.gold.category_dim")


# COMMAND ----------

dataframes = [calendar, currency, category, counter_party, transactions]
names = ["calendar", "currency", "category", "counter_party", "transactions"]

for name, df in zip(names, dataframes):
    print(f"Count of {name}: {df.count()}")

# COMMAND ----------

currency = spark.sql("SELECT * FROM tables.gold.currency_dim")
calendar = spark.sql("SELECT * FROM tables.gold.calendar_dim")
counter_party = spark.sql("SELECT * FROM tables.gold.counter_party_dim")
transactions = spark.sql("SELECT * FROM tables.gold.transactions")
category = spark.sql("SELECT * FROM tables.gold.category_dim")

dataframes = [calendar, currency, category, counter_party, transactions]
names = ["calendar", "currency", "category", "counter_party", "transactions"]

for name, df in zip(names, dataframes):
    print(f"Count of {name}: {df.count()}")

# COMMAND ----------


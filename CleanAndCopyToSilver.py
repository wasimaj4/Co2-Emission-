# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
from pyspark.sql import functions as F


# COMMAND ----------

spark = SparkSession.builder.appName("CleanAndCopyToSilverAsDelta").getOrCreate()


# COMMAND ----------

input_file_name = dbutils.widgets.get('input_file_name')
result_path = dbutils.widgets.get("result_path")


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

f = open(result_path)
result_list = json.load(f)

# COMMAND ----------

delete_these = []

for expectation in result_list['results']:
    indices = expectation['result']['partial_unexpected_index_list']
    for index in indices:
        if index not in delete_these:
            delete_these.append(index)


df = df.toPandas().drop(delete_these, axis=0) 


# COMMAND ----------

df = spark.createDataFrame(df) 

# COMMAND ----------

df = df.withColumn("ctpty_adr_line2" , F.lit(None).cast("string"))
df = df.withColumn("rmt_inf_ustrd2" , F.lit(None).cast("string"))
df = df.withColumn("tx_tp" , F.lit(None).cast("string"))

# COMMAND ----------

output_file_name = "/Volumes/main/default/silver_vol/staging_parquet"
df.write.format('parquet') \
    .option("path", output_file_name) \
    .mode("overwrite") \
    .save()

# save the silver data manually to main>silver_vol 

# COMMAND ----------

# create external tables in "abfss://files@datalakebc01.dfs.core.windows.net/external_tables/<FOLDER_NAME>/<TABLE_NAME>
# FOLDER_NAME can be "silver" or "gold"
# if TABLE_NAME folder exists it will be overwritten, if not a new file will be craeted 


# df_weird.write.mode("OVERWRITE").format("parquet").option("path", "abfss://files@datalakebc01.dfs.core.windows.net/external_tables/silver/transactions").saveAsTable("tables.silver.transaction_staging") 

# COMMAND ----------

# currency_id , currency , currency_name, exchange_rate 

# catagory_id , catagory_code , catagory_name , CO2_emissions_multiplier
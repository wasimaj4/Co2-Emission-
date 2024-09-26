# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, to_date, round, format_number, date_format, lit, year, month, dayofmonth, monotonically_increasing_id, cast,split
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType


# COMMAND ----------


# "acct_ccy"
# "bookg_amt"
# "bookg_amt_nmrc"
# "bookg_dt_tm_cet"
# "booking_id"
# "ctpty_ctry"
# "ctpty_nm"
# "dtld_tx_tp"
# "ctpty_adr_line1"

# COMMAND ----------

def reorder_columns(df: DataFrame, partially_desired_order: list) -> DataFrame:
    all_columns = df.columns
    remaining_columns = [col for col in all_columns if col not in partially_desired_order]
    new_order = partially_desired_order + remaining_columns
    df_reordered = df.select(*new_order)
    return df_reordered

# COMMAND ----------

spark = SparkSession.builder \
    .appName("DataProcessing") \
    .getOrCreate()

transactions_file_path ="/Volumes/main/default/silver_vol/staging_parquet/*.parquet"

# exc_file_path = "abfss://files@datalakebc01.dfs.core.windows.net/external_location/raw/currency_exchange_rates.csv"
category_file_path = "abfss://files@datalakebc01.dfs.core.windows.net/external_location/raw/category_map.csv"
df_transactions = spark.read.parquet(
    transactions_file_path,
    header=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    
)

df_categories = spark.read.csv(
    category_file_path,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True
)
df_transactions.count()
df_categories.count()

# COMMAND ----------

df_transactions = df_transactions.drop(*[
    "bal_aftr_bookg","bal_aftr_bookg_nmrc", "bookg_cdt_dbt_ind", "bookg_dt_tm_gmt","acct_id","card_poi_id", "cdtr_schme_id", "ctpty_acct_id_iban","ctpty_acct_id_bban","ctpty_acct_ccy", "ctpty_adr_line2", "ctpty_agt_bic","ntry_seq_nb", "rmt_inf_ustrd1", "rmt_inf_ustrd2","tx_acct_svcr_ref","tx_tp","year_month","end_to_end_id", "booking_id", "bookg_amt_nmrc",
])


# COMMAND ----------

df_transactions.printSchema

# COMMAND ----------


df_transactions = df_transactions \
    .withColumnRenamed("acct_ccy", "currency") \
    .withColumnRenamed("bookg_amt", "amount_euro") \
    .withColumnRenamed("bookg_amt_nmrc", "amount_cent") \
    .withColumnRenamed("bookg_dt_tm_cet", "payment_date") \
    .withColumnRenamed("booking_id", "payment_id") \
    .withColumnRenamed("ctpty_ctry", "counterparty_country") \
    .withColumnRenamed("ctpty_nm", "counterparty_name") \
    .withColumnRenamed("dtld_tx_tp", "category_code") \
    .withColumnRenamed("ctpty_adr_line1", "counterparty_add")

df_transactions.printSchema()

# COMMAND ----------

df_transactions = df_transactions.withColumn("payment_date", to_date(date_format(col("payment_date"), "dd-MM-yyyy"),"dd-MM-yyyy"))
df_transactions.display()


# COMMAND ----------

df_calendar = df_transactions \
    .select(
        col("payment_date"),
        year(col("payment_date")).alias("year"),
        month(col("payment_date")).alias("month"),
        dayofmonth(col("payment_date")).alias("day")
    ) \
    .dropDuplicates() \
    .orderBy(col("payment_date"))
df_calendar = df_calendar.withColumn("year", col("year").cast("int"))
df_calendar = df_calendar.withColumn("month", col("month").cast("int"))
df_calendar = df_calendar.withColumn("day", col("day").cast("int"))


# COMMAND ----------

df_calendar.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Translate

# COMMAND ----------

# import pandas as pd
# from langdetect import detect
# from googletrans import Translator


# df_transactions_pd = df_transactions.toPandas()

# translator = Translator()
# cache = {}

# def translate_if_not_english(text):
#     if text in cache:
#         return cache[text]
#     try:
#         lang = detect(text)
#         if lang in ['zh', 'ja', 'ko']:
#             translated = translator.translate(text, src='auto', dest='en').text
#             cache[text] = translated
#             return translated
#         else:
#             return text
#     except Exception as e:
#         return text

# df_transactions_pd['counterparty_name'] = df_transactions_pd['counterparty_name'].apply(translate_if_not_english)
# df_transactions_pd['counterparty_add'] = df_transactions_pd['counterparty_add'].apply(translate_if_not_english)

# df_transactions = spark.createDataFrame(df_transactions_pd)
# display(df_transactions.limit(10))

# COMMAND ----------

conversion_rates = [
    ('USD', 0.85),  
    ('GBP', 1.10),  
    ('EUR', 1.00),
    ('J', 0.0062),
    ('CAD', 0.67)
]

df_exc_rates = spark.createDataFrame(conversion_rates, ['currency', 'conversion_rate'])


# COMMAND ----------

df_counter_parties = df_transactions \
    .select(
        ["counterparty_add",
        "counterparty_country",
        "counterparty_name"]
    ).dropDuplicates()

# COMMAND ----------


# df_cleaned  = df_with_rates.withColumn(
#     'amount_euro',
#     when(col('currency') != 'EUR', round(col('amount_euro') * col('conversion_rate'),3)).otherwise(col('amount_euro'))
# ).withColumn(
#     'amount_cent',
#     when(col('currency') != 'EUR', round(col('amount_cent') * col('conversion_rate'),3)).otherwise(col('amount_cent'))
# ).withColumn(
#     'currency',
#     when(col('currency') != 'EUR', 'EUR').otherwise(col('currency'))
# )

# df_with_rates = df_cleaned.join(df_with_rates, on='currency', how='left')
# display(df_cleaned.limit(50))


# COMMAND ----------



# COMMAND ----------

print( "df_transactions" , df_transactions.columns)
print( "df_exc_rates" , df_exc_rates.columns)
print( "df_calendar" , df_calendar.columns)
print( "df_counter_parties" , df_counter_parties.columns)
print( "df_categories" , df_categories.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC TOMORROW

# COMMAND ----------

"df_transactions" , df_transactions.printSchema()
"df_exc_rates" , df_exc_rates.printSchema()
"df_calendar" , df_calendar.printSchema()
"df_counter_parties" , df_counter_parties.printSchema()
"df_categories" , df_categories.printSchema()

# COMMAND ----------


silver_file_root = "abfss://files@datalakebc01.dfs.core.windows.net/external_tables/silver"
gold_file_root = "abfss://files@datalakebc01.dfs.core.windows.net/external_tables/gold"


df_transactions.write.mode("OVERWRITE") \
    .option("path", f"{silver_file_root}/sta_transactions") \
    .saveAsTable("tables.silver.sta_transactions")

df_calendar.write.mode("OVERWRITE") \
    .option("path", f"{silver_file_root}/sta_calendar_dim")\
    .saveAsTable("tables.silver.sta_calendar_dim")

df_counter_parties.write.mode("OVERWRITE") \
    .option("path", f"{silver_file_root}/sta_counter_party_dim") \
    .saveAsTable("tables.silver.sta_counter_party_dim")

df_categories.write.mode("OVERWRITE") \
    .option("path", f"{silver_file_root}/sta_category_dim") \
    .saveAsTable("tables.silver.sta_category_dim")

df_exc_rates.write.mode("OVERWRITE") \
    .option("path", f"{silver_file_root}/sta_currency_dim") \
    .saveAsTable("tables.silver.sta_currency_dim")



# COMMAND ----------

# co2_emission = df_cleaned.join(category_dim, on='category_code', how='left')


# co2_emission = co2_emission \
#     .withColumn("co2_emission_g", col("amount_euro") * col("co2_multiplier")) \
#     .withColumn("co2_emission_g", round(col("co2_emission_g"), 2)) \
#     .filter(col("category_name") != "Aircarriers airlines not elsewhere classified") \
#     .filter(col("counterparty_country") == "The Netherlands")
# co2_emission = co2_emission.select(
#     col("co2_emission_g"),
#     col("amount_euro"),
#     col("category_code"),
#     col("category_name"),
#     col("co2_multiplier")
# )

# display(co2_emission.limit(10))

# COMMAND ----------

# from pyspark.sql.functions import col, round

# co2_emission = co2_emission \
#     .withColumn("co2_emission_g", round(col("amount_euro") * col("co2_multiplier"), 2))

# co2_emission_pd = co2_emission.toPandas()


# COMMAND ----------

# import matplotlib.pyplot as plt
# import seaborn as sns

# sns.set(style="whitegrid")

# plt.figure(figsize=(10, 6))
# sns.barplot(data=co2_emission_pd, x='category_name', y='co2_emission_g', errorbar=None)
# plt.xticks(rotation=45, ha='right')
# plt.title('CO2 Emission by Category in The Netherlands')
# plt.xlabel('Category')
# plt.ylabel('CO2 Emission (g)')
# plt.tight_layout()
# plt.show()

# plt.figure(figsize=(10, 6))
# sns.scatterplot(data=co2_emission_pd, x='amount_euro', y='co2_emission_g', hue='category_name')
# plt.title('CO2 Emission vs. Amount by Category')
# plt.xlabel('Amount (€)')
# plt.ylabel('CO2 Emission (g)')
# plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.tight_layout()
# plt.show()

# print(co2_emission_pd.head(10))

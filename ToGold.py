# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, to_date, round, format_number, date_format, lit, year, month, dayofmonth, monotonically_increasing_id, cast,split
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql import DataFrame
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window

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

# COMMAND ----------

# def merge_tables( gold , silver , column_id_name , comparison_column ):

#     window_spec = Window.orderBy(monotonically_increasing_id())
#     if column_id_name is not None:
#         silver = silver.withColumn(column_id_name , lit(None))
#     temp = gold.union(silver)
#     print("temp")
#     display(temp)
#     if comparison_column is not None:
#         temp = temp.drop_duplicates(subset = [comparison_column])
#     print("temp_after")
#     display(temp)
#     temp_with_ids = temp.coalesce(1).withColumn(column_id_name, row_number().over(window_spec))
#     temp_with_ids = temp_with_ids.withColumn(column_id_name, col(column_id_name).cast("int"))
#     return temp_with_ids

# def add_id_column(silver, column_id_name):
#     silver = silver.withColumn(column_id_name , monotonically_increasing_id()+1)
#     silver = silver.withColumn(column_id_name , col(column_id_name).cast("int"))
#     return silver

# def create_or_merge(gold_table , silver_table , column_id_name , comparison_column ):
#     if spark.catalog.tableExists(gold_table):
#         gold = spark.sql(f"SELECT * FROM {gold_table}")
#         silver = spark.sql(f"SELECT * FROM {silver_table}")
#         merged_dim = merge_tables(gold, silver, column_id_name ,comparison_column )
#         merged_dim.limit(2).display()
#         return merged_dim
#     else:
#         silver = spark.sql(f"SELECT * FROM {silver_table}")
#         return add_id_column(silver, column_id_name)


# COMMAND ----------

# calendar_dim = create_or_merge(gold_table= 'tables.gold.calendar_dim' , silver_table= 'tables.silver.sta_calendar_dim' , column_id_name="date_id" , comparison_column="payment_date" )

# currency_dim = create_or_merge(gold_table="tables.gold.currency_dim" , silver_table="tables.silver.sta_currency_dim", column_id_name="currency_id" , comparison_column="currency" )

# category_dim = create_or_merge(gold_table="tables.gold.category_dim" , silver_table="tables.silver.sta_category_dim", column_id_name="category_id" , comparison_column="category_code"  )

# counter_party_dim = create_or_merge(gold_table="tables.gold.counter_party_dim" , silver_table="tables.silver.sta_counter_party_dim" , column_id_name="counterparty_id" , comparison_column="counterparty_add" )




# COMMAND ----------

def silver_with_id( gold_table_name , silver_table_name , column_id_name ):
    
    gold = spark.table(gold_table_name)
    max_id_in_gold = gold.agg({f"{column_id_name}": "max"}).collect()[0][0]
    
    if max_id_in_gold is None:
        max_id_in_gold = 0
    
    silver = spark.table(silver_table_name)
    silver = silver.coalesce(1).withColumn(column_id_name , monotonically_increasing_id() + 1 +max_id_in_gold)
    silver = silver.withColumn(column_id_name , col(column_id_name).cast("int"))
    silver.printSchema()
    return silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### CURRENCY
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tables.gold.currency_dim (
# MAGIC     currency STRING,
# MAGIC     conversion_rate DOUBLE,
# MAGIC     currency_id INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://files@datalakebc01.dfs.core.windows.net/external_tables/gold/currency_dim';

# COMMAND ----------

gold_table_name = "tables.gold.currency_dim"
silver_table_name = "tables.silver.sta_currency_dim"

gold = spark.sql(f"SELECT * FROM {gold_table_name}").createOrReplaceTempView("gold_temp")
silver = silver_with_id(gold_table_name , silver_table_name , "currency_id").createOrReplaceTempView("silver_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_temp AS g
# MAGIC USING silver_temp AS s
# MAGIC ON g.currency = s.currency
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     currency,
# MAGIC     conversion_rate,
# MAGIC     currency_id
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.currency,
# MAGIC     s.conversion_rate,
# MAGIC     s.currency_id
# MAGIC   )
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### CALENDAR

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tables.gold.calendar_dim (
# MAGIC     payment_date DATE,
# MAGIC     year INT,
# MAGIC     month INT,
# MAGIC     day INT,
# MAGIC     date_id INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://files@datalakebc01.dfs.core.windows.net/external_tables/gold/calendar_dim';

# COMMAND ----------

gold_table_name = "tables.gold.calendar_dim"
silver_table_name = "tables.silver.sta_calendar_dim"

gold = spark.sql(f"SELECT * FROM {gold_table_name}").createOrReplaceTempView("gold_temp")
silver = silver_with_id(gold_table_name , silver_table_name , "date_id").createOrReplaceTempView("silver_temp")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_temp AS g
# MAGIC USING silver_temp AS s
# MAGIC ON g.payment_date = s.payment_date
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     payment_date,
# MAGIC     year,
# MAGIC     month,
# MAGIC     day,
# MAGIC     date_id
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.payment_date,
# MAGIC     s.year,
# MAGIC     s.month,
# MAGIC     s.day,
# MAGIC     s.date_id
# MAGIC   )
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### CATEGORY

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tables.gold.category_dim (
# MAGIC     category_code INT,
# MAGIC     category_name STRING,
# MAGIC     co2_multiplier DOUBLE,
# MAGIC     category_id INT)
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://files@datalakebc01.dfs.core.windows.net/external_tables/gold/category_dim';

# COMMAND ----------

gold_table_name = "tables.gold.category_dim"
silver_table_name = "tables.silver.sta_category_dim"

gold = spark.sql(f"SELECT * FROM {gold_table_name}").createOrReplaceTempView("gold_temp")
silver = silver_with_id(gold_table_name , silver_table_name , "category_id").createOrReplaceTempView("silver_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_temp AS g
# MAGIC USING silver_temp AS s
# MAGIC ON g.category_code = s.category_code
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     category_code,
# MAGIC     category_name,
# MAGIC     co2_multiplier,
# MAGIC     category_id
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.category_code,
# MAGIC     s.category_name,
# MAGIC     s.co2_multiplier,
# MAGIC     s.category_id
# MAGIC   )
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### COUNTER PARTY

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tables.gold.counter_party_dim (
# MAGIC     counterparty_add STRING,
# MAGIC     counterparty_country STRING,
# MAGIC     counterparty_name STRING,
# MAGIC     counterparty_id INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://files@datalakebc01.dfs.core.windows.net/external_tables/gold/counter_party_dim';

# COMMAND ----------

gold_table_name = "tables.gold.counter_party_dim"
silver_table_name = "tables.silver.sta_counter_party_dim"

gold = spark.sql(f"SELECT * FROM {gold_table_name}").createOrReplaceTempView("gold_temp")
silver = silver_with_id(gold_table_name , silver_table_name , "counterparty_id").createOrReplaceTempView("silver_temp")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_temp AS g
# MAGIC USING silver_temp AS s
# MAGIC ON g.counterparty_add = s.counterparty_add
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     counterparty_add,
# MAGIC     counterparty_country,
# MAGIC     counterparty_name,
# MAGIC     counterparty_id
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.counterparty_add,
# MAGIC     s.counterparty_country,
# MAGIC     s.counterparty_name,
# MAGIC     s.counterparty_id
# MAGIC   )
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSACTIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tables.gold.transactions (
# MAGIC     currency_id INT,
# MAGIC     date_id INT,
# MAGIC     counterparty_id INT,
# MAGIC     category_id INT,
# MAGIC     amount_euro DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://files@datalakebc01.dfs.core.windows.net/external_tables/gold/transactions';

# COMMAND ----------

gold_table_name = "tables.gold.transactions"
silver_table_name = "tables.silver.sta_transactions"

gold = spark.sql(f"SELECT * FROM {gold_table_name}").createOrReplaceTempView("gold_temp")
silver = spark.sql(f"SELECT * FROM {silver_table_name}").createOrReplaceTempView("silver_temp")


# COMMAND ----------

silver = spark.sql("""SELECT
    s.amount_euro,
    cur.currency_id,
    cal.date_id,
    cat.category_id,
    cp.counterparty_id
FROM 
    silver_temp AS s
LEFT JOIN 
    tables.gold.currency_dim AS cur ON s.currency = cur.currency
LEFT JOIN 
    tables.gold.calendar_dim AS cal ON s.payment_date = cal.payment_date
LEFT JOIN 
    tables.gold.category_dim AS cat ON s.category_code = cat.category_code
LEFT JOIN 
    tables.gold.counter_party_dim AS cp ON s.counterparty_add = cp.counterparty_add;""").createOrReplaceTempView("silver_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold_temp
# MAGIC SELECT *
# MAGIC FROM silver_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC ### PREVIOUS IMPLEMENTATION

# COMMAND ----------

# sta_transactions = spark.sql("SELECT * FROM tables.silver.sta_transactions")
# sta_transactions.printSchema()
# sta_transactions.limit(5).display()


# COMMAND ----------

# sta_transactions = sta_transactions \
#     .drop(*["counterparty_country" , "counterparty_name" , "counterparty_code"])
# display(sta_transactions.limit(2))

# COMMAND ----------

# sta_transactions = sta_transactions \
#     .join(currency_dim, on='currency', how='left') \
#     .join(calendar_dim, on='payment_date', how='left') \
#     .join(category_dim , on='category_code', how='left') \
#     .join(counter_party_dim , on='counterparty_add' , how='left')


# sta_transactions.limit(2).display()

# COMMAND ----------

# sta_transactions = sta_transactions \
#     .select([col("currency_id") , col("date_id") , col("counterparty_id"), col("category_ID"), col("amount_euro")])
# display(sta_transactions.limit(2))

# sta_transactions.printSchema()

# if spark.catalog.tableExists("tables.gold.transactions"):
#     transactions = spark.table("tables.gold.transactions").printSchema()

# COMMAND ----------

# gold_file_root = "abfss://files@datalakebc01.dfs.core.windows.net/external_tables/gold"

# if not spark.catalog.tableExists("tables.gold.transactions"):
#     print("creating gold table")
#     sta_transactions.write \
#         .option("path", f"{gold_file_root}/transactions") \
#         .saveAsTable("tables.gold.transactions")

#     calendar_dim.write \
#         .option("path", f"{gold_file_root}/calendar_dim")\
#         .saveAsTable("tables.gold.calendar_dim")

#     counter_party_dim.write \
#         .option("path", f"{gold_file_root}/counter_party_dim") \
#         .saveAsTable("tables.gold.counter_party_dim")

#     category_dim.write \
#         .option("path", f"{gold_file_root}/category_dim") \
#         .saveAsTable("tables.gold.category_dim")

#     currency_dim.write \
#         .option("path", f"{gold_file_root}/currency_dim") \
#         .saveAsTable("tables.gold.currency_dim")
# else :
#     print("adding new data to gold table")

#     sta_transactions.write.mode("APPEND") \
#         .option("path", f"{gold_file_root}/transactions") \
#         .saveAsTable("tables.gold.transactions") \
        

#     calendar_dim.write.mode("OVERWRITE") \
#         .option("path", f"{gold_file_root}/calendar_dim") \
#         .saveAsTable("tables.gold.calendar_dim")
        

#     counter_party_dim.write.mode("OVERWRITE") \
#         .option("path", f"{gold_file_root}/counter_party_dim") \
#         .saveAsTable("tables.gold.counter_party_dim")

      
#     category_dim.write.mode("OVERWRITE") \
#         .option("path", f"{gold_file_root}/category_dim") \
#         .saveAsTable("tables.gold.category_dim")


#     currency_dim.write.mode("OVERWRITE") \
#         .option("path", f"{gold_file_root}/currency_dim") \
#         .saveAsTable("tables.gold.currency_dim")
        
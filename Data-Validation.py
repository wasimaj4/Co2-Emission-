# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
import great_expectations as gx
from pyspark.sql.functions import col

import pyspark
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.exceptions.exceptions import GreatExpectationsError
from pyspark.sql.functions import col
# https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/?results=result_format_keys

# COMMAND ----------

input_file_name = dbutils.widgets.get('input_file_name')
# input_file_name ="raw_data12024-09-03T19:30:35.3573904Z_bronze.parquet"

# COMMAND ----------

spark = SparkSession.builder.appName("validataion").getOrCreate()

# COMMAND ----------

# df = spark.read.option('multiline',True).parquet(f'dbfs:/Volumes/main/default/bronze_vol/{inputs}')
df = spark.read.option('multiline',True).parquet(f'dbfs:/Volumes/main/default/bronze_vol/{input_file_name}').toPandas()

# COMMAND ----------

df.columns

# COMMAND ----------

context = gx.get_context(mode = 'file')
data_source = context.data_sources.add_or_update_pandas(name='transaction_data_source')
data_asset = data_source.add_dataframe_asset(name="transaction_dataframe_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe("transaction_batch_definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe":df})


# COMMAND ----------

expectation_suite = gx.ExpectationSuite('expectations_suite')

booking_amount_expectation = gx.expectations.ExpectColumnValuesToBeBetween(column="bookg_amt" , min_value = 0 , max_value = None)
booking_amount_numeric_expectation = gx.expectations.ExpectColumnValuesToBeBetween(column="bookg_amt_nmrc" , min_value = 0 , max_value = None)
currency_expectation = gx.expectations.ExpectColumnValuesToBeInSet(column="acct_ccy" , value_set = ["EUR", "USD", "GBP", "J","CAD"])
category_expectation = gx.expectations.ExpectColumnValuesToBeInSet(column="dtld_tx_tp" , value_set = [ 2222, 3333, 4111, 4112, 4121, 4131, 4511, 5411, 5420, 5422, 5462, 5541, 5542, 5912, 6542, 6630, 7512]
)

date_expectation1 = gx.expectations.ExpectColumnValuesToNotBeNull(column="bookg_dt_tm_gmt")
date_expectation2 = gx.expectations.ExpectColumnValuesToNotBeNull(column="bookg_dt_tm_cet")

expectation_suite.add_expectation(booking_amount_expectation)
expectation_suite.add_expectation(booking_amount_numeric_expectation)
expectation_suite.add_expectation(currency_expectation)
expectation_suite.add_expectation(date_expectation1)
expectation_suite.add_expectation(date_expectation2)

# COMMAND ----------

validation_definition_dict = {"data":batch_definition, "suite":expectation_suite,"name":"transaction_validation_definition"}
#,"result_format": "COMPLETE","unexpected_index_column_names": ["index"]}

validation_definition = gx.ValidationDefinition(**validation_definition_dict )

# COMMAND ----------

results = batch.validate(expectation_suite)


# COMMAND ----------

# TODO Save results_dict as a JSON file




# COMMAND ----------

import json
results_dict = results.to_json_dict()
results_dict.pop('meta')
# unexpected_values = {
#     "booking_amount_unexpected": results_dict['results'][0]['result'].get('unexpected_list', []),
#     "booking_amount_numeric_unexpected": results_dict['results'][1]['result'].get('unexpected_list', []),
#     "currency_unexpected": results_dict['results'][2]['result'].get('unexpected_list', [])
# }
# results_dict['unexpected_values'] = unexpected_values

result_path = f'/Volumes/main/default/configs/gx_configs/{input_file_name.replace("bronze.parquet", "_validation")}.json'
# Save results_dict as a JSON file
with open(result_path, 'w') as f:
    json.dump(results_dict, f)

output = {'success' : results_dict['success'] , 'result_path' : result_path }
dbutils.notebook.exit(json.dumps(output))

# COMMAND ----------


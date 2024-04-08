# Databricks notebook source
# MAGIC %run /Workspace/deployments/ireland_housing_demand/utils

# COMMAND ----------

from pyspark.sql.functions import col, round, lit
import re

# COMMAND ----------

pp_df_1 = spark.sql("SELECT age_group, `year`, `value` * 1000 AS `value`  FROM cso.population_projection_1_2016_2051 WHERE `year` > 2021 AND criteria_for_projection = 'Method - M1F1' AND sex = 'Both sexes' AND age_group IN ('25 - 29 years', '30 - 34 years') ")
pp_df_1.persist()

# COMMAND ----------

dwellings_df_1 = spark.sql("SELECT census_year AS `year`, detached_house_proportion, semi_detached_house_proportion, flat_or_apartment_in_a_purpose_built_block_proportion FROM report.dwellings WHERE census_year = 2022")
dwellings_df_dict = dwellings_df_1.collect()[0].asDict()

# COMMAND ----------

dwellings_df_dict

# COMMAND ----------

headship_df = spark.sql("SELECT age_group_of_reference_person AS age_group, headship_rate FROM report.headship WHERE census_year = 2022 ")
headship_df.persist()

# COMMAND ----------

pp_df_1 = pp_df_1.join(headship_df, ['age_group'])

# COMMAND ----------

result_df = (pp_df_1
             .withColumn('detached_house_prediction', round(col('value') * col('headship_rate') * dwellings_df_dict['detached_house_proportion'], 2))
             .withColumn('semi_detached_house_prediction', round(col('value') * col('headship_rate') * dwellings_df_dict['semi_detached_house_proportion'], 2))
             .withColumn('flat_or_apartment_in_a_purpose_built_block_prediction', round(col('value') * col('headship_rate') * dwellings_df_dict['flat_or_apartment_in_a_purpose_built_block_proportion'], 2))
            )
result_df = result_df.withColumn('total_prediction', round(col('detached_house_prediction') + col('semi_detached_house_prediction') + col('flat_or_apartment_in_a_purpose_built_block_prediction'), 2))
result_df.display()

# COMMAND ----------

createDeltaTable(result_df, 'report', 'demand_prediction', '/mnt/dbstables/report/demand_prediction')

# COMMAND ----------


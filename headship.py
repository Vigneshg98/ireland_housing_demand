# Databricks notebook source
from pyspark.sql.functions import col
import re

# COMMAND ----------

data_df = spark.sql("SELECT statistic_label, census_year, age_group_of_reference_person, `value` FROM cso.private_households_4_2011_2022 WHERE marital_status_of_reference_person = 'All marital status' AND sex_of_reference_person = 'All reference persons' ")
data_df.persist()

# COMMAND ----------

households_df = data_df.filter(col('statistic_label') == 'Total private households')
households_df = households_df.withColumnRenamed('value', 'households_value').drop('statistic_label')

persons_df = data_df.filter(col('statistic_label') == 'All persons in private households')
persons_df = persons_df.withColumnRenamed('value', 'persons_value').drop('statistic_label')

# COMMAND ----------

headship_df = households_df.join(persons_df, ['census_year', 'age_group_of_reference_person'])
headship_df = headship_df.withColumn('headship_rate', col('households_value')/ col('persons_value'))
headship_df = headship_df.drop(*['households_value','persons_value'])
# headship_df = headship_df.groupBy("census_year").pivot("age_group_of_reference_person").agg({"value": "first"})

# COMMAND ----------

headship_df.display()

# COMMAND ----------

# MAGIC %run /Workspace/Users/23079924@studentmail.ul.ie/ireland_housing_demand/utils

# COMMAND ----------

createDeltaTable(headship_df, 'report', 'headship', '/mnt/dbstables/report/headship')

# COMMAND ----------


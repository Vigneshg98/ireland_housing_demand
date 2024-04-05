# Databricks notebook source
from pyspark.sql.functions import col
import re

# COMMAND ----------

data_df = spark.sql("SELECT census_year, type_of_private_accommodation, `value` FROM cso.private_households_2_2011_2022 WHERE type_of_family_unit = 'All family units' AND nature_of_occupancy = 'All types of occupancy' ")
data_df.persist()

# COMMAND ----------

dwellings_df = data_df.groupBy("census_year").pivot("type_of_private_accommodation").agg({"value": "first"})

# COMMAND ----------

dwellings_df_columns = dwellings_df.columns
for c in dwellings_df_columns:
    dwellings_df = dwellings_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())

# COMMAND ----------

population_df = spark.sql("SELECT year as census_year, population FROM report.population_component")
dwellings_df = dwellings_df.join(population_df, 'census_year')

# COMMAND ----------

columns_to_divide = dwellings_df.columns
for c in columns_to_divide:
    if c not in ['census_year', 'populaton']:
        dwellings_df = dwellings_df.withColumn(c + '_proportion', col(c) / col('population'))

# COMMAND ----------

dwellings_df.display()

# COMMAND ----------

# MAGIC %run /Workspace/Users/23079924@studentmail.ul.ie/ireland_housing_demand/utils

# COMMAND ----------

createDeltaTable(dwellings_df, 'report', 'dwellings', '/mnt/dbstables/report/dwellings')

# COMMAND ----------


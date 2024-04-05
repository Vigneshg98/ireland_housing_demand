# Databricks notebook source
# MAGIC %run /Workspace/Users/23079924@studentmail.ul.ie/ireland_housing_demand/utils

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
import re

# COMMAND ----------

data_df = spark.sql("SELECT age_group, criteria_for_projection, `year`, `value`  FROM cso.population_projection_1_2016_2051 WHERE age_group = 'All ages' AND sex = 'Both sexes' ")
data_df.persist()

# COMMAND ----------

pp_criteria_df = data_df.withColumn('criteria_for_projection', regexp_replace(col('criteria_for_projection'), '-', ''))
pp_criteria_df = pp_criteria_df.groupBy("year").pivot("criteria_for_projection").agg({"value": "first"})

# COMMAND ----------

pp_criteria_df_columns = pp_criteria_df.columns
for c in pp_criteria_df_columns:
    if c.lower() != 'year':
        pp_criteria_df = pp_criteria_df.withColumn(c, col(c)*1000)
    pp_criteria_df = pp_criteria_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())

# COMMAND ----------

pp_criteria_df.display()

# COMMAND ----------

createDeltaTable(pp_criteria_df, 'report', 'population_projection_by_criteria', '/mnt/dbstables/report/population_projection_by_criteria')

# COMMAND ----------

data_df = spark.sql("SELECT age_group, criteria_for_projection, `year`, `value`  FROM cso.population_projection_1_2016_2051 WHERE sex = 'Both sexes' ")
data_df.persist()

# COMMAND ----------

pp_age_df = data_df.groupBy("age_group").pivot("year").agg({"value": "first"})

# COMMAND ----------

pp_age_df_columns = pp_age_df.columns
for c in pp_age_df_columns:
    if c.lower() != 'age_group':
        pp_age_df = pp_age_df.withColumn(c, col(c)*1000)
    pp_age_df = pp_age_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())

# COMMAND ----------

pp_age_df.display()

# COMMAND ----------

createDeltaTable(pp_age_df, 'report', 'population_projection_by_age', '/mnt/dbstables/report/population_projection_by_age')

# COMMAND ----------


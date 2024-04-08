# Databricks notebook source
from pyspark.sql.functions import col
import re

# COMMAND ----------

data_df = spark.sql("SELECT year, age_group, sex, inward_or_outward_flow, `value` FROM cso.estimated_migration_1_1987_2023 WHERE inward_or_outward_flow = 'Immigrants: All origins' ")
data_df.persist()

# COMMAND ----------

migration_df = data_df.groupBy("year").pivot("age_group").agg({"value": "first"})

# COMMAND ----------

migration_df_columns = migration_df.columns
for c in migration_df_columns:
    if c.lower() != 'year':
        migration_df = migration_df.withColumn(c, col(c)*1000)
    migration_df = migration_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())

# COMMAND ----------

migration_df.display()

# COMMAND ----------

# MAGIC %run /Workspace/deployments/ireland_housing_demand/utils

# COMMAND ----------

createDeltaTable(migration_df, 'report', 'migration_age_wise', '/mnt/dbstables/report/migration_age_wise')

# COMMAND ----------



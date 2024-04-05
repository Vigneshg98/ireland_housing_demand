# Databricks notebook source
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window
import re

# COMMAND ----------

data_df = spark.sql("SELECT statistic_label, year, component, `value` FROM cso.annual_population_change_1_1951_2023 ")
data_df.persist()

# COMMAND ----------

population_df = data_df.groupBy("statistic_label", "year").pivot("component").agg({"value": "first"})

# COMMAND ----------

# Define a window specification over the year column
windowSpec = Window.orderBy("year")

population_df = population_df.withColumn('Population', col('Population')*1000).withColumn('Natural increase', col('Natural increase')*1000).withColumn('Net migration', col('Net migration')*1000)

# Calculate the lagged value of the Population column
population_df = population_df.withColumn("Previous Population", lag("Population").over(windowSpec))

# Calculate the Population Change
population_df = population_df.withColumn("Population Change", col("Population") - col("Previous Population"))

# Calculate the Population Change Percentage
population_df = population_df.withColumn("Population Change Percentage", (col("Population Change") / col("Previous Population")) * 100)

# Drop the "Previous Population" column if not needed
population_df = population_df.drop(*["statistic_label", "Previous Population"])

# COMMAND ----------

population_df_columns = population_df.columns
for c in population_df_columns:
    population_df = population_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())

# COMMAND ----------

population_df.display()

# COMMAND ----------

# MAGIC %run /Workspace/Users/23079924@studentmail.ul.ie/ireland_housing_demand/utils

# COMMAND ----------

createDeltaTable(population_df, 'report', 'population_component', '/mnt/dbstables/report/population_component')

# COMMAND ----------



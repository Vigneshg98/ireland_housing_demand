# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

data_df = spark.sql("SELECT statistic_label, census_year, county_and_city, `value` FROM cso.private_households_1_2011_2022 WHERE composition_of_private_household = 'Total private households' ")
data_df.persist()

# COMMAND ----------

total_households_df = data_df.filter(col("statistic_label") == "Total households").withColumnRenamed("value", "households_value")
total_persons_df = data_df.filter(col("statistic_label") == "Total persons").withColumnRenamed("value", "persons_value")

# COMMAND ----------

joined_df = total_households_df.join(total_persons_df, ["county_and_city", "census_year"])
average_persons_per_household_df = (
    joined_df.withColumn("average_persons_per_household", col("persons_value") / col("households_value"))
    .select("county_and_city", "census_year", "average_persons_per_household")
    )

# COMMAND ----------

average_persons_per_household_df.display()

# COMMAND ----------



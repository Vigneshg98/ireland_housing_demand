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
average_persons_per_household_df_2011_2022 = (
    joined_df.withColumn("average_persons_per_household", col("persons_value") / col("households_value"))
    .select("county_and_city", "census_year", "average_persons_per_household")
    )

# COMMAND ----------

data_df_2002 = spark.sql("SELECT statistic_label, censusyear as census_year, province_county_or_city as county_and_city, `value` FROM cso.private_households_3_2002 WHERE type_of_household = 'All households' AND statistic_label IN ('Private households', 'Persons in private households') ")
data_df_2002.persist()

data_df_2006 = spark.sql("SELECT statistic_label, censusyear as census_year, province_county_or_city as county_and_city, `value` FROM cso.private_households_4_2006 WHERE type_of_household = 'All households' AND statistic_label IN ('Private households', 'Persons in private households') ")
data_df_2006.persist()

# COMMAND ----------

total_households_df_2002 = data_df_2002.filter(col("statistic_label") == "Private households").withColumnRenamed("value", "households_value")
total_persons_df_2002 = data_df_2002.filter(col("statistic_label") == "Persons in private households").withColumnRenamed("value", "persons_value")

total_households_df_2006 = data_df_2006.filter(col("statistic_label") == "Private households").withColumnRenamed("value", "households_value")
total_persons_df_2006 = data_df_2006.filter(col("statistic_label") == "Persons in private households").withColumnRenamed("value", "persons_value")

# COMMAND ----------

joined_df_2002 = total_households_df_2002.join(total_persons_df_2002, ["county_and_city", "census_year"])
average_persons_per_household_df_2002 = (
    joined_df_2002.withColumn("average_persons_per_household", col("persons_value") / col("households_value"))
    .select("county_and_city", "census_year", "average_persons_per_household")
    )

joined_df_2006 = total_households_df_2006.join(total_persons_df_2006, ["county_and_city", "census_year"])
average_persons_per_household_df_2006 = (
    joined_df_2006.withColumn("average_persons_per_household", col("persons_value") / col("households_value"))
    .select("county_and_city", "census_year", "average_persons_per_household")
    )

# COMMAND ----------

average_persons_per_household_df = average_persons_per_household_df_2011_2022.union(average_persons_per_household_df_2002).union(average_persons_per_household_df_2006)


# COMMAND ----------

average_persons_per_household_df.display()

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

pandas_df = average_persons_per_household_df.filter(col('county_and_city').isin(['State', 'Dublin', 'Galway', 'Limerick'])).toPandas()

grouped_df = pandas_df.groupby(['county_and_city', 'census_year'])['average_persons_per_household'].mean().unstack()

grouped_df.plot(kind='bar', figsize=(10, 6))
plt.xlabel('County and City')
plt.ylabel('Average Persons per Household')
plt.title('Average Persons per Household by County and City')
plt.xticks(rotation=45)
plt.legend(title='Year')
plt.tight_layout()

plt.show()

# COMMAND ----------



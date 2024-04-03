# Databricks notebook source
from pyspark.sql.functions import col
import re

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

average_persons_per_household_df_columns = average_persons_per_household_df.columns
for c in average_persons_per_household_df_columns:
    average_persons_per_household_df = average_persons_per_household_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())

# COMMAND ----------

average_persons_per_household_df.display()

# COMMAND ----------

# MAGIC %run /Workspace/Users/23079924@studentmail.ul.ie/ireland_housing_demand/utils

# COMMAND ----------

createDeltaTable(average_persons_per_household_df, 'report', 'average_persons_per_household', '/mnt/dbstables/report/average_persons_per_household')

# COMMAND ----------



# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import lit


# COMMAND ----------

# Index the county_and_city column
indexer = StringIndexer(inputCol="county_and_city", outputCol="county_and_city_index")
indexed_df = indexer.fit(average_persons_per_household_df).transform(average_persons_per_household_df)

# One-hot encode the indexed county_and_city column
encoder = OneHotEncoder(inputCols=["county_and_city_index"], outputCols=["county_and_city_encoded"])
encoder_model = encoder.fit(indexed_df)
encoded_df = encoder_model.transform(indexed_df)

# Define the features including 'census_year' and the encoded 'county_and_city'
feature_columns = ['census_year', 'county_and_city_encoded']

# Vectorize the features
vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
vectorized_df = vector_assembler.transform(encoded_df)

# Split data into training and testing sets
(training_data, testing_data) = vectorized_df.randomSplit([0.8, 0.2], seed=1234)

# Create Linear Regression model
lr = LinearRegression(featuresCol='features', labelCol='average_persons_per_household')

# Train the model
lr_model = lr.fit(training_data)

# Make predictions on the testing data
predictions = lr_model.transform(testing_data)

# Predict for the next 4 years for each county_and_city
future_years = [2023, 2024, 2025, 2026]
next_years = []
for year in future_years:
    # Create DataFrame with next year and each unique county_and_city
    year_df = spark.createDataFrame([(year, city) for city in average_persons_per_household_df.select("county_and_city").distinct().rdd.map(lambda row: row[0]).collect()], ["census_year", "county_and_city"])
    
    # Index and encode the county_and_city column
    indexed_year_df = indexer.fit(average_persons_per_household_df).transform(year_df)  # Using transform from StringIndexerModel
    encoded_year_df = encoder_model.transform(indexed_year_df)  # Using transform from OneHotEncoderModel
    
    # Vectorize features
    vectorized_year_df = vector_assembler.transform(encoded_year_df)
    
    # Predict average persons per household
    predictions_year_df = lr_model.transform(vectorized_year_df)
    
    # Append predictions for the year
    next_years.append(predictions_year_df.select("census_year", "county_and_city", "prediction"))

# Concatenate predictions for all years
all_predictions = next_years[0]
for i in range(1, len(next_years)):
    all_predictions = all_predictions.union(next_years[i])


# COMMAND ----------

# Show predicted values for the next 4 years for each county_and_city
all_predictions.orderBy("census_year", "county_and_city").display()

# COMMAND ----------



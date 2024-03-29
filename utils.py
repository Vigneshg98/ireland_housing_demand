# Databricks notebook source
# MAGIC %run /Workspace/Users/23079924@studentmail.ul.ie/ireland_housing_demand/dataset_configuration

# COMMAND ----------

import pandas as pd
import re

# COMMAND ----------

def getDatasetAsCSV(url):
    print(f'BEGIN :: getDatasetAsCSV')
    data_df = pd.read_csv(url)
    data_df = spark.createDataFrame(data_df) 
    data_df_columns = data_df.columns
    for c in data_df_columns:
        data_df = data_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())
    print(f'END :: getDatasetAsCSV')
    return data_df


# COMMAND ----------

def createDeltaTable(data_df, db_name, table_name, table_path):
    print(f'BEGIN :: createDeltaTable')
    data_df.createOrReplaceTempView(f"temp_table_{table_name}")
    create_string = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name} USING DELTA """
    option_string = f"""OPTIONS (path = "{table_path}") AS SELECT * from temp_table_{table_name}"""
    create_sql_string = create_string + option_string
    print(create_sql_string)
    spark.sql(create_sql_string)
    print(f'END :: createDeltaTable')


# COMMAND ----------



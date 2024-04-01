# Databricks notebook source
# MAGIC %run /Workspace/Users/23079924@studentmail.ul.ie/ireland_housing_demand/utils

# COMMAND ----------

for item in dataset_endpoints:
    print(item)
    data_df = getDatasetAsCSV(dataset_endpoints[item])
    table_path = f"/mnt/dbstables/{db_name}/{item}/"
    createDeltaTable(data_df, db_name, item, table_path)
    print()


# COMMAND ----------



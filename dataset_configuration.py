# Databricks notebook source
db_name = 'cso'

# COMMAND ----------

dataset_endpoints = {
    'private_households_1_2011_2022': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/F3069/CSV/1.0/en',
    'private_households_2_2011_2022': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/F3035/CSV/1.0/en',
    'population_1_2011_2022': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/F3027/CSV/1.0/en',
    'population_2_2011_2022': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/F3001/CSV/1.0/en',
    'annual_population_change_1_1951_2023': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/PEA15/CSV/1.0/en',
    'estimated_population_1_2011_2023': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/PEA04/CSV/1.0/en',
    'estimated_population_2_1950_2023': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/PEA01/CSV/1.0/en',
    'estimated_migration_1_1987_2023': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/PEA03/CSV/1.0/en',
    'estimated_migration_2_1987_2023': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/PEA18/CSV/1.0/en',
    'average_number_persons_1_2011_2022': 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/FY004B/CSV/1.0/en'
}

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS cso COMMENT 'This holds all the dataset from CSO' LOCATION '/mnt/dbstables/cso'
# MAGIC

# COMMAND ----------


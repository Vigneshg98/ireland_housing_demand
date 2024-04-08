# Databricks notebook source
pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run /Workspace/deployments/ireland_housing_demand/utils

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

# COMMAND ----------

dataset_url_1 = 'https://www.cso.ie/en/media/csoie/releasespublications/documents/ep/censusprofile1-housinginireland/housingstock/P-CP1HII2016TBL1.1.xlsx'


data_df = pd.read_excel(dataset_url_1)
column_names = data_df.iloc[0]
data_df = data_df.tail(-1)
data_df = data_df.rename(columns=column_names)
data_df = spark.createDataFrame(data_df)

data_df_columns = data_df.columns
for c in data_df_columns:
    data_df = data_df.withColumnRenamed(c, re.sub(r'[\W_]+', '_', c).lower())
data_df = data_df.withColumnRenamed('_housing_stock_change', 'housing_stock_change_percentage').withColumnRenamed('_population_change', 'population_change_percentage').withColumnRenamed('dwellings_per_1_000_pop', 'dwellings_per_1000_pop').withColumnRenamed('_change_dwellings_per_1_000_pop', 'change_dwellings_per_1000_pop_percentage')

# COMMAND ----------

data_df.display()

# COMMAND ----------

dataset_url_2 = 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/F2095/CSV/1.0/en'
data_df_2 = getDatasetAsCSV(dataset_url_2)

# COMMAND ----------

data_df_2 = data_df_2.where((col('electoral_divisions') == 'Ireland') & (col('housing_stock') == 'Total housing stock'))
housing_stock = data_df_2.collect()[0]['value']
population = spark.sql("SELECT population FROM report.population_component WHERE year = 2022").collect()[0]['population']
housing_stock_change = housing_stock - data_df.select('housing_stock').where(col('census_year') == 2016).collect()[0]['housing_stock']
population_change = population - data_df.select('population').where(col('census_year') == 2016).collect()[0]['population']
_housing_stock_change = (housing_stock_change / housing_stock) * 100
_population_change = (population_change / population) * 100
dwellings_per_1000_pop = (housing_stock / (population / 1000))
dwellings_per_1000_pop_change = dwellings_per_1000_pop - data_df.select('dwellings_per_1000_pop').where(col('census_year') == 2016).collect()[0]['dwellings_per_1000_pop']
_change_dwellings_per_1000_pop = (dwellings_per_1000_pop_change / dwellings_per_1000_pop) * 100

# COMMAND ----------

data_2022 = {
    'census_year': 2022,
    'housing_stock': housing_stock,
    'population': population,
    'housing_stock_change': housing_stock_change,
    'population_change': population_change,
    'housing_stock_change_percentage': _housing_stock_change,
    'population_change_percentage': _population_change,
    'dwellings_per_1000_pop': dwellings_per_1000_pop,
    'change_dwellings_per_1000_pop_percentage': _change_dwellings_per_1000_pop
}

# COMMAND ----------

data_2022_schema = StructType([
    StructField("census_year", LongType(), True),
    StructField("housing_stock", DoubleType(), True),
    StructField("population", DoubleType(), True),
    StructField("housing_stock_change", DoubleType(), True),
    StructField("population_change", DoubleType(), True),
    StructField("housing_stock_change_percentage", DoubleType(), True),
    StructField("population_change_percentage", DoubleType(), True),
    StructField("dwellings_per_1000_pop", DoubleType(), True),
    StructField("change_dwellings_per_1000_pop_percentage", DoubleType(), True)
])

data_list = [
    (
        data_2022['census_year'],
        data_2022['housing_stock'],
        data_2022['population'],
        data_2022['housing_stock_change'],
        data_2022['population_change'],
        data_2022['housing_stock_change_percentage'],
        data_2022['population_change_percentage'],
        data_2022['dwellings_per_1000_pop'],
        data_2022['change_dwellings_per_1000_pop_percentage']
    )
]

# COMMAND ----------

row_2022 = spark.createDataFrame(data_list, schema=data_2022_schema)
data_df = data_df.union(row_2022)
data_df.display()

# COMMAND ----------

table_path = f"/mnt/dbstables/{db_name}/housing_stock_and_population_1991_2022/"
print(table_path)
createDeltaTable(data_df, db_name, "housing_stock_and_population_1991_2022", table_path)
print()

# COMMAND ----------


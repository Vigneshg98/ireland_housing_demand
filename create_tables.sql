-- Databricks notebook source
CREATE TABLE cso.population_census (
  STATISTIC STRING,
  Statistic_Label STRING,
  TLIST_A1 STRING,
  CensusYear STRING,
  C02779V03348 STRING,
  County STRING,
  C02199V02655 STRING,
  Sex STRING,
  UNIT STRING,
  VALUE STRING
)
USING delta
LOCATION 'dbfs:/mnt/dbstables/cso/population_census'


-- COMMAND ----------


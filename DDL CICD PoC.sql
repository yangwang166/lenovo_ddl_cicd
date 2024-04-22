-- Databricks notebook source
create catalog if not exists lenovo

-- COMMAND ----------

use catalog lenovo

-- COMMAND ----------

create schema if not exists poc

-- COMMAND ----------

use schema poc

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sample_table (
  id INT,
  name VARCHAR(50),
  age INT,
  is_admin BOOLEAN,
  registration_date DATE,
  col1 INT,
  col2 LONG
);

-- COMMAND ----------

--Change DDL

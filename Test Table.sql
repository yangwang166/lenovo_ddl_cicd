-- Databricks notebook source
use catalog lenovo;
use schema poc;

-- COMMAND ----------

INSERT INTO sample_table (id, name, age, is_admin, registration_date, col1, col2)
VALUES (1, 'Will Wang', 37, true, '2024-04-22', 99, 999);

-- COMMAND ----------

select * from sample_table;

-- COMMAND ----------

drop table sample_table;

-- COMMAND ----------



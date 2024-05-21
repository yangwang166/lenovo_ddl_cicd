# Databricks notebook source
# MAGIC %md
# MAGIC # The Databricks Assistant
# MAGIC
# MAGIC <hr>
# MAGIC
# MAGIC Please see the <a href="https://docs.databricks.com/notebooks/notebook-assistant-faq.html" target="_blank">FAQ</a> if you have any questions
# MAGIC
# MAGIC ### Supports the following core tasks:
# MAGIC
# MAGIC - Generate code from scratch
# MAGIC - Complete code
# MAGIC - Explain code
# MAGIC - Transform/optimize code
# MAGIC - Fix code
# MAGIC - Documentation lookups
# MAGIC <br>
# MAGIC
# MAGIC ### What is the Assistant and why use it?
# MAGIC - The Databricks Assistant is your data-focused AI-pair programmer for notebooks, queries, and files
# MAGIC - It helps users answer questions about their code, and increase productivity by making it faster to develop with Databricks
# MAGIC - Users can benefit from the capabilities of LLMs, including finding new ways to do things that are faster, more efficient, and easier
# MAGIC - Reduces the number of times a user will have to leave the notebook, such as reading documentation, knowledge bases, etc...
# MAGIC - Easily write code without needing to know about every syntax or feature of a language
# MAGIC - Automatically generate comments and documentation about your code

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Prompt:</b> Generate pandas code to read in parquet files from this path https://raw.githubusercontent.com/databricks-demos/dbdemos-dataset/main/retail/c360/users_parquet/users.parquet.snappy and save it into a dataframe called df and display the first 5 rows 

# COMMAND ----------

import pandas as pd

parquet_url = "https://raw.githubusercontent.com/databricks-demos/dbdemos-dataset/main/retail/c360/users_parquet/users.parquet.snappy"
df = pd.read_parquet(parquet_url)
df.head(5)

# COMMAND ----------

# Initialize the variable to store the sum
sum = 0

# Use a loop to iterate through the numbers from 1 to 100
for num in range(1, 101):
    sum += num

# Print the sum
print("The sum of numbers from 1 to 100 is:", sum)

# COMMAND ----------

# Initialize the variable to store the sum
sum = 0

# Use a loop to iterate through the numbers from 1 to 100
for num in range(1, 101):
    sum += num

# Print the sum
print("The sum of numbers from 1 to 100 is:", sum)

# COMMAND ----------

# DBTITLE 0,Inline Code Generation: Read Parquet Files from an GitHub Repo
import pandas as pd

url = "https://raw.githubusercontent.com/databricks-demos/dbdemos-dataset/main/retail/c360/users_parquet/users.parquet.snappy"
df = pd.read_parquet(url)
df.head(5)

# COMMAND ----------

# DBTITLE 1,Generate Code: Rename Columns
# MAGIC %md
# MAGIC <b>Prompt:</b> Generate code to rename FR to FRANCE in the country column, and show dataframe

# COMMAND ----------

#Paste code here
import pandas as pd 

# assume the DataFrame is called df
df.loc[df['country'] == 'FR', 'country'] = 'FRANCE'
df.head()

# COMMAND ----------

# DBTITLE 1,Generate Code: Create Plot
# MAGIC %md
# MAGIC <b>Prompt:</b> Generated a stacked bar chart using matplotlib that shows the number of churned users for each country and canal

# COMMAND ----------

# Paste code here
import matplotlib.pyplot as plt

churned_users = df[df['churn'] == True]
churned_users_by_country_and_canal = churned_users.groupby(['country', 'canal'])['churn'].count().unstack()

churned_users_by_country_and_canal.plot(kind='bar', stacked=True)
plt.show()

# COMMAND ----------

# DBTITLE 1,Fix Code (Basic)
# MAGIC %md
# MAGIC <b>Prompt:</b> Fix this code, the column names should be lowercase

# COMMAND ----------

# Incorrect code
churned_users = df[df['Churn'] == True]

grouped = churned_users.groupby(['Country', 'Canal'])['Churn'].count().reset_index()
idx = grouped.groupby(['Country'])['Churn'].transform(max) == grouped['Churn']

result = grouped[idx]

# COMMAND ----------

# DBTITLE 1,Explain Code
# MAGIC %md
# MAGIC <b>Prompt:</b> Explain this code <br>
# MAGIC <b>Extra:</b> Add "be concise", "be detailed", or "go line by line" to get more or less detail

# COMMAND ----------

churned_users = df[df['churn'] == True]

grouped = churned_users.groupby(['country', 'canal'])['churn'].count().reset_index()
idx = grouped.groupby(['country'])['churn'].transform(max) == grouped['churn']

result = grouped[idx]
print(result)

# COMMAND ----------

# DBTITLE 1,Result:
# This is a Python code that filters out users that have churned and groups them by their country and canal. It then finds the rows with the maximum count of churned users per country and outputs the result for those rows.

# Here are the steps:

# churned_users = df[df['churn'] == True] selects all rows with a churn value of True.

# grouped = churned_users.groupby(['country', 'canal'])['churn'].count().reset_index() groups the churned users by their country and canal columns and counts their number. The reset_index() method is used to make sure the result is a dataframe with a reset index.

# idx = grouped.groupby(['country'])['churn'].transform(max) == grouped['churn'] creates a Boolean index that is true for all the rows with the maximum count of churned users per country. It does this by grouping the data by country and then using transform(max) to find the maximum count of churned users per country.

# result = grouped[idx] selects the rows identified by the Boolean index.

# print(result) prints out the result dataframe.

# Note: The code assumes that the input dataframe df has columns 'churn', 'country' and 'canal'.

# COMMAND ----------

# DBTITLE 1,Convert pandas Code to PySpark
# MAGIC %md
# MAGIC <b>Prompt:</b> convert this code from pandas to pyspark, including the code needed to convert the pandas dataframe to a pyspark dataframe and changing the data type of column churn from boolean to integer

# COMMAND ----------

# Starting code
churned_counts = df.groupby(['country', 'gender', 'age_group']).agg(churned_count=('churn', 'sum')).sort_values(by='churned_count', ascending=False)

# Print the resulting dataframe
churned_counts.head(10)

# COMMAND ----------

#Paste code here
from pyspark.sql.functions import col, sum
from pyspark.sql.types import IntegerType

# Convert pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(df)

# Cast churn column from boolean to integer
spark_df = spark_df.withColumn('churn', col('churn').cast(IntegerType()))

# Group by country, gender, and age_group, summing churned counts
churned_counts = spark_df.groupby(['country', 'gender', 'age_group']).agg(sum('churn').alias('churned_count')).sort(col('churned_count').desc())

# Print the resulting dataframe
churned_counts.show(10)

# COMMAND ----------

# DBTITLE 1,Generate: Advanced PySpark Code
# MAGIC %md
# MAGIC <b>Prompt: </b> generate pyspark code to convert the pandas dataframe df to a pyspark dataframe, churn column from boolean to integer, convert the creation_date and last_activity_date columns to timestamp with the MM-dd-yyyy HH:mm:ss format, add a new column which is the difference between the last_activity_date and the creation_date. If that new column has a value larger than 5 years, return the number of those users who churned (column "churn") using the dataframe in this code

# COMMAND ----------

# Paste code here
from pyspark.sql import functions as F
from pyspark.sql.functions import datediff, unix_timestamp, from_unixtime

# create spark dataframe from pandas dataframe
df = spark.createDataFrame(df)

# Convert churn column from boolean to integer
df = df.withColumn("churn", F.col("churn").cast("integer"))

# Convert creation_date and last_activity_date columns to timestamp with the MM-dd-yyyy HH:mm:ss format
df = df.withColumn("creation_date", from_unixtime(unix_timestamp(df["creation_date"], "MM-dd-yyyy HH:mm:ss")))
df = df.withColumn("last_activity_date", from_unixtime(unix_timestamp(df["last_activity_date"], "MM-dd-yyyy HH:mm:ss")))

# Add a new column which is the difference between the last_activity_date and the creation_date
df = df.withColumn("time_span", datediff(df["last_activity_date"], df["creation_date"]))

# If the time span is more than 5 years, then count users who churned
num_churned_users = df.filter((df["time_span"] > 365*5) & (df["churn"] ==True)).count()
print(num_churned_users)

# COMMAND ----------

# DBTITLE 1,Convert PySpark code to SparkSQL
# MAGIC %md
# MAGIC <b>Prompt: </b> Convert this code to ANSI SQL and query the view called "users_table"

# COMMAND ----------

# Starting code
df.createOrReplaceTempView("users_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Paste code here
# MAGIC SELECT COUNT(*) as num_churned_users
# MAGIC FROM users_table
# MAGIC WHERE churn = 1 AND DATEDIFF(last_activity_date, creation_date) > (365 * 5);

# COMMAND ----------

# DBTITLE 1,Diagnose Error
# MAGIC %md
# MAGIC <b>Prompt:</b> Explain this error, I want to get the number of days between two dates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Paste code here
# MAGIC SELECT COUNT(*) as num_churned_users
# MAGIC FROM users_table
# MAGIC WHERE churn = 1 AND DATE_SUB(last_activity_date, creation_date) > (365 * 5);

# COMMAND ----------

total = 0

for num in range(1, 101):
    total += num

print("The sum of numbers from 1 to 100 is", total)

# COMMAND ----------



# COMMAND ----------

total = 0

for num in range(1, 101):
    total += num

print("The sum of numbers from 1 to 100 is", total)

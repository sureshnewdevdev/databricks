# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import lit
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Create SilverCustomer Table and Insert Data") \
    .getOrCreate()

# Define the table schema
schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("BillToCustomerID", StringType(), True),
    StructField("CustomerCategoryID", StringType(), True),
    StructField("BuyingGroupID", StringType(), True),
    StructField("PrimaryContactPersonID", StringType(), True),
    StructField("PostalCityID", StringType(), True),
    StructField("ValidFrom", DateType(), True),
    StructField("ValidTo", DateType(), True),
    StructField("LineageKey", IntegerType(), True)
])

# Read the CSV file with specified column names
df = spark.read.csv("/FileStore/tables/Customer.csv", header=True, schema=schema)

# Map the columns according to the provided mapping
df = df.selectExpr(
    "CustomerID as CustomerKey",
    "CustomerID as WWICustomerID",
    "CustomerName as Customer",
    "BillToCustomerID as BillToCustomer",
    "CustomerCategoryID as Category",
    "BuyingGroupID as BuyingGroup",
    "PrimaryContactPersonID as PrimaryContact",
    "PostalCityID as PostalCode",
    "ValidFrom as ValidFrom",
    "ValidTo as ValidTo",
    "LineageKey as LineageKey"
)

 

# Save the DataFrame as a CSV file in /FileStore/tables/BSGOutput directory
output_directory = "/FileStore/tables/BSGOutput"
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
df.write.mode("overwrite").csv(output_directory)

# Create or replace the SilverCustomer table
df.write.mode("overwrite").saveAsTable("SilverCustomer")

# Show the DataFrame
display(df)

# Stop SparkSession
# spark.stop()


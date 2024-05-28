# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import lit, udf
from pyspark.sql.functions import col
from pyspark.sql.functions import uuid

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Create Table in Databricks PySpark") \
    .getOrCreate()
 
# Define the schema for the table
schema = StructType([
    StructField("Order Key", StringType(), nullable=False),
    StructField("Customer Key", IntegerType(), nullable=True),
    StructField("City Key", IntegerType(), nullable=True),
    StructField("Stock Item Key", IntegerType(), nullable=True),
    StructField("Order Date Key", DateType(), nullable=True),
    StructField("Picked Date Key", DateType(), nullable=True),
    StructField("Salesperson Key", IntegerType(), nullable=True),
    StructField("Picker Key", IntegerType(), nullable=True),
    StructField("WWI Order ID", IntegerType(), nullable=True),
    StructField("WWI Backorder ID", IntegerType(), nullable=True),
    StructField("Description", StringType(), nullable=True),
    StructField("Package", StringType(), nullable=True),
    StructField("Quantity", IntegerType(), nullable=True),
    StructField("Unit Price", DecimalType(18, 2), nullable=True),
    StructField("Tax Rate", DecimalType(18, 3), nullable=True),
    StructField("Total Excluding Tax", DecimalType(18, 2), nullable=True),
    StructField("Tax Amount", DecimalType(18, 2), nullable=True),
    StructField("Total Including Tax", DecimalType(18, 2), nullable=True),
    StructField("Lineage Key", IntegerType(), nullable=True)
])

# Create an empty DataFrame with the specified schema and generate GUIDs for Order Key
GoldFactOrder_df = spark.createDataFrame([], schema)

# Load customer_to_gold_df from Delta table
customer_to_gold_df = spark.read.format("delta").load("/FileStore/tables/temp_silver_customers_table_delta")

# Read data from the SalesTable
df_from_SalesOrder = spark.table("SalesTable")

# Generate GUIDs for Order Key
df_from_SalesOrder = df_from_SalesOrder.withColumn("Order Key", uuid())

# Join df_from_SalesOrder with customer_to_gold_df and select necessary columns
joined_df = df_from_SalesOrder.join(customer_to_gold_df, df_from_SalesOrder["CustomerID"].cast("int") == customer_to_gold_df["WWICustomerID"], "left") \
    .select(
        df_from_SalesOrder["OrderID"].alias("Order Key"),
        customer_to_gold_df["CustomerKey"].alias("Customer Key"),
        lit(None).alias("City Key"),
        lit(None).alias("Stock Item Key"),
        lit(None).alias("Order Date Key"),
        lit(None).alias("Picked Date Key"),
        lit(None).alias("Salesperson Key"),
        lit(None).alias("Picker Key"),
        lit(None).alias("WWI Order ID"),
        lit(None).alias("WWI Backorder ID"),
        lit(None).alias("Description"),
        lit(None).alias("Package"),
        lit(None).alias("Quantity"),
        lit(None).alias("Unit Price"),
        lit(None).alias("Tax Rate"),
        lit(None).alias("Total Excluding Tax"),
        lit(None).alias("Tax Amount"),
        lit(None).alias("Total Including Tax"),
        lit(None).alias("Lineage Key")
    )

# Union the joined DataFrame with GoldFactOrder_df to keep all rows
final_df = GoldFactOrder_df.union(joined_df)

# Replace the existing GoldFactOrder_df with the final DataFrame
GoldFactOrder_df = final_df

# Display the updated GoldFactOrder_df
display(GoldFactOrder_df)


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/
# MAGIC
# MAGIC

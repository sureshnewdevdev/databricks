# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/SaleOrder.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# Load the CSV file into a DataFrame
dfSales = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)

# Display the DataFrame
display(dfSales)

# Save DataFrame as a permanent table
dfSales.write.mode("overwrite").saveAsTable("SalesTable")

# Access the permanent table created in the first notebook
permanent_table_df = spark.table("SilverCustomer")

# Filter rows where customer_id is not null
silver_customers_df = permanent_table_df.filter(permanent_table_df["CustomerKey"].isNotNull())

# Create a temporary view or register DataFrame as a temporary table
silver_customers_df.createOrReplaceTempView("temp_silver_customers_table")

# Create a Delta table from the temporary view
delta_table_location = "/FileStore/tables/temp_silver_customers_table_delta"

# Check if the location is empty
if len(dbutils.fs.ls(delta_table_location)) > 0:
    # If not empty, delete existing files
    dbutils.fs.rm(delta_table_location, True)

# Save the DataFrame as a Delta table
silver_customers_df.write.format("delta").mode("overwrite").save(delta_table_location)

# Access the saved Delta table
delta_table_df = spark.read.format("delta").load(delta_table_location)

# Display the Delta table
display(delta_table_df)


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/CustomerGold

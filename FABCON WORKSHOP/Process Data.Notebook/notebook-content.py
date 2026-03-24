# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4377425c-cf05-430c-a6b3-1d8151741906",
# META       "default_lakehouse_name": "RawData",
# META       "default_lakehouse_workspace_id": "7985a237-e120-49b0-af7d-e2bf580ec433",
# META       "known_lakehouses": [
# META         {
# META           "id": "4377425c-cf05-430c-a6b3-1d8151741906"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Load the data Files

# Load Intermediary Files
saleDataNorth_df = spark.read.format("parquet").option("header","true").load("Files/wwsales/SaleDataNorth")
saleDataSouth_df = spark.read.format("parquet").option("header","true").load("Files/wwsales/SalesDataSouth")

# Load Final Files Into Dataframes
customer_df = spark.read.format("parquet").option("header","true").load("Files/wwsales/Customer")
product_df = spark.read.format("parquet").option("header","true").load("Files/wwsales/Product")
state_df = spark.read.format("parquet").option("header","true").load("Files/wwsales/State")
salesData_df = saleDataNorth_df.unionByName(saleDataSouth_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from pyspark.sql.functions import col

# Define a function to sanitize column names
def sanitize_column_name(col_name):
  # Replace invalid characters with an underscore
    return re.sub(r'[ ,;{}()\n\t=]', '_', col_name)

# Apply the function to rename columns
formatted_customer_df = customer_df.toDF(*[sanitize_column_name(col) for col in customer_df.columns])
formatted_product_df = product_df.toDF(*[sanitize_column_name(col) for col in product_df.columns])
formatted_state_df = state_df.toDF(*[sanitize_column_name(col) for col in state_df.columns])
formatted_salesData_df = salesData_df.toDF(*[sanitize_column_name(col) for col in salesData_df.columns])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

formatted_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.Customer")

formatted_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.Product")

formatted_state_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.State")

formatted_salesData_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove NULL sales

cleaned_formatted_salesdata_df = formatted_salesData_df.na.drop(subset="Sale_Key")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the tables to tables

formatted_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.Customer")

formatted_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.Product")

formatted_state_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.State")

cleaned_formatted_salesdata_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("dbo.Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

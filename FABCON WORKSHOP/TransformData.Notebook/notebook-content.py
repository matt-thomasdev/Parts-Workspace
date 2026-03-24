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
# META         },
# META         {
# META           "id": "49821685-40cb-4887-b754-32cc96415407"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

product_df = spark.sql("SELECT * from RawData.dbo.product")
sales_df = spark.sql("""
    SELECT
        s.*,
        p.Unit_Cost
    FROM RawData.dbo.sales AS s
    LEFT JOIN RawData.dbo.product AS p
        ON s.Stock_Item_Key = p.Stock_Item_Key
""")
customer_df = spark.sql("""
    SELECT
        c.Customer,
        c.Bill_To_Customer,
        c.Category,
        c.Buying_Group,
        c.Primary_Contact,
        c.Postal_Code,
        c.Customer_Key,
        st.Country,
        st.Continent,
        st.Sales_Territory,
        st.Region,
        st.Subregion
    FROM RawData.dbo.Customer AS c
    LEFT JOIN RawData.dbo.State AS st
        ON st.State_Province_Key = c.State_Key
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://7985a237-e120-49b0-af7d-e2bf580ec433@onelake.dfs.fabric.microsoft.com/49821685-40cb-4887-b754-32cc96415407/Tables/dbo.Product")
sales_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://7985a237-e120-49b0-af7d-e2bf580ec433@onelake.dfs.fabric.microsoft.com/49821685-40cb-4887-b754-32cc96415407/Tables/dbo.Sales")
customer_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://7985a237-e120-49b0-af7d-e2bf580ec433@onelake.dfs.fabric.microsoft.com/49821685-40cb-4887-b754-32cc96415407/Tables/dbo.Customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

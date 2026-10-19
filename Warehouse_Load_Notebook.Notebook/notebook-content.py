# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "39e65af4-dd0a-40e7-8f69-e6fd524c0401",
# META       "default_lakehouse_name": "retail_Lakhouse",
# META       "default_lakehouse_workspace_id": "aa7643ac-cb35-4247-a5f0-491f80de52cb",
# META       "known_lakehouses": [
# META         {
# META           "id": "39e65af4-dd0a-40e7-8f69-e6fd524c0401"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#### This notebook should be run ONLY when we add the data for the first time !!!

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (col,date_format,dayofmonth,month,quarter,year)
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


df_categories_silver = spark.table("silver.categories")
df_customers_silver = spark.table("silver.customers")
df_order_details_silver = spark.table("silver.order_details")
df_orders_silver = spark.table("silver.orders")
df_products_silver = spark.table("silver.products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add data from Customers silver to df_stg_customer

df_stg_customer = df_customers_silver.select("CustomerID","Gender","Region","City","Age","CustomerSegment","SignUpDate")

df_stg_customer.write .mode("overwrite").synapsesql("retail_warehouse.dbo.stg_dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add data from Product silver to df_stg_product

df_stg_product = (
    df_categories_silver
    .join(df_products_silver, "CategoryID", "inner")
    .select(
        "ProductID",
        "ProductName",
        "CategoryName"
    )
)

df_stg_product.write.mode("overwrite").synapsesql("retail_warehouse.dbo.stg_dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Add data from date silver to dim_date


df_dim_date = (
    df_orders_silver
    .select(col("OrderDate").alias("FullDate"))
    .distinct()
    .withColumn("DateKey", date_format(col("FullDate"), "yyyyMMdd").cast("int"))
    .withColumn("Day", dayofmonth(col("FullDate")))
    .withColumn("Month", month(col("FullDate")))
    .withColumn("MonthName", date_format(col("FullDate"), "MMMM"))
    .withColumn("Quarter", quarter(col("FullDate")))
    .withColumn("Year", year(col("FullDate")))
    .select("DateKey","FullDate","Day","Month","MonthName","Quarter","Year")
)

df_dim_date.write.mode("append").synapsesql("retail_warehouse.dbo.dim_date")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read generated surrogate keys

df_dim_product = (spark.read.synapsesql("retail_warehouse.dbo.dim_product").select("ProductKey","ProductID"))

df_dim_customer = (spark.read.synapsesql("retail_warehouse.dbo.dim_customer").select("CustomerKey","CustomerID"))

df_dim_date_lookup = (spark.read.synapsesql("retail_warehouse.dbo.dim_date").select("DateKey","FullDate"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build fact_sales table
df_fact_sales = (df_order_details_silver.join(df_orders_silver,"OrderID","inner")
    .select(
        "OrderID",
        "ProductID",
        "CustomerID",
        "OrderDate",
        "Quantity",
        "UnitCost",
        "UnitPrice",
        "DiscountRate",
        "GrossAmount",
        "DiscountAmount",
        "NetAmount",
        "CostAmount",
        "ProfitAmount",
        "ReturnAmount"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace natural keys with Warehouse surrogate keys
df_fact_sales = (df_fact_sales.join(
        df_dim_product,
        "ProductID",
        "inner"
    ).join(
        df_dim_customer,
        "CustomerID",
        "inner"
    ).join(
        df_dim_date_lookup,
        df_fact_sales["OrderDate"] == df_dim_date_lookup["FullDate"],
        "inner"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final fact_sales schema
df_fact_sales = df_fact_sales.select(
    "OrderID",
    "CustomerKey",
    "ProductKey",
    "DateKey",
    "Quantity",
    "UnitCost",
    "UnitPrice",
    "DiscountRate",
    "GrossAmount",
    "DiscountAmount",
    "NetAmount",
    "CostAmount",
    "ProfitAmount",
    "ReturnAmount"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load fact_sales
df_fact_sales.write.mode("append").synapsesql("retail_warehouse.dbo.fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Customers:", spark.read.synapsesql("retail_warehouse.dbo.dim_customer").count())

print("Products:",spark.read.synapsesql("retail_warehouse.dbo.dim_product").count())

print("Dates:",spark.read.synapsesql("retail_warehouse.dbo.dim_date").count())

print("Fact rows:",spark.read.synapsesql("retail_warehouse.dbo.fact_sales").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM retail_Lakhouse.bronze.orders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM retail_Lakhouse.bronze.order_details LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

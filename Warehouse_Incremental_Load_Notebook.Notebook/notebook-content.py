# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from datetime import date
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, lit, max as spark_max, row_number,
    date_format, dayofmonth, month, quarter, year
)
from pyspark.sql.window import Window

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


# New customer
batch_customers = spark.createDataFrame([
    ("CSTMR1001", "Male", 30, "Rotterdam", "West", "Regular", date(2026, 6, 2))
], [
    "CustomerID", "Gender", "Age", "City",
    "Region", "CustomerSegment", "SignUpDate"
])


# New product
batch_products = spark.createDataFrame([
    ("PRDCT0101", "Incremental Test Product", "New Category")
], [
    "ProductID", "ProductName", "CategoryName"
])


# New order
batch_orders = spark.createDataFrame([
    ("ORD10001", "CSTMR1001", date(2026, 6, 2))
], [
    "OrderID", "CustomerID", "OrderDate"
])


# New sales line
batch_order_details = spark.createDataFrame([
    (
        "ORD10001",
        "PRDCT0101",
        2,
        60.00,
        100.00,
        0.10,
        200.00,
        20.00,
        180.00,
        120.00,
        60.00,
        0.00
    )
], [
    "OrderID",
    "ProductID",
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
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



existing_customer = spark.read.synapsesql(
    "retail_warehouse.dbo.dim_customer"
)

new_customers = (
    batch_customers
    .dropDuplicates(["CustomerID"])
    .join(
        existing_customer.select("CustomerID"),
        "CustomerID",
        "left_anti"
    )
)

max_customer_key = (
    existing_customer
    .agg(spark_max("CustomerKey"))
    .first()[0]
    or 0
)

window_customer = Window.orderBy("CustomerID")

new_customers = (
    new_customers
    .withColumn(
        "CustomerKey",
        (row_number().over(window_customer) + lit(max_customer_key)).cast("int")
    ).withColumn("Age", col("Age").cast("int"))

    .select(
        "CustomerKey",
        "CustomerID",
        "Gender",
        "Region",
        "City",
        "Age",
        "CustomerSegment",
        "SignUpDate"
    )
)

if new_customers.take(1):
    new_customers.write.mode("append").synapsesql(
        "retail_warehouse.dbo.dim_customer"
    )



existing_product = spark.read.synapsesql(
    "retail_warehouse.dbo.dim_product"
)

new_products = (
    batch_products
    .dropDuplicates(["ProductID"])
    .join(
        existing_product.select("ProductID"),
        "ProductID",
        "left_anti"
    )
)

max_product_key = (
    existing_product
    .agg(spark_max("ProductKey"))
    .first()[0]
    or 0
)

window_product = Window.orderBy("ProductID")

new_products = (
    new_products
    .withColumn(
        "ProductKey",
        (row_number().over(window_product) + lit(max_product_key)).cast("int")
    )
    .select(
        "ProductKey",
        "ProductID",
        "ProductName",
        "CategoryName"
    )
)

if new_products.take(1):
    new_products.write.mode("append").synapsesql(
        "retail_warehouse.dbo.dim_product"
    )




existing_date = spark.read.synapsesql(
    "retail_warehouse.dbo.dim_date"
)

new_dates = (
    batch_orders
    .select(col("OrderDate").alias("FullDate"))
    .distinct()
    .join(
        existing_date.select("FullDate"),
        "FullDate",
        "left_anti"
    )
    .withColumn(
        "DateKey",
        date_format("FullDate", "yyyyMMdd").cast("int")
    )
    .withColumn("Day", dayofmonth("FullDate"))
    .withColumn("Month", month("FullDate"))
    .withColumn("MonthName", date_format("FullDate", "MMMM"))
    .withColumn("Quarter", quarter("FullDate"))
    .withColumn("Year", year("FullDate"))
    .select(
        "DateKey",
        "FullDate",
        "Day",
        "Month",
        "MonthName",
        "Quarter",
        "Year"
    )
)

if new_customers.take(1):
    new_customers.write.mode("append").synapsesql(
        "retail_warehouse.dbo.dim_customer"
    )

# dim_product
existing_product = spark.read.synapsesql(
    "retail_warehouse.dbo.dim_product"
)

new_products = (
    batch_products
    .dropDuplicates(["ProductID"])
    .join(
        existing_product.select("ProductID"),
        "ProductID",
        "left_anti"
    )
)

max_product_key = (
    existing_product
    .agg(spark_max("ProductKey"))
    .first()[0]
    or 0
)

window_product = Window.orderBy("ProductID")

new_products = (
    new_products
    .withColumn(
        "ProductKey",
        (row_number().over(window_product) + lit(max_product_key)).cast("int")
    )
    .select(
        "ProductKey",
        "ProductID",
        "ProductName",
        "CategoryName"
    )
)

if new_products.take(1):
    new_products.write.mode("append").synapsesql(
        "retail_warehouse.dbo.dim_product"
    )


# dim_date

existing_date = spark.read.synapsesql(
    "retail_warehouse.dbo.dim_date"
)

new_dates = (
    batch_orders
    .select(col("OrderDate").alias("FullDate"))
    .distinct()
    .join(
        existing_date.select("FullDate"),
        "FullDate",
        "left_anti"
    )
    .withColumn(
        "DateKey",
        date_format("FullDate", "yyyyMMdd").cast("int")
    )
    .withColumn("Day", dayofmonth("FullDate"))
    .withColumn("Month", month("FullDate"))
    .withColumn("MonthName", date_format("FullDate", "MMMM"))
    .withColumn("Quarter", quarter("FullDate"))
    .withColumn("Year", year("FullDate"))
    .select(
        "DateKey",
        "FullDate",
        "Day",
        "Month",
        "MonthName",
        "Quarter",
        "Year"
    )
)

if new_dates.take(1):
    new_dates.write.mode("append").synapsesql(
        "retail_warehouse.dbo.dim_date"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Re-read dimensions after inserting new members
df_dim_customer = (
    spark.read
    .synapsesql("retail_warehouse.dbo.dim_customer")
    .select("CustomerKey", "CustomerID")
)

df_dim_product = (
    spark.read
    .synapsesql("retail_warehouse.dbo.dim_product")
    .select("ProductKey", "ProductID")
)

df_dim_date = (
    spark.read
    .synapsesql("retail_warehouse.dbo.dim_date")
    .select("DateKey", "FullDate")
)


fact_batch = (
    batch_order_details
    .join(batch_orders, "OrderID", "inner")
    .join(df_dim_product, "ProductID", "inner")
    .join(df_dim_customer, "CustomerID", "inner")
    .join(
        df_dim_date,
        col("OrderDate") == col("FullDate"),
        "inner"
    )
    .select(
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
)


# Match Warehouse decimal types
fact_batch = (
    fact_batch
    .withColumn("UnitCost", col("UnitCost").cast("decimal(18,2)"))
    .withColumn("UnitPrice", col("UnitPrice").cast("decimal(18,2)"))
    .withColumn("DiscountRate", col("DiscountRate").cast("decimal(5,4)"))
    .withColumn("GrossAmount", col("GrossAmount").cast("decimal(18,2)"))
    .withColumn("DiscountAmount", col("DiscountAmount").cast("decimal(18,2)"))
    .withColumn("NetAmount", col("NetAmount").cast("decimal(18,2)"))
    .withColumn("CostAmount", col("CostAmount").cast("decimal(18,2)"))
    .withColumn("ProfitAmount", col("ProfitAmount").cast("decimal(18,2)"))
    .withColumn("ReturnAmount", col("ReturnAmount").cast("decimal(18,2)"))
)


existing_fact = (
    spark.read
    .synapsesql("retail_warehouse.dbo.fact_sales")
    .select("OrderID", "ProductKey")
)


new_fact_rows = fact_batch.join(
    existing_fact,
    ["OrderID", "ProductKey"],
    "left_anti"
)


print("New fact rows:", new_fact_rows.count())

if new_fact_rows.take(1):
    new_fact_rows.write.mode("append").synapsesql(
        "retail_warehouse.dbo.fact_sales"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

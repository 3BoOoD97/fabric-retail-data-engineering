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

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create daily_sales table (Each row represents a single day and all sales that took place on that day)
from pyspark.sql.functions import col, sum, countDistinct
df_orders_silver = spark.table("silver.orders")
df_order_details_silver = spark.table("silver.order_details")


df_join_order_details_orders = df_orders_silver.join(df_order_details_silver, "OrderID", "inner").select(
    df_orders_silver["OrderID"],
    df_orders_silver["OrderDate"],
    df_order_details_silver["Quantity"],
    df_order_details_silver["GrossAmount"],
    df_order_details_silver["DiscountAmount"],
    df_order_details_silver["NetAmount"],
    df_order_details_silver["ReturnAmount"],
    df_order_details_silver["ProfitAmount"]
)

df_daily_sales = df_join_order_details_orders.groupBy("OrderDate").agg(
    countDistinct("OrderID").alias("TotalOrders"),
    sum("Quantity").alias("TotalQuantity"),
    sum("GrossAmount").alias("TotalGrossAmount"),
    sum("DiscountAmount").alias("TotalDiscountAmount"),
    sum("NetAmount").alias("TotalSalesAmount"),
    sum("ReturnAmount").alias("TotalReturnAmount"),
    sum("ProfitAmount").alias("TotalProfitBeforeReturns"),
    (sum("NetAmount")- sum("ReturnAmount")).alias("NetSalesAfterReturns")
)



df_daily_sales.show()

df_daily_sales.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold.daily_sales")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum, countDistinct


# Create product_performance table (Each row represents the total performance of each product)
df_categories_silver = spark.table("silver.categories")
df_products_silver = spark.table("silver.products")

df_join_product_performance = df_products_silver.join(df_order_details_silver,"ProductID","inner")\
.join(df_categories_silver,"CategoryID", "inner")\
.select(
    df_categories_silver["CategoryName"],
    df_order_details_silver["ProductID"],
    df_order_details_silver["Quantity"],
    df_order_details_silver["GrossAmount"],
    df_order_details_silver["DiscountAmount"],
    df_order_details_silver["NetAmount"],
    df_order_details_silver["ReturnAmount"],
    df_order_details_silver["ProfitAmount"],
    df_products_silver["ProductName"],
    df_order_details_silver["OrderID"],
)

df_product_performance= df_join_product_performance.groupBy("ProductID", "CategoryName", "ProductName").agg(
    countDistinct("OrderID").alias("TotalOrders"),
    sum("Quantity").alias("TotalQuantity"),
    sum("GrossAmount").alias("TotalGrossAmount"),
    sum("NetAmount").alias("TotalSalesAmount"),
    sum("ReturnAmount").alias("TotalReturnAmount"),
    sum("ProfitAmount").alias("TotalProfitBeforeReturns"),
    sum("DiscountAmount").alias("TotalDiscountAmount"),

    (sum("NetAmount")- sum("ReturnAmount")).alias("NetSalesAfterReturns")

)

df_product_performance.show()

df_product_performance.write.format("delta").mode("overwrite").saveAsTable("gold.product_performance")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create a customer_summary table (Each row represnts a customer, summarizing the entire purchasing activity over the full period)

df_customers_silver = spark.read.table("silver.customers")


df_customer_summary_join = df_orders_silver.join(df_customers_silver,"CustomerID", "inner")\
.join(df_order_details_silver,"OrderID","inner")\
.select(
    df_customers_silver["CustomerID"],
    df_customers_silver["CustomerSegment"],
    df_orders_silver["OrderID"],
    df_order_details_silver["Quantity"],
    df_order_details_silver["ReturnAmount"],
    df_order_details_silver["NetAmount"],
    df_order_details_silver["GrossAmount"],
    df_order_details_silver["DiscountAmount"],
    df_order_details_silver["ProfitAmount"]
)


df_customer_summary = df_customer_summary_join.groupBy("CustomerID","CustomerSegment").agg(
  countDistinct("OrderID").alias("TotalOrders"),
    sum("Quantity").alias("TotalQuantity"),
    sum("GrossAmount").alias("TotalGrossAmount"),
    sum("DiscountAmount").alias("TotalDiscountAmount"),
    sum("NetAmount").alias("TotalSalesAmount"),
    sum("ReturnAmount").alias("TotalReturnAmount"),
    sum("ProfitAmount").alias("TotalProfitBeforeReturns"),
    (sum("NetAmount") - sum("ReturnAmount")).alias("NetSalesAfterReturns")
)
df_customer_summary.show()

df_customer_summary.write.format("delta").mode("overwrite").saveAsTable("gold.customer_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

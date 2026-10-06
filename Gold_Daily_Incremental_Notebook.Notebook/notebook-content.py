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

# PARAMETERS CELL ********************

# This value will be overwritten by the Pipeline
ProcessDate = "2026-09-26"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_silver_orders= spark.table("silver.orders")
df_silver_order_details= spark.table("silver.order_details")


df_silver_orders.show()
#df_silver_order_details.show()


df_silver_orders_new_batch= df_silver_orders.filter(col("BatchDate") == ProcessDate)
df_silver_orders_new_batch.show()

df_affected_orders_dates = df_silver_orders_new_batch.select("OrderDate").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch the entire orders from the silver table based on df_affected_orders_dates so we can update the daily gold on that day

# Dates affected because the Order itself changed
dates_from_orders = (
    df_silver_orders_new_batch
    .select("OrderDate")
    .distinct()
)

# Dates affected because Order Details changed
dates_from_details = (
    df_silver_order_details
    .filter(col("BatchDate") == ProcessDate)
    .select("OrderID")
    .distinct()
    .join(df_silver_orders, "OrderID", "inner")
    .select("OrderDate")
    .distinct()
)

# All affected dates
affected_dates = (
    dates_from_orders
    .union(dates_from_details)
    .distinct()
)

# Get ALL orders belonging to those dates
orders_affected_date = df_silver_orders.join(
    affected_dates,
    "OrderDate",
    "inner"
)




orders_affected_date.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from silver.order_details LIMIT 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, sum, countDistinct

# UPSERT  gold.daily_sales TABLE


# Join orders_affected_date with order_details to get all orders details to get all the order details we need for the gold layer
orders_details_affected_date_join = df_silver_order_details.join(
    orders_affected_date, "OrderID","inner"
)

# Add Derived Aggregate Metric that matches the source table(gold.daily_sales)
df_daily_sales = orders_details_affected_date_join.groupBy("OrderDate").agg(
    countDistinct("OrderID").alias("TotalOrders"),
    sum("Quantity").alias("TotalQuantity"),
    sum("GrossAmount").alias("TotalGrossAmount"),
    sum("DiscountAmount").alias("TotalDiscountAmount"),
    sum("NetAmount").alias("TotalSalesAmount"),
    sum("ReturnAmount").alias("TotalReturnAmount"),
    sum("ProfitAmount").alias("TotalProfitBeforeReturns"),
    (sum("NetAmount")- sum("ReturnAmount")).alias("NetSalesAfterReturns")
)


# Prepare target & source tables
target = DeltaTable.forName(spark,"gold.daily_sales") 
source =  df_daily_sales


# UPSERT 
target.alias("target_table").merge(source.alias("source_table"), "target_table.OrderDate = source_table.OrderDate").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from gold.daily_sales where OrderDate='2026-09-26';
# MAGIC SELECT * FROM gold.product_performance LIMIT 1;
# MAGIC SELECT * from silver.orders LIMIT 1;
# MAGIC SELECT * FROM silver.order_details where BatchDate = '2026-09-26';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# UPSERT  gold.product_performance TABLE

# Get the the products that have changed in this batch
affected_product_ids = (
    df_silver_order_details
    .filter(col("BatchDate") == ProcessDate)
    .select("ProductID")
    .distinct()
)

display(affected_product_ids)


# Get the all orders that have same ProductID as in the batch 
get_all_products = df_silver_order_details.join(
    affected_product_ids,
    "ProductID",
    "inner"
)


# Create product_performance table (Each row represents the total performance of each product)
df_categories_silver = spark.table("silver.categories")
df_products_silver = spark.table("silver.products")


df_join_product_performance = get_all_products.join(df_products_silver,"ProductID","inner")\
.join(df_categories_silver,"CategoryID", "inner")\
.select(
    df_categories_silver["CategoryName"],
    get_all_products["ProductID"],
    get_all_products["Quantity"],
    get_all_products["GrossAmount"],
    get_all_products["DiscountAmount"],
    get_all_products["NetAmount"],
    get_all_products["ReturnAmount"],
    get_all_products["ProfitAmount"],
    df_products_silver["ProductName"],
    get_all_products["OrderID"],
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



# Prepare target & source tables
target = DeltaTable.forName(spark,"gold.product_performance") 
source =  df_product_performance

# UPSERT 
target.alias("target_table").merge(source.alias("source_table"), "target_table.ProductID = source_table.ProductID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If an order or order detail has changed, customer_summary should be updated

df_customers_silver = spark.read.table("silver.customers")

# Customers affected because the Order itself changed
customers_from_orders = df_silver_orders_new_batch.select("CustomerID").distinct()


# Customers affected because Order Details changed
customers_from_details = df_silver_order_details.filter(col("BatchDate") == ProcessDate).select("OrderID").distinct()\
    .join(df_silver_orders, "OrderID", "inner").select("CustomerID").distinct()


# All affected customers
affected_customer_ids = customers_from_orders.union(customers_from_details).distinct()



affected_customers = df_customers_silver.join(affected_customer_ids,"CustomerID","inner").select("CustomerID", "CustomerSegment")
display(affected_customers)




df_customer_summary_join = df_silver_orders.join(affected_customers,"CustomerID", "inner")\
.join(df_silver_order_details,"OrderID","inner")\
 .select(
        "CustomerID",
        "CustomerSegment",
        "OrderID",
        "Quantity",
        "ReturnAmount",
        "NetAmount",
        "GrossAmount",
        "DiscountAmount",
        "ProfitAmount"
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



# Prepare target & source tables
target = DeltaTable.forName(spark,"gold.customer_summary") 
source =  df_customer_summary

# UPSERT 
target.alias("target_table").merge(source.alias("source_table"), "target_table.CustomerID = source_table.CustomerID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 







# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Load Bronze tables
df_categories_bronze= spark.table("bronze.categories")
df_customers_bronze= spark.table("bronze.customers")
df_orders_bronze = spark.table("bronze.orders")
df_products_bronze = spark.table("bronze.products")
df_order_details_bronze = spark.table("bronze.order_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#customers transformations

from pyspark.sql.functions  import current_timestamp
df_customers_silver = df_customers_bronze.select(
    "CustomerID",
    "Gender",
    "Age",
    "City",
    "Region",
    "CustomerSegment",
    "SignUpDate"
).withColumn("silver_load_timestamp", current_timestamp())


df_customers_silver.write.format("delta").mode("overwrite").saveAsTable("silver.customers")


df_customers_silver.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#categories transformations

df_categories_silver = df_categories_bronze.select(
    "CategoryID",
    "CategoryName"
)

df_categories_silver.write.format("delta").mode("overwrite").saveAsTable("silver.categories")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#products transformations
df_products_silver = df_products_bronze.select(
    "ProductID",
    "ProductName",
    "CategoryID"
)

df_products_silver.write.format("delta").mode("overwrite").saveAsTable("silver.products")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# orders transformations
from pyspark.sql.functions import col, concat_ws, to_timestamp

df_orders_silver= df_orders_bronze.select(
        "OrderID",
        "CustomerID",
        "OrderDate",
        "OrderTime"
).withColumn(
    "OrderTimestamp",
    to_timestamp(
            concat_ws(
                " ",
                col("OrderDate").cast("string"),
                col("OrderTime")
            ),
            "yyyy-MM-dd HH:mm:ss"
    )
)

df_orders_silver.show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# order_details transformations
from pyspark.sql.functions import col, when, lit

#df_order_details_silver = df_order_details_bronze
df_update = df_order_details_bronze.withColumn("ReturnReason",
when(col("IsReturned")==0, lit(None).cast("string")).otherwise(col("ReturnReason"))).withColumn(
"ReturnDate", when(col("IsReturned")== 0, lit(None).cast("date")).otherwise(col("ReturnDate"))).withColumn(
"ReturnTime", when(col("IsReturned")== 0, lit(None).cast("string")).otherwise(col("ReturnTime")))


df_update.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

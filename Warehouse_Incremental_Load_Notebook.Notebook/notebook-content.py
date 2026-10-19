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
ProcessDate = "2026-10-10"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (col,date_format,dayofmonth,month,quarter,year)

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

df_orders_silver = spark.table("silver.orders")
df_order_details_silver = spark.table("silver.order_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get batched OrderIDs

df_batch_order_ids = (df_orders_silver.filter(col("BatchDate") == ProcessDate).select("OrderID")
    .union(df_order_details_silver.filter(col("BatchDate") == ProcessDate).select("OrderID")).distinct()
)

# Get batched Orders
df_batch_orders = (df_orders_silver.join(df_batch_order_ids,"OrderID","inner")
)

# Get batched order_details
df_batch_order_details = (df_order_details_silver.join(df_batch_order_ids,"OrderID","inner")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Date staging

df_stg_date = (df_batch_orders.select(col("OrderDate").alias("FullDate")).distinct()
    .withColumn("DateKey",date_format(col("FullDate"), "yyyyMMdd").cast("int"))
    .withColumn("Day",dayofmonth(col("FullDate")))

    .withColumn(
        "Month",
        month(col("FullDate"))
    )

    .withColumn(
        "MonthName",
        date_format(col("FullDate"), "MMMM")
    )

    .withColumn(
        "Quarter",
        quarter(col("FullDate"))
    )

    .withColumn(
        "Year",
        year(col("FullDate"))
    )

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

df_stg_date.write.mode("overwrite").synapsesql("retail_warehouse.dbo.stg_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fact staging

df_stg_fact_sales = (df_batch_order_details.join(df_batch_orders,"OrderID","inner")

    .select(
        "OrderID",
        "CustomerID",
        "ProductID",
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

df_stg_fact_sales.write.mode("overwrite").synapsesql("retail_warehouse.dbo.stg_fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("ProcessDate:", ProcessDate)

print("Batch Order IDs:", df_batch_order_ids.count())

print("Staged Dates:", df_stg_date.count())

print("Staged Fact Rows:", df_stg_fact_sales.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

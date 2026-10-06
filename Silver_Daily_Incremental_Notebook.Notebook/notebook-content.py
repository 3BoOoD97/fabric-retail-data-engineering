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

from pyspark.sql.functions import col, to_timestamp, concat_ws
from delta.tables import DeltaTable


date_path = ProcessDate.replace("-", "/")


df_orders_bronze = spark.table("bronze.orders").filter(col("BatchDate")==ProcessDate)
df_order_details_bronze = spark.table("bronze.order_details").filter(col("BatchDate")==ProcessDate)

df_orders_bronze.show()
df_order_details_bronze.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM silver.orders WHERE OrderID = 'ORD10003' or OrderID="ORD10002";
# MAGIC SELECT * FROM silver.order_details WHERE OrderID IN ('ORD10002', 'ORD10003');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# orders transformation 

df_orders_silver= df_orders_bronze.select("OrderID", "CustomerID", "OrderDate", "OrderTime", "BatchDate").withColumn(
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
 
df_orders_silver.show()

# df_orders_silver -> orders table UPSERT
# Prepare target & source tables
source_orders = df_orders_silver
target_orders = DeltaTable.forName(spark, "silver.orders")

target_orders.alias("target_table").merge(source_orders.alias("source_table"), "target_table.OrderID = source_table.OrderID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# order_details transformations
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import  DecimalType


df_order_details_silver = df_order_details_bronze.withColumn("ReturnReason",
when(col("IsReturned")==0, lit(None).cast("string")).otherwise(col("ReturnReason"))).withColumn(
"ReturnDate", when(col("IsReturned")== 0, lit(None).cast("date")).otherwise(col("ReturnDate"))).withColumn(
"ReturnTime", when(col("IsReturned")== 0, lit(None).cast("string")).otherwise(col("ReturnTime")))


#Convert Unit Cost&Price and DiscountRate to Decimal before adding Derived columns
df_order_details_silver = df_order_details_silver.withColumn(
    "UnitCost", col("UnitCost").cast(DecimalType(18, 2))
).withColumn(
    "UnitPrice", col("UnitPrice").cast(DecimalType(18, 2))
).withColumn(
    "DiscountRate", col("DiscountRate").cast(DecimalType(5, 4))
)



# ===== Derived columns ========


# GrossAmount col
df_order_details_silver= df_order_details_silver.withColumn(
    "GrossAmount", 
    (col("UnitPrice") * col("Quantity")).cast(DecimalType(18, 2))
).withColumn(
    "DiscountAmount",
    (col("GrossAmount") * col("DiscountRate")).cast(DecimalType(18, 2))
).withColumn(
    "NetAmount",
    (col("GrossAmount") - col("DiscountAmount")).cast(DecimalType(18, 2))
). withColumn(
    "CostAmount",
    (col("Quantity") * col("UnitCost")).cast(DecimalType(18, 2))
).withColumn(
    "ProfitAmount",
    (col("NetAmount") - col("CostAmount")).cast(DecimalType(18, 2))
).withColumn(
    "ReturnAmount",
    when(col("IsReturned")==1, col("NetAmount")).otherwise((lit(0))
    ).cast(DecimalType(18, 2))
)


#df_order_details_silver = df_order_details_silver.drop("BatchDate")

df_order_details_silver.show()

# df_order_details_silver -> order_details table UPSERT
# Prepare target & source tables
source_order_details = df_order_details_silver
target_order_details = DeltaTable.forName(spark, "silver.order_details")

target_order_details.alias("target_table").merge(source_order_details.alias("source_table"), "target_table.OrderID = source_table.OrderID AND target_table.ProductID = source_table.ProductID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM silver.orders WHERE OrderID = 'ORD10003' or OrderID="ORD10002";
# MAGIC SELECT * FROM silver.order_details WHERE OrderID IN ('ORD10002', 'ORD10003');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

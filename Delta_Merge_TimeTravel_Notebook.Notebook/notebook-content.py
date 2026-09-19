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

# Create a copy of silver.orders to test incremental update
from pyspark.sql.functions import col
from datetime import date, datetime
from delta.tables import DeltaTable

df_silver_orders=spark.table("silver.orders")

# Create a copy of silver.orders table
orders_merge_tst = df_silver_orders
orders_merge_tst.write.format('delta').mode("overwrite").saveAsTable("silver.orders_merge_tst")

orders_merge_tst.orderBy(col("OrderID").desc()).show(10)

# Mock df to test
df_source = spark.createDataFrame([
    ("ORD09991", "CSTMR0175", date(2025, 2, 22), "16:42:55", datetime(2025, 2, 22, 16, 42, 55)), # Exsists 
    ("ORD09992", "CSTMR0999", date(2025, 3, 21), "16:42:55", datetime(2024, 3, 21, 16, 42, 55)), # Exsists 
    ("ORD09993", "CSTMR0175", date(2025, 3, 21), "16:42:55", datetime(2024, 3, 21, 16, 42, 55)), # Exsists 
    ("ORD10001", "CSTMR0175", date(2024, 3, 21), "16:42:55", datetime(2024, 3, 21, 16, 42, 55)), # New OrderID
    ("ORD10002", "CSTMR0233", date(2024, 3, 21), "16:42:55", datetime(2024, 3, 21, 16, 42, 55)), # New OrderID
    ("ORD10003", "CSTMR0891", date(2024, 3, 21), "16:42:55", datetime(2024, 3, 21, 16, 42, 55)), # New OrderID
    ],
    schema=["OrderID", "CustomerID", "OrderDate", "OrderTime", "OrderTimestamp"])


# Prepare target & source tables
target = DeltaTable.forName(spark,"silver.orders_merge_tst")
source = df_source

# Read the table version before merge
df_history_before_merge=spark.sql("DESCRIBE HISTORY silver.orders_merge_tst")
before_merge_version = df_history_before_merge.agg({"version": "max"}).collect()[0][0]

# UPSERT
target.alias("target_table").merge(source.alias("source_table"), "target_table.OrderID = source_table.OrderID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 
df_history_after_merge=spark.sql("DESCRIBE HISTORY silver.orders_merge_tst")
display(df_history_after_merge)


orders_merge = spark.table("silver.orders_merge_tst")
orders_merge.orderBy(col("OrderID").desc()).show(15)


# Time Travel to previous version
df_before_merge = spark.read.format("delta").option("versionAsOf", 10).table("silver.orders_merge_tst")
df_before_merge.orderBy(col("OrderID").desc()).show(15)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

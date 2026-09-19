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


from datetime import datetime
from pyspark.sql.functions import col

# heck active file count and table size
spark.sql("DESCRIBE DETAIL silver.customers").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)
spark.sql("DESCRIBE DETAIL silver.order_details").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)
spark.sql("DESCRIBE DETAIL silver.products").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)
spark.sql("DESCRIBE DETAIL silver.orders").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)

# All tables have 1 Parquet file with small sizeInBytes !!!

# Test optimize
spark.sql("OPTIMIZE silver.customers")
spark.sql("OPTIMIZE silver.order_details")
spark.sql("OPTIMIZE silver.products")
spark.sql("OPTIMIZE silver.orders")


spark.sql("DESCRIBE DETAIL silver.customers").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)
spark.sql("DESCRIBE DETAIL silver.order_details").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)
spark.sql("DESCRIBE DETAIL silver.products").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)
spark.sql("DESCRIBE DETAIL silver.orders").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)
# Optimize did not achieve additional compaction because the table was already a single file !!!

# Test ZORDER for order_details table
spark.sql("OPTIMIZE silver.order_details ZORDER BY (OrderID)")
spark.sql("DESCRIBE DETAIL silver.order_details").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)


# Test vacum on order_details table
detail = spark.sql("DESCRIBE DETAIL silver.order_details")
detail.select("location").show(truncate=False)


location = detail.select("location").first()[0]

files = notebookutils.fs.ls(location)

for f in files:
    print(f.name, f.size)


df_history=spark.sql("DESCRIBE HISTORY silver.order_details").select("version", "timestamp", "operation","operationMetrics")
display(df_history)


# Get version 9 timestamp d
version_9_time = df_history.filter(col("version") == 9).select("timestamp").first()[0]

# Calculate the difference between current time and version 9
diff_hours = (datetime.now() - version_9_time).total_seconds() / 3600

# Disable the default protection  
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# Delete any file that has been left unused for a period exceeding the diff_hours
spark.sql(f"VACUUM silver.order_details RETAIN {diff_hours} HOURS")


detail = spark.sql("DESCRIBE DETAIL silver.order_details")
detail.select("location").show(truncate=False)


location = detail.select("location").first()[0]

files = notebookutils.fs.ls(location)

for f in files:
    print(f.name, f.size)


df_history=spark.sql("DESCRIBE HISTORY silver.order_details").select("version", "timestamp", "operation","operationMetrics")
display(df_history)

# Enable the default protection  
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

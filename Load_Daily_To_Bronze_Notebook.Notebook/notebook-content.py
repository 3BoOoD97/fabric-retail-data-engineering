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

# This value will be overwritten by the Pipeline
ProcessDate = "2026-09-26"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col,trim, lit

date_path = ProcessDate.replace("-", "/")


df_daily_order_update = spark.read.csv(f"Files/incoming/orders/{date_path}/orders_daily.csv", header=True)
df_daily_order_details_update = spark.read.csv(f"Files/incoming/order_details/{date_path}/order_details_daily.csv", header=True)

df_raw_order_details_bronze = spark.read.table("bronze.order_details")
df_raw_order_bronze = spark.table("bronze.orders")



df_daily_order_update.show()
# 2 ROWS!
df_daily_order_details_update.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_daily_order_update.printSchema()
spark.table("bronze.orders").printSchema()


df_daily_order_update = (
    df_daily_order_update
    .withColumn("OrderID", trim(col("OrderID")))
    .withColumn("CustomerID", trim(col("CustomerID")))
    .withColumn("OrderDate", trim(col("OrderDate")))
    .withColumn("OrderTime", trim(col("OrderTime")))
)


df_daily_order_update = df_daily_order_update.withColumn("OrderDate",col("OrderDate").cast("date")).withColumn("BatchDate", lit(ProcessDate).cast("DATE"))

# Prepare target & source tables
source_order = df_daily_order_update
target_order = DeltaTable.forName(spark, "bronze.orders")

target_order.alias("target_table").merge(source_order.alias("source_table"), "target_table.OrderID = source_table.OrderID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM bronze.orders WHERE OrderID = 'ORD10003' or OrderID="ORD10002";
# MAGIC 
# MAGIC SELECT * FROM bronze.orders LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_daily_order_details_update.printSchema()
spark.table("bronze.order_details").printSchema()

df_daily_order_details_update = (
    df_daily_order_details_update
    .withColumn("OrderID", trim(col("OrderID")))
    .withColumn("ProductID", trim(col("ProductID")))
    .withColumn("ReturnTime", trim(col("ReturnTime")))
    .withColumn("ReturnReason", trim(col("ReturnReason")))
)


df_daily_order_details_update = (
    df_daily_order_details_update
    .withColumn("Quantity", col("Quantity").cast("INT"))
    .withColumn("UnitCost", col("UnitCost").cast("double"))
    .withColumn("UnitPrice", col("UnitPrice").cast("double"))
    .withColumn("DiscountRate", col("DiscountRate").cast("double"))
    .withColumn("IsReturned", col("IsReturned").cast("INT"))
    .withColumn("ReturnDate", col("ReturnDate").cast("DATE"))
    .withColumn("BatchDate", lit(ProcessDate).cast("DATE"))
)


# Prepare target & source tables
source_order_details = df_daily_order_details_update
target_order_details = DeltaTable.forName(spark, "bronze.order_details")

target_order_details.alias("target_table").merge(source_order_details.alias("source_table"), "target_table.OrderID = source_table.OrderID AND target_table.ProductID = source_table.ProductID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

        


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM bronze.order_details WHERE OrderID IN ('ORD10002', 'ORD10003');
# MAGIC 
# MAGIC SELECT * FROM bronze.order_details LIMIT 10;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

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

# Load the tables from the silver layer

df_categories_silver=spark.table("silver.categories")
df_customers_silver=spark.table("silver.customers")
df_order_details_silver=spark.table("silver.order_details")
df_orders_silver=spark.table("silver.orders")
df_products_silver=spark.table("silver.products")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


windowSpec = Window.orderBy("CustomerID")

df_dim_customer = df_customers_silver.withColumn("CustomerKey", row_number().over(windowSpec))

df_dim_customer = df_dim_customer.select("CustomerKey", "CustomerID", "Gender", "Region", "City", "Age", "CustomerSegment", "SignUpDate")

df_dim_customer.write.mode("append").synapsesql("retail_warehouse.dbo.dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM retail_warehouse.dbo.dim_customer;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


windowSpec = Window.orderBy("ProductID")

df_dim_products = df_categories_products_join = df_categories_silver.join(df_products_silver,"CategoryID", "inner")\
.select("CategoryName","ProductID", "ProductName").withColumn("ProductKey", row_number().over(windowSpec))

df_dim_products.write.mode("append").synapsesql("retail_warehouse.dbo.dim_product")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM retail_warehouse.dbo.dim_product;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col,date_format,dayofmonth,month,quarter,year
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


df_dim_date = (df_orders_silver.select(col("OrderDate").alias("FullDate")).distinct()
    .withColumn("DateKey", date_format(col("FullDate"), "yyyyMMdd").cast("int"))
    .withColumn("Day", dayofmonth(col("FullDate")))
    .withColumn("Month", month(col("FullDate")))
    .withColumn("MonthName", date_format(col("FullDate"), "MMMM"))
    .withColumn("Quarter", quarter(col("FullDate")))
    .withColumn("Year", year(col("FullDate")))
    .select("DateKey","FullDate", "Day", "Month", "MonthName", "Quarter","Year"
    ).orderBy("FullDate")
)

df_dim_date.write .mode("append").synapsesql("retail_warehouse.dbo.dim_date")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT count (*) FROM retail_warehouse.dbo.dim_date


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# One row = one product within one order
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col,date_format,dayofmonth,month,quarter,year
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


# Read dimensions from Warehouse
df_dim_product = spark.read.synapsesql("retail_warehouse.dbo.dim_product").select("ProductKey", "ProductID")

df_dim_customer = spark.read.synapsesql("retail_warehouse.dbo.dim_customer").select("CustomerKey", "CustomerID")

df_dim_date = spark.read.synapsesql("retail_warehouse.dbo.dim_date").select("DateKey", "FullDate")


df_dim_fact_sales = df_order_details_silver.join(df_orders_silver, "OrderID", "inner").select("OrderID", "ProductID", "CustomerID", "OrderDate", "Quantity", "UnitCost", "UnitPrice", "DiscountRate", "GrossAmount",
"DiscountAmount", "NetAmount", "CostAmount","ProfitAmount","ReturnAmount")


df_dim_fact_sales = df_dim_fact_sales.join(df_dim_product, "ProductID", "inner")

df_dim_fact_sales = df_dim_fact_sales.join(df_dim_customer, "CustomerID", "inner")

df_dim_fact_sales = df_dim_fact_sales.join( df_dim_date, df_dim_fact_sales["OrderDate"] == df_dim_date["FullDate"], "inner")

# Keep only the columns required by fact_sales
df_dim_fact_sales = df_dim_fact_sales.select("OrderID", "CustomerKey", "ProductKey", "DateKey", "Quantity", "UnitCost", 
"UnitPrice", "DiscountRate", "GrossAmount", "DiscountAmount", "NetAmount", "CostAmount", "ProfitAmount", "ReturnAmount")

# Load into Warehouse
df_dim_fact_sales.write.mode("append").synapsesql("retail_warehouse.dbo.fact_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM retail_warehouse.dbo.fact_sales;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_product.groupBy("ProductID").count().filter("count > 1").show()

df_dim_customer.groupBy("CustomerID").count().filter("count > 1").show()

df_dim_date.groupBy("FullDate").count().filter("count > 1").show()

df_dim_fact_sales.groupBy("OrderID", "ProductKey").count().filter("count > 1").show()

print(df_dim_fact_sales.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

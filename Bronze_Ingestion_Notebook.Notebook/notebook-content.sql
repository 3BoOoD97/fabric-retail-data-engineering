-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "39e65af4-dd0a-40e7-8f69-e6fd524c0401",
-- META       "default_lakehouse_name": "retail_Lakhouse",
-- META       "default_lakehouse_workspace_id": "aa7643ac-cb35-4247-a5f0-491f80de52cb",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "39e65af4-dd0a-40e7-8f69-e6fd524c0401"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- bronze.customers Exploration SPARK SQL
SELECT * FROM bronze.customers LIMIT 10;
SELECT COUNT(*) FROM bronze.customers; 
DESCRIBE bronze.customers;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


%%pyspark
# bronze.customers Exploration PYSPARK


from pyspark.sql.functions import col, sum, min, max, trim

# Check if we have null values
df = spark.table("bronze.customers")
condition = " OR ".join([f"{c} IS NULL" for c in df.columns])
df.filter(condition).show()


for c in df.columns:
    null_count= df.filter(col(c).isNull()).count()
    print(c, null_count)


# Check empty strings
str_columns = [col_name for col_name, data_type in df.dtypes if data_type == "string"]

for c in str_columns:
    empty_count = df.filter(trim(col(c))== "").count()
    print(c, empty_count)


# Check Categorical values
df.groupBy("Gender").count().show()
df.groupBy("Region").count().show()
df.groupBy("CustomerSegment").count().show()


# Duplicate rows
duplicates_rows = df.groupBy(df.columns).count().filter(col("count") > 1)
duplicates_rows.show()

# Duplicate keys
duplicates_keys = df.groupBy(df.CustomerID).count().filter(col("count")>1)
duplicates_keys.show()

# Check if the CustomerID is unique
total_rows = df.count()
total_CID = df.select("CustomerID").distinct().count()
print("Rows:", total_rows)
print("Unique customers:", total_CID)

# Min / max values
min_age =  df.select(min("Age"))
min_age.show()

max_age = df.select(max("Age"))
max_age.show()

min_sign_date= df.select(min("SignUpDate"))
min_sign_date.show()

max_sign_date= df.select(max("SignUpDate"))
max_sign_date.show()


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- bronze.categories Exploration SPARK SQL
SELECT * FROM bronze.categories LIMIT 10;

SELECT count(*) FROM bronze.categories;

DESCRIBE bronze.categories;

SELECT * FROM bronze.categories
WHERE CategoryID IS NULL OR CategoryName IS NULL;

SELECT CategoryID, COUNT(*) AS cnt FROM bronze.categories
GROUP BY CategoryID
HAVING COUNT(*) > 1;

SELECT * FROM bronze.categories
WHERE TRIM(CategoryID) = '' OR TRIM(CategoryName) = '';



-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- bronze.products Exploration SPARK SQL

SELECT * FROM bronze.products LIMIT 10;

SELECT COUNT(*) FROM bronze.products;

DESCRIBE bronze.products;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # bronze.products Exploration PYSPARK
-- MAGIC 
-- MAGIC from pyspark.sql.functions import col, trim
-- MAGIC df= spark.table("bronze.products")
-- MAGIC 
-- MAGIC # Check null values
-- MAGIC condition = " OR ".join([f"{c} IS NULL" for c in df.columns])
-- MAGIC df.filter(condition).show()
-- MAGIC 
-- MAGIC # Check empty values
-- MAGIC for c in df.columns:
-- MAGIC     empty_count = df.filter(trim(col(c))== "").count()
-- MAGIC     print(c, empty_count)
-- MAGIC     
-- MAGIC # Check total & unique ProductID
-- MAGIC total_PID= df.select("ProductID").count()
-- MAGIC unique_PID= df.select("ProductID").distinct().count()
-- MAGIC 
-- MAGIC 
-- MAGIC print("Total ProductID: ",total_PID)
-- MAGIC print("Total unique ProductID: ",unique_PID)
-- MAGIC 
-- MAGIC 
-- MAGIC # Check CategoryID values
-- MAGIC categoryID_products_list = df.select("CategoryID").distinct()
-- MAGIC categoryID_products_list.show()
-- MAGIC 
-- MAGIC # Check if all CategoryID vlaues exsit in categories table
-- MAGIC df_cata= spark.table("bronze.categories")
-- MAGIC categoryID_categories_list = df_cata.select("CategoryID")
-- MAGIC category_diff = categoryID_products_list.exceptAll(categoryID_categories_list).count()
-- MAGIC print("CategoryIDs in products missing from categories: ",category_diff)
-- MAGIC 
-- MAGIC 
-- MAGIC # Check CategoryID distribution
-- MAGIC df_dis = df.groupBy("CategoryID").count()
-- MAGIC df_dis.show()


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- bronze.orders Exploration SPARK SQL
SELECT * FROM bronze.orders LIMIT 10;

SELECT count(*) FROM bronze.orders;

DESCRIBE bronze.orders;

-- NOTE: Automatic schema inference incorrectly interpreted the source OrderTime field as a timestamp and attached an artificial date.
-- Therefore, this table will be loaded into a table using PYSPAK below 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # Load the orders.csv and save it as a table 
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, DateType
-- MAGIC 
-- MAGIC schema = StructType([
-- MAGIC     StructField("OrderID", StringType(), True),
-- MAGIC     StructField("CustomerID", StringType(), True),
-- MAGIC     StructField("OrderDate", DateType(), True),
-- MAGIC     StructField("OrderTime", StringType(), True)
-- MAGIC ])
-- MAGIC 
-- MAGIC 
-- MAGIC df = (spark.read.option("header", True)
-- MAGIC     .schema(schema)
-- MAGIC     .csv("Files/raw/orders.csv")
-- MAGIC )
-- MAGIC df.printSchema()
-- MAGIC 
-- MAGIC 
-- MAGIC df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.orders")
-- MAGIC df.show(10, truncate=False)
-- MAGIC 


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # bronze.orders Exploration PYSPARK
-- MAGIC 
-- MAGIC from pyspark.sql.functions import trim, col, min, max
-- MAGIC df_orders= spark.table("bronze.orders")
-- MAGIC 
-- MAGIC # Check null values
-- MAGIC condition = " OR ".join([f"{c} IS NULL" for c in df_orders.columns])
-- MAGIC df_orders.filter(condition).show()
-- MAGIC 
-- MAGIC # Check Empty strings
-- MAGIC str_columns = []
-- MAGIC 
-- MAGIC for col_name, data_type in df_orders.dtypes:
-- MAGIC     if data_type == "string":
-- MAGIC         str_columns.append(col_name)
-- MAGIC 
-- MAGIC     
-- MAGIC for c in str_columns:
-- MAGIC     empty_count = df_orders.filter(trim(col(c))== "").count()
-- MAGIC     print(c, empty_count)
-- MAGIC 
-- MAGIC # Check if OrderID are uniuqe 
-- MAGIC Total_orders_rows= df_orders.count()
-- MAGIC total_OID= df_orders.select("OrderID").distinct().count()
-- MAGIC 
-- MAGIC print("Total rows: ", Total_orders_rows)
-- MAGIC print("Total unique Order ID: ", total_OID)
-- MAGIC 
-- MAGIC # Check if every customerID in order table exsist in customers table
-- MAGIC df_customers= spark.table("bronze.customers")
-- MAGIC df_customers_ID = df_customers.select("CustomerID")
-- MAGIC df_orders_CusID = df_orders.select("CustomerID")
-- MAGIC customersID_diff = df_orders_CusID.exceptAll(df_customers_ID).count()
-- MAGIC print("customersID in orders missing: ",customersID_diff)
-- MAGIC 
-- MAGIC # OrderDate range
-- MAGIC min_order_date = df_orders.select(min("OrderDate"))
-- MAGIC max_order_date = df_orders.select(max("OrderDate"))
-- MAGIC min_order_date.show()
-- MAGIC max_order_date.show()
-- MAGIC 
-- MAGIC 
-- MAGIC # Check if the OrderTime is in this format HH:mm:ss
-- MAGIC simple_regex = r"^\d{2}:\d{2}:\d{2}$"
-- MAGIC invalid_OrderTime_count = df_orders.filter(~col("OrderTime").rlike(simple_regex)).count()
-- MAGIC 
-- MAGIC print("Total invalid order date:", invalid_OrderTime_count)
-- MAGIC 
-- MAGIC # Check total orders for each customers
-- MAGIC total_orders_cus= df.groupBy("CustomerID").count()
-- MAGIC total_orders_cus.show()

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * from bronze.order_details LIMIT 10;

SELECT count(*) from bronze.order_details;

DESCRIBE bronze.order_details;

-- This table will be re-uplaoded using PYSPAK to avoid adding an automate date to ReturnTime col

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # Load the order_details.csv and save it as a table 
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType, DecimalType
-- MAGIC from pyspark.sql.functions import col, min, max, trim, to_date, to_timestamp, current_date, lit
-- MAGIC 
-- MAGIC # Define the schema and load the CSV file and save it as a delta table
-- MAGIC schema = StructType([
-- MAGIC     StructField("OrderID", StringType(), True),
-- MAGIC     StructField("ProductID", StringType(), True),
-- MAGIC     StructField("Quantity", IntegerType(), True),
-- MAGIC     StructField("UnitCost", DoubleType(), True),
-- MAGIC     StructField("UnitPrice", DoubleType(), True),
-- MAGIC     StructField("DiscountRate", DoubleType(), True),
-- MAGIC     StructField("IsReturned", IntegerType(), True),
-- MAGIC     StructField("ReturnDate", DateType(), True),
-- MAGIC     StructField("ReturnTime", StringType(), True),
-- MAGIC     StructField("ReturnReason", StringType(), True),
-- MAGIC 
-- MAGIC ])
-- MAGIC 
-- MAGIC 
-- MAGIC df= spark.read.option("header", True).schema(schema).csv("Files/raw/order_details.csv")
-- MAGIC df.show()
-- MAGIC df.printSchema()
-- MAGIC 
-- MAGIC df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.order_details")
-- MAGIC df.show(10, truncate=False)
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC # Check null values
-- MAGIC from pyspark.sql.functions import col
-- MAGIC condition = " OR ".join([f"{col} IS NULL" for col in df.columns])
-- MAGIC 
-- MAGIC df.filter(condition).show()
-- MAGIC 
-- MAGIC 
-- MAGIC # Check if we have duplicate rows
-- MAGIC df_unique_rows= df.groupBy(df.columns).count()
-- MAGIC df_unique_rows.filter(col("count") > 1).show()
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC # Check if the same product may appear twice within the same order
-- MAGIC df_unique_orderID_ProductID=df.groupBy("OrderID","ProductID").count()
-- MAGIC df_unique_orderID_ProductID.filter(col("count") > 1).show()
-- MAGIC 
-- MAGIC 
-- MAGIC # Check if every orderID exsist in orders table 
-- MAGIC df_orders_orderID= spark.table("bronze.orders").select("OrderID").distinct()
-- MAGIC df_ordersDet_orderID=df.select("OrderID").distinct()
-- MAGIC 
-- MAGIC dup_orderID_counter= df_ordersDet_orderID.exceptAll(df_orders_orderID).count()
-- MAGIC print(f"Number of invalid OrderIDs: {dup_orderID_counter}")
-- MAGIC 
-- MAGIC 
-- MAGIC # Check if every ProductID exsist in products table
-- MAGIC df_products_productID= spark.table("bronze.products").select("ProductID").distinct()
-- MAGIC df_ordersDet_productID= df.select("ProductID").distinct()
-- MAGIC 
-- MAGIC dup_productID_counter= df_ordersDet_productID.exceptAll(df_products_productID).count()
-- MAGIC print(f"Number of invalid ProductID: {dup_productID_counter}")
-- MAGIC 
-- MAGIC 
-- MAGIC # Check negative values & Min-Max for:
-- MAGIC # Quantity
-- MAGIC df_quantity_checker = df.filter(col("Quantity")<0).show()
-- MAGIC df_quantity_min= df.select(min("Quantity")).show()
-- MAGIC df_quantity_max= df.select(max("Quantity")).show()
-- MAGIC 
-- MAGIC # UnitCost
-- MAGIC df_UnitCost_checker = df.filter(col("UnitCost")<0).show()
-- MAGIC df_UnitCost_min= df.select(min("UnitCost")).show()
-- MAGIC df_UnitCost_max=df.select(max("UnitCost")).show()
-- MAGIC 
-- MAGIC # UnitPrice
-- MAGIC df_UnitCost_checker = df.filter(col("UnitPrice")<0).show()
-- MAGIC df_UnitCost_min= df.select(min("UnitPrice")).show()
-- MAGIC df_UnitCost_max=df.select(max("UnitPrice")).show()
-- MAGIC 
-- MAGIC 
-- MAGIC # Check if the value is not less than 0 or bigger than 1 for:
-- MAGIC # DiscountRate
-- MAGIC df_UnitCost_checker = df.filter((col("DiscountRate")>1) | (col("DiscountRate")<0)).show()
-- MAGIC 
-- MAGIC # IsReturned
-- MAGIC df_UnitCost_checker = df.filter((col("IsReturned")>1) | (col("IsReturned")<0)).show()
-- MAGIC 
-- MAGIC 
-- MAGIC #df_quantity_checker = df.filter(col("IsReturned")==1).show()
-- MAGIC #df_IsReturned_col_value= df.select("IsReturned").distinct().show() # 0 = NOT Returned, 1 = Returned
-- MAGIC 
-- MAGIC 
-- MAGIC df_IsReturned_col_value = df.select("IsReturned")
-- MAGIC 
-- MAGIC # Check when IsReturned is false then ReturnDate=9999-12-31, ReturnTime= 00:00:00, ReturnReason= None
-- MAGIC df_Not_Returned = df.filter(col("IsReturned")==0)
-- MAGIC df_Not_Returned_checker = df_Not_Returned.filter((col("ReturnDate")!=lit("9999-12-31").cast("date")) | 
-- MAGIC (col("ReturnTime")!="00:00:00") | (col("ReturnReason")!= "None"))
-- MAGIC df_Not_Returned_checker.show()
-- MAGIC 
-- MAGIC # Check when IsReturned is true then ReturnDate, ReturnTime and ReturnReason have real values
-- MAGIC df_IsReturned = df.filter(col("IsReturned")==1)
-- MAGIC 
-- MAGIC df_IsReturned_checker =df_IsReturned.filter(
-- MAGIC     (to_date(col("ReturnDate"), "yyyy-MM-dd").isNull()) | 
-- MAGIC     (to_date(col("ReturnDate"), "yyyy-MM-dd")> current_date()) | 
-- MAGIC     (col("ReturnDate") == lit("9999-12-31").cast("date")) |
-- MAGIC     (col("ReturnTime").isNull()) |
-- MAGIC     (trim(col("ReturnTime")) == "") |
-- MAGIC     (col("ReturnTime") == "00:00:00") |
-- MAGIC     to_timestamp(col("ReturnTime"), "HH:mm:ss").isNull() |
-- MAGIC     col("ReturnReason").isNull() |
-- MAGIC     (trim(col("ReturnReason")) == "") |
-- MAGIC     (trim(col("ReturnReason")) == "None")
-- MAGIC     )
-- MAGIC df_IsReturned_checker.show()
-- MAGIC 
-- MAGIC 
-- MAGIC # Check if ReturnDate >= OrderDate
-- MAGIC df_orders= spark.table("bronze.orders")
-- MAGIC df_join_order_orderDet = df_orders.join(df_IsReturned,"OrderID","inner")
-- MAGIC 
-- MAGIC df_ReturnDate_OrderDate_Checker = df_join_order_orderDet.filter(col("ReturnDate")<col("OrderDate")).show()


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

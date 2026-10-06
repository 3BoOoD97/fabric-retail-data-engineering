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

ProcessDate = "2026-09-26"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim, expr


# Convert ProcessDate to folder path
date_path = ProcessDate.replace("-", "/")

# Read daily orders
df_orders = spark.read.option("header", True).csv(f"Files/incoming/orders/{date_path}/orders_daily.csv")

# Read daily order details
df_details = spark.read.option("header", True).csv(f"Files/incoming/order_details/{date_path}/order_details_daily.csv")

# Check results
print("Orders:", df_orders.count())
print("Orders columns:", df_orders.columns)

print("Order details:", df_details.count())
print("Order details columns:", df_details.columns)

display(df_orders)
display(df_details)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema and null validation
               
            
###### Schema Validation :
# Orders table
orders_required_col = ["OrderID","CustomerID","OrderDate","OrderTime"]
orders_col=df_orders.columns

df_missing_order_col = set(orders_required_col) - set(orders_col)

print("Number of missing orders columns: ", len(df_missing_order_col))
print("Missing orders columns: ", df_missing_order_col)

# order_details table
order_details_required_col = ['OrderID', 'ProductID', 'Quantity', 'UnitCost', 'UnitPrice', 'DiscountRate', 'IsReturned', 'ReturnDate', 'ReturnTime', 'ReturnReason']
order_details_col= df_details.columns
df_missing_order_details_col = set(order_details_required_col) - set(order_details_col)
print("Number of missing order details columns:", len(df_missing_order_details_col))
print("Missing order details columns: ", df_missing_order_details_col)


if df_missing_order_col or df_missing_order_details_col:
    print("VALIDATION FAILED: Missing required columns")
    notebookutils.notebook.exit("False")

###### Null Validation:
# Orders table
invalid_orders = df_orders.filter( col("OrderID").isNull() | (trim(col("OrderID")) == "") | col("CustomerID").isNull() |(trim(col("CustomerID")) == ""))
print("Invalid orders:", invalid_orders.count())


# order_details table
invalid_details = df_details.filter(col("OrderID").isNull() | (trim(col("OrderID")) == "") | col("ProductID").isNull() | (trim(col("ProductID")) == ""))
print("Invalid order details:", invalid_details.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# order_details_daily range validation

df_details.describe()


# Create DataFrame for Cast Validation
df_cast = (
    df_details
    .withColumn("Quantity_cast", expr("try_cast(Quantity AS INT)"))
    .withColumn("UnitCost_cast", expr("try_cast(UnitCost AS DECIMAL(18,2))"))
    .withColumn("UnitPrice_cast", expr("try_cast(UnitPrice AS DECIMAL(18,2))"))
    .withColumn("DiscountRate_cast", expr("try_cast(DiscountRate AS DECIMAL(5,4))"))
    .withColumn("IsReturned_cast", expr("try_cast(IsReturned AS INT)"))
)


# Quantity
invalid_quantity = df_cast.filter(col("Quantity_cast") <= 0)
print("Invalid Quantity:", invalid_quantity.count())

# UnitCost
invalid_unitCost = df_cast.filter(col("UnitCost_cast") < 0)
print("Invalid UnitCost:", invalid_unitCost.count())

# UnitPrice
invalid_UnitPrice = df_cast.filter(col("UnitPrice_cast") < 0)
print("Invalid UnitPrice:", invalid_UnitPrice.count())

# DiscountRate
invalid_DiscountRate = df_cast.filter(
    (col("DiscountRate_cast") < 0) |
    (col("DiscountRate_cast") > 1)
)
print("Invalid DiscountRate:", invalid_DiscountRate.count())

# IsReturned
invalid_IsReturned = df_cast.filter(
    ~col("IsReturned_cast").isin(0, 1)
)
print("Invalid IsReturned:", invalid_IsReturned.count())


# Count each row with an invalid range only once
invalid_range_records = df_cast.filter(
    (col("Quantity_cast") <= 0) |
    (col("UnitCost_cast") < 0) |
    (col("UnitPrice_cast") < 0) |
    (col("DiscountRate_cast") < 0) |
    (col("DiscountRate_cast") > 1) |
    (~col("IsReturned_cast").isin(0, 1))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cast validation

invalid_cast_condition = None


for name in ["Quantity", "UnitCost", "UnitPrice",
             "DiscountRate", "IsReturned"]:

    condition = (
    col(name).isNull() |
    (trim(col(name)) == "") |
    col(f"{name}_cast").isNull()
    )


    invalid = df_cast.filter(condition)

    print("Invalid cast in:", name, ":", invalid.count())

    if invalid_cast_condition is None:
        invalid_cast_condition = condition
    else:
        invalid_cast_condition = invalid_cast_condition | condition


# Count each invalid row only once
invalid_cast_records = df_cast.filter(invalid_cast_condition)

print("Total invalid cast records:", invalid_cast_records.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Total invalid range records for each row

invalid_range_records = df_cast.filter(
    (col("Quantity") <= 0) |
    (col("UnitCost") < 0) |
    (col("UnitPrice") < 0) |
    ((col("DiscountRate") < 0) | (col("DiscountRate") > 1)) |
    ((col("IsReturned") != 0) & (col("IsReturned") != 1))
)

print("",invalid_orders.count())
print("",invalid_details.count())
print("",invalid_cast_records.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Duplicate Validation: Orders

duplicate_orders = (df_orders.groupBy("OrderID").count().filter(col("count") > 1))

print("Orders duplicate records:", duplicate_orders.agg({"count": "sum"}).first()[0] or 0)

duplicate_orders.show()


# Duplicate Validation: Order Details
duplicate_details = (df_details.groupBy("OrderID", "ProductID").count().filter(col("count") > 1))

print("Order details duplicate records:",duplicate_details.agg(
          {"count": "sum"}
      ).first()[0] or 0)

duplicate_details.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Mark duplicate records
orders_checked = df_orders.join(
    duplicate_orders.select("OrderID").withColumn("_dup", F.lit(True)),
    "OrderID",
    "left"
)

details_checked = df_cast.join(
    duplicate_details.select("OrderID", "ProductID")
                     .withColumn("_dup", F.lit(True)),
    ["OrderID", "ProductID"],
    "left"
)

# 2. Combine validation conditions for Orders
bad_orders = (
    F.col("OrderID").isNull() |
    (F.trim(F.col("OrderID")) == "") |
    F.col("CustomerID").isNull() |
    (F.trim(F.col("CustomerID")) == "") |
    F.col("_dup").isNotNull()
)

# 3. Combine validation conditions for Order Details
bad_details = (
    F.col("OrderID").isNull() |
    (F.trim(F.col("OrderID")) == "") |
    F.col("ProductID").isNull() |
    (F.trim(F.col("ProductID")) == "") |
    F.col("Quantity_cast").isNull() |
    (~F.trim(F.col("Quantity")).rlike(r"^[+]?[0-9]+$")) |
    (F.col("Quantity_cast") <= 0) |
    F.col("UnitCost_cast").isNull() |
    (F.col("UnitCost_cast") < 0) |
    F.col("UnitPrice_cast").isNull() |
    (F.col("UnitPrice_cast") < 0) |
    F.col("DiscountRate_cast").isNull() |
    (F.col("DiscountRate_cast") < 0) |
    (F.col("DiscountRate_cast") > 1) |
    F.col("IsReturned_cast").isNull() |
    (~F.trim(F.col("IsReturned")).isin("0", "1")) |
    F.col("_dup").isNotNull()
)

# 4. Count invalid records once per row
total_orders = orders_checked.count()
total_details = details_checked.count()

invalid_orders_total = orders_checked.filter(bad_orders).count()
invalid_details_total = details_checked.filter(bad_details).count()

# 5. Calculate quality scores
orders_quality = (
    (total_orders - invalid_orders_total) / total_orders * 100
    if total_orders > 0 else 0
)

details_quality = (
    (total_details - invalid_details_total) / total_details * 100
    if total_details > 0 else 0
)

print(f"Orders quality: {orders_quality:.2f}%")
print(f"Order Details quality: {details_quality:.2f}%")

# 6. Apply the 80% threshold
quality_passed = orders_quality >= 80 and details_quality >= 80

print("Quality result:", "PASS" if quality_passed else "FAIL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return validation result to the pipeline

if quality_passed:
    notebookutils.notebook.exit("True")
else:
    notebookutils.notebook.exit("False")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

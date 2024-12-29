# Databricks notebook source
customers_df = spark.read.table("salesdata_catalog.bronze.customers")
products_df = spark.read.table("salesdata_catalog.bronze.products")
orders_df = spark.read.table("salesdata_catalog.bronze.orders")
sales_df = spark.read.table("salesdata_catalog.bronze.sales")

# COMMAND ----------

from pyspark.sql.functions import current_date, date_format, col, to_date, lit, current_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, LongType

from delta.tables import *

spark.conf.set('set spark.sql.legacy.timeParserPolicy','LEGACY')

# COMMAND ----------

orders_df = orders_df.withColumnRenamed("OrderDate", "Date")


# COMMAND ----------

#udf function to check delta table existence
def istableExists(tableName):
    status = True
    try:
        spark.table(tableName)
    except Exception as e:
        if( "TABLE_OR_VIEW_NOT_FOUND" in str(e)):
            status = False
    return status 

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import to_date, current_timestamp, from_utc_timestamp, lit, col
from delta.tables import DeltaTable

# Prepare the transformed DataFrame for both insert and update
orders_silver_df = orders_df.withColumn("OrderDate", to_date("Date")) \
                            .withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')) \
                            .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')) \
                            .drop("Date")

orders_silver_insert_df = orders_df.withColumn("OrderDate", to_date("Date")) \
                                   .withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')) \
                                   .withColumn("UpdatedDatetime", lit(None).cast(StringType())) \
                                   .drop("Date")

# Check if the Delta table exists
if istableExists("`salesdata_catalog`.silver.orders"):
    print("Performing Merge")
    
    # Load the existing Delta table
    orders_silver_deltaTable = DeltaTable.forName(spark, "`salesdata_catalog`.silver.orders")
    
    # Perform the merge (upsert)
    orders_silver_deltaTable.alias('orders') \
        .merge(
            orders_silver_df.alias("updates"),
            'orders.OrderId = updates.OrderId'
        ) \
        .whenMatchedUpdate(
            condition=(
                (col("orders.CustomerId") != col("updates.CustomerId")) |
                (col("orders.OrderDate") != col("updates.OrderDate"))
            ),  # Only update when there's a change
            set={
                "OrderId": "updates.OrderId",
                "CustomerId": "updates.CustomerId",
                "OrderDate": "updates.OrderDate",
                "UpdatedDatetime": "updates.UpdatedDatetime"
            }
        ) \
        .whenNotMatchedInsert(values={
            "OrderId": "updates.OrderId",
            "CustomerId": "updates.CustomerId",
            "OrderDate": "updates.OrderDate",
            "InsertedDatetime": "updates.InsertedDatetime",
            "UpdatedDatetime": lit(None).cast(StringType())  # Initial null for new rows
        }) \
        .execute()
else:
    print("Creating table for the first time")
    
    # Create a new Delta table for the first time
    orders_silver_insert_df.write.format("delta").saveAsTable("`salesdata_catalog`.silver.orders")


# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import to_date, current_timestamp, from_utc_timestamp, lit
from delta.tables import DeltaTable

# Prepare data
customers_silver_df = customers_df.withColumn(
    "InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')
).withColumn(
    "UpdatedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')
)

customers_silver_insert_df = customers_df.withColumn(
    "InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')
).withColumn(
    "UpdatedDatetime", lit(None).cast(StringType())  # NULL for new inserts
)

# Check if the table exists
if istableExists("`salesdata_catalog`.silver.customers"):
    print("Performing Merge")
    
    # Load the existing Delta table
    customers_silver_deltaTable = DeltaTable.forName(spark, "`salesdata_catalog`.silver.customers")
    
    # Perform the Merge
    customers_silver_deltaTable.alias('customers') \
        .merge(
            customers_silver_df.alias("updates"),
            'customers.CustomerId = updates.CustomerId'
        ) \
        .whenMatchedUpdate(
            condition="""
                customers.Active != updates.Active OR
                customers.Address != updates.Address OR
                customers.Name != updates.Name OR
                customers.City != updates.City OR
                customers.Country != updates.Country OR
                customers.Email != updates.Email
            """,
            set={
                "CustomerId": "updates.CustomerId",
                "Active": "updates.Active",
                "Name": "updates.Name",
                "Address": "updates.Address",
                "City": "updates.City",
                "Country": "updates.Country",
                "Email": "updates.Email",
                "UpdatedDatetime": "updates.UpdatedDatetime"
            }
        ) \
        .whenNotMatchedInsert(
            values={
                "CustomerId": "updates.CustomerId",
                "Active": "updates.Active",
                "Name": "updates.Name",
                "Address": "updates.Address",
                "City": "updates.City",
                "Country": "updates.Country",
                "Email": "updates.Email",
                "InsertedDatetime": "updates.InsertedDatetime",
                 "UpdatedDatetime": lit(None).cast(StringType())

            }
        ) \
        .execute()
    
else:
    print("Creating table for the first time")
    customers_silver_insert_df.write.format("delta").saveAsTable(
        "`salesdata_catalog`.silver.customers"
    )

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, current_timestamp, lit
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# Prepare the DataFrame for transformation
products_silver_df = products_df.withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')) \
                                .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'))

products_silver_insert_df = products_df.withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')) \
                                       .withColumn("UpdatedDatetime", lit(None).cast(StringType()))

# Check if the table exists
if istableExists("`salesdata_catalog`.silver.products"):
    print("Performing Merge")
    
    # Load the existing Delta table
    products_silver_deltaTable = DeltaTable.forName(spark, "`salesdata_catalog`.silver.products")
    
    # Perform the merge (upsert) with schema evolution
    products_silver_deltaTable.alias('products') \
        .merge(
            products_silver_df.alias("updates"),
            'products.ProductId = updates.ProductId'
        ) \
        .whenMatchedUpdate(
            condition="""
                products.Name != updates.Name OR
                products.ManufacturedCountry != updates.ManufacturedCountry OR
                products.WeightGrams != updates.WeightGrams OR
                products.UnitPrice != updates.UnitPrice
                """,
            set={
            "ProductId": "updates.ProductId",
            "Name": "updates.Name",
            "ManufacturedCountry": "updates.ManufacturedCountry",
            "WeightGrams": "updates.WeightGrams",
            "UnitPrice": "updates.UnitPrice",
            "UpdatedDatetime": "updates.UpdatedDatetime"
        }) \
        .whenNotMatchedInsert(values={
            "ProductId": "updates.ProductId",
            "Name": "updates.Name",
            "ManufacturedCountry": "updates.ManufacturedCountry",
            "WeightGrams": "updates.WeightGrams",
            "UnitPrice": "updates.UnitPrice",
            "InsertedDatetime": "updates.InsertedDatetime",
             "UpdatedDatetime": lit(None).cast(StringType())
        }) \
        .execute()
else:
    print("Creating table for the first time")
    
    # Create a new Delta table for the first time with schema evolution
    products_silver_insert_df.write.format("delta").option("mergeSchema", "true").saveAsTable("`salesdata_catalog`.silver.products")

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit, col
from delta.tables import DeltaTable

# Prepare the DataFrame for transformation
sales_silver_df = sales_df.withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')) \
                          .withColumn("UpdatedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'))

sales_silver_insert_df = sales_df.withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')) \
                                 .withColumn("UpdatedDatetime", lit(None).cast(StringType()))

# Check if the table exists
if istableExists("`salesdata_catalog`.silver.sales"):
    print("Performing Merge")
    
    # Load the existing Delta table
    sales_silver_deltaTable = DeltaTable.forName(spark, "`salesdata_catalog`.silver.sales")
    
    # Perform the merge (upsert)
    sales_silver_deltaTable.alias('sales') \
        .merge(
            sales_silver_df.alias("updates"),
            'sales.SaleId = updates.SaleId'
        ) \
        .whenMatchedUpdate(
            condition=(
                (col("sales.OrderId") != col("updates.OrderId")) |
                (col("sales.ProductId") != col("updates.ProductId")) |
                (col("sales.Quantity") != col("updates.Quantity"))
            ),  # Only update when there’s a change in data
            set={
                "SaleId": "updates.SaleId",
                "OrderId": "updates.OrderId",
                "ProductId": "updates.ProductId",
                "Quantity": "updates.Quantity",
                "UpdatedDatetime": "updates.UpdatedDatetime"
            }
        ) \
        .whenNotMatchedInsert(
            values={
                "SaleId": "updates.SaleId",
                "OrderId": "updates.OrderId",
                "ProductId": "updates.ProductId",
                "Quantity": "updates.Quantity",
                "InsertedDatetime": "updates.InsertedDatetime",
                "UpdatedDatetime": lit(None).cast(StringType())  # Initial null for new rows
            }
        ) \
        .execute()
else:
    print("Creating table for the first time")
    
    # Create a new Delta table for the first time
    sales_silver_insert_df.write.format("delta").saveAsTable("`salesdata_catalog`.silver.sales")

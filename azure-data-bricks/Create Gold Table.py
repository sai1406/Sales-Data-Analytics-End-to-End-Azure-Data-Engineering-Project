# Databricks notebook source
products_df = spark.read.table('salesdata_catalog.silver.products')
orders_df = spark.read.table('salesdata_catalog.silver.orders')
sales_df = spark.read.table('salesdata_catalog.silver.sales')
customers_df = spark.read.table('salesdata_catalog.silver.customers')

# COMMAND ----------

#Finding the most sold product in each month and write it to gold table
from pyspark.sql.functions import sum, count, desc, rank, date_format, col, current_date, lit, from_utc_timestamp, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import Window

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

sales_at_month_df = products_df.join(sales_df, sales_df.ProductId == products_df.ProductId,"inner")\
                .join(orders_df, sales_df.OrderId == orders_df.OrderId, "inner")\
                .withColumn("Order_MonthYear", date_format("OrderDate","MMM-yyyy"))\
                .withColumn("Order_Month", date_format("OrderDate","MM"))\
                .withColumn("Order_Year", date_format("OrderDate","yyyy"))\
                .groupBy("Name","Order_MonthYear","Order_Month","Order_Year")\
                .agg(count("SaleId").alias("total_sales"))\
                .orderBy(desc("total_sales"))        

window = rank().over(Window.partitionBy(sales_at_month_df.Order_MonthYear).orderBy(desc(sales_at_month_df.total_sales)))

top_sold_product_month_df = sales_at_month_df.withColumn("rank",window)\
                                             .filter("rank == 1")\
                                             .select(col('Order_MonthYear'), col('Name'),col('total_sales'))\
                                             .withColumn("InsertedDatetime", from_utc_timestamp(current_timestamp(),'Asia/Kolkata'))\
                                             .orderBy(desc(col('Order_Year')), desc(col('Order_Month')))

top_sold_product_month_df.write.format("delta").mode("overwrite").option("mergeSchema","True").saveAsTable("`salesdata_catalog`.gold.top_sold_products")

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Ensure CustomerId is present in sales_df
# Assuming sales_df has a column that can be used to derive CustomerId, e.g., OrderId
# You need to join sales_df with another DataFrame that has CustomerId, e.g., orders_df

# Join sales_df with orders_df to get CustomerId
sales_with_order_df = sales_df.join(
    orders_df,
    "OrderId",
    "inner"
)

# Join sales_with_order_df with customers_df to get CustomerId and Name
sales_with_customer_df = sales_with_order_df.join(
    customers_df,
    "CustomerId",
    "inner"
).select(
    "CustomerId", customers_df["Name"], "ProductId", "Quantity"
)

# Join sales_with_customer_df with products_df to calculate SaleAmount (UnitPrice * Quantity)
sales_with_revenue_df = sales_with_customer_df.join(
    products_df,
    "ProductId",
    "inner"
).withColumn(
    "SaleAmount",
    col("UnitPrice") * col("Quantity")
)

# Aggregate total revenue per CustomerId and Name
clv_df = sales_with_revenue_df.groupBy("CustomerId", customers_df["Name"]).agg(
    sum("SaleAmount").alias("TotalRevenuePerCustomer")
).orderBy(
    col("CustomerId")
)

# Optional: Save this as a Delta table for visualization
clv_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("salesdata_catalog.gold.customer_lifetime_value")

# COMMAND ----------

# yearly revenue

from pyspark.sql.functions import year, col, sum

yearly_revenue_df = products_df.join(
    sales_df, sales_df.ProductId == products_df.ProductId, "inner"
).join(
    orders_df, sales_df.OrderId == orders_df.OrderId, "inner"
).withColumn(
    "Order_Year", year("OrderDate")
).withColumn(
    "SaleAmount", col("UnitPrice") * col("Quantity")
).groupBy(
    "Name", "Order_Year"
).agg(
    sum("SaleAmount").alias("total_revenue")
).orderBy(
    "Name", "Order_Year"
)

yearly_revenue_df.write.format("delta").mode("overwrite").option("mergeSchema", "True").saveAsTable("`salesdata_catalog`.gold.yearly_revenue")

# COMMAND ----------

#Product brought/shown interest by most number of customers

from pyspark.sql.functions import col, count, desc

product_customer_count_df = customers_df.alias("cust").join(
    orders_df.alias("ord"), col("cust.CustomerId") == col("ord.CustomerId"), "inner"
).join(
    sales_df.alias("sale"), col("ord.OrderId") == col("sale.OrderId"), "inner"
).join(
    products_df.alias("prod"), col("sale.ProductId") == col("prod.ProductId"), "inner"
).groupBy(
    col("prod.ProductId"), col("prod.Name")
).agg(
    count("cust.CustomerId").alias("customer_count")
).orderBy(
    desc("customer_count")
)

product_customer_count_df.write.format("delta").mode("overwrite").option("mergeSchema", "True").saveAsTable("`salesdata_catalog`.gold.product_customer_count")
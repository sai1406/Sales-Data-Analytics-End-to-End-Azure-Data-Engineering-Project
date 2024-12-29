# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS salesdata_catalog.bronze.Customers;
# MAGIC CREATE TABLE IF NOT EXISTS salesdata_catalog.bronze.Customers (
# MAGIC   CustomerId LONG,
# MAGIC   Active BOOLEAN,
# MAGIC   Name STRING,
# MAGIC   Address STRING,
# MAGIC   City STRING,
# MAGIC   Country STRING,
# MAGIC   Email STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 'abfss://bronze@salesdatalake14.dfs.core.windows.net/Customers',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC DROP TABLE IF EXISTS salesdata_catalog.bronze.Orders;
# MAGIC CREATE TABLE IF NOT EXISTS salesdata_catalog.bronze.Orders (
# MAGIC   OrderId LONG,
# MAGIC   CustomerId LONG,
# MAGIC   OrderDate STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 'abfss://bronze@salesdatalake14.dfs.core.windows.net/Orders',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC DROP TABLE IF EXISTS salesdata_catalog.bronze.Products;
# MAGIC CREATE TABLE IF NOT EXISTS salesdata_catalog.bronze.Products (
# MAGIC   ProductId LONG,
# MAGIC   Name STRING,
# MAGIC   ManufacturedCountry STRING,
# MAGIC   WeightGrams LONG,
# MAGIC   UnitPrice LONG
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 'abfss://bronze@salesdatalake14.dfs.core.windows.net/Products',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC DROP TABLE IF EXISTS salesdata_catalog.bronze.Sales;
# MAGIC CREATE TABLE IF NOT EXISTS salesdata_catalog.bronze.Sales (
# MAGIC  SaleId LONG,
# MAGIC   OrderId LONG,
# MAGIC   ProductId LONG,
# MAGIC   Quantity LONG
# MAGIC   )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 'abfss://bronze@salesdatalake14.dfs.core.windows.net/Sales',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

customers_df = spark.read.table("salesdata_catalog.bronze.customers")

# COMMAND ----------

customers_df.display()
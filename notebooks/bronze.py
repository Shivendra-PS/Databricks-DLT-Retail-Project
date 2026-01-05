from pyspark import pipelines as dp
from pyspark.sql.functions import *

def autoload(path):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(path)
        .withColumn("ingestion_ts", current_timestamp())
    )

@dp.table(name="bronze_customers")
def bronze_customers(): return autoload("/Volumes/retail/raw/customers")

@dp.table(name="bronze_products")
def bronze_products(): return autoload("/Volumes/retail/raw/products")

@dp.table(name="bronze_orders")
def bronze_orders(): return autoload("/Volumes/retail/raw/orders")

@dp.table(name="bronze_order_items")
def bronze_order_items(): return autoload("/Volumes/retail/raw/order_items")

@dp.table(name="bronze_payments")
def bronze_order_items(): return autoload("/Volumes/retail/raw/payments")

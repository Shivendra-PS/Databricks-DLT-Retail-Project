from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(name="gold.dim_customer")
def dim_customer(): return spark.readStream.table("silver.silver_customers")

@dp.table(name="gold.dim_product")
def dim_product(): return spark.readStream.table("silver.silver_products")

@dp.table(name="gold.fact_orders")
def fact_orders():
    return spark.readStream.table("silver.silver_orders").select("order_id","customer_id","order_date","status")

@dp.table(name="gold.fact_sales")
def fact_sales():
    return (
        spark.readStream.table("silver.silver_orders")
        .join(spark.readStream.table("silver.silver_order_items"), "order_id")
        .join(spark.readStream.table("gold.dim_product"), "product_id")
        .select(
            "order_id","customer_id","product_id","order_date",
            "quantity","price",
            (col("quantity")*col("price")).alias("sales_amount")
        )
    )

@dp.table(name="gold.fact_payments")
def fact_payments():
    return (
        spark.readStream.table("silver.silver_payments")
        .join(spark.readStream.table("silver.silver_orders"), "order_id")
        .select(
            "payment_id",
            "order_id",
            "customer_id",
            "amount",
            "silver_payments.status",
            "order_date"
        )
    )

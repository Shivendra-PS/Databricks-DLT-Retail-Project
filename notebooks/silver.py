from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.temporary_view(name="silver_customers_stage")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def customers_stage(): return spark.readStream.table("bronze_customers")

dp.create_streaming_table('silver.silver_customers')
dp.create_auto_cdc_flow(
    target="silver.silver_customers",
    source="silver_customers_stage",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)

@dp.temporary_view(name="silver_products_stage")
@dp.expect_or_drop("valid_product", "product_id IS NOT NULL")
def products_stage(): return spark.readStream.table("bronze_products")

dp.create_streaming_table('silver.silver_products')
dp.create_auto_cdc_flow(
    target="silver.silver_products",
    source="silver_products_stage",
    keys=["product_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)

@dp.temporary_view(name="silver_orders_stage")
@dp.expect_or_drop("valid_order", "order_id IS NOT NULL")
@dp.expect("valid_status", "status IN ('CREATED','SHIPPED','DELIVERED')")
def orders_stage(): return spark.readStream.table("bronze_orders")

dp.create_streaming_table('silver.silver_orders')
dp.create_auto_cdc_flow(
    target="silver.silver_orders",
    source="silver_orders_stage",
    keys=["order_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)

@dp.table(name="silver.silver_order_items")
@dp.expect_or_drop("valid_keys", "order_id IS NOT NULL AND product_id IS NOT NULL")
@dp.expect("positive_qty", "quantity > 0")
def order_items(): return spark.readStream.table("bronze_order_items")

@dp.table(name="silver.silver_payments")
@dp.expect_or_drop("valid_payment", "payment_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
@dp.expect("valid_status", "status IN ('SUCCESS','FAILED')")
def silver_payments(): return spark.readStream.table("bronze_payments")

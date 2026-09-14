from pathlib import Path
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr


BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"

CUSTOMERS_DIR = RAW_DIR / "customers"
PRODUCTS_DIR = RAW_DIR / "products"
ORDERS_DIR = RAW_DIR / "orders"


def main():
    spark = (
        SparkSession.builder
        .appName("GenerateEcommerceData")
        .master("local[2]")
        .getOrCreate()
    )

    # ---------------------------------------------------------
    # Configuration
    # ---------------------------------------------------------

    customer_count = 500_000
    product_count = 10_000

    # Start with 10 million orders.
    # We will check resulting size before scaling further.
    order_count = 10_000_000

    # ---------------------------------------------------------
    # Customers
    # ---------------------------------------------------------

    cities = [
        ("Delhi", "Delhi"),
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Bengaluru", "Karnataka"),
        ("Hyderabad", "Telangana"),
        ("Chennai", "Tamil Nadu"),
        ("Kolkata", "West Bengal"),
        ("Jaipur", "Rajasthan"),
        ("Gurugram", "Haryana"),
        ("Noida", "Uttar Pradesh"),
    ]

    city_df = spark.createDataFrame(
        cities,
        ["city", "state"]
    )

    customers = (
    spark.range(1, customer_count + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn(
        "customer_name",
        expr(
            "concat('Customer_', cast(customer_id as string))"
        )
    )
    .withColumn(
        "city",
        expr(
            """
            CASE
                WHEN customer_id % 10 = 0 THEN 'Delhi'
                WHEN customer_id % 10 = 1 THEN 'Mumbai'
                WHEN customer_id % 10 = 2 THEN 'Pune'
                WHEN customer_id % 10 = 3 THEN 'Bengaluru'
                WHEN customer_id % 10 = 4 THEN 'Hyderabad'
                WHEN customer_id % 10 = 5 THEN 'Chennai'
                WHEN customer_id % 10 = 6 THEN 'Kolkata'
                WHEN customer_id % 10 = 7 THEN 'Jaipur'
                WHEN customer_id % 10 = 8 THEN 'Gurugram'
                ELSE 'Noida'
            END
            """
        )
    )
    .withColumn(
        "state",
        expr(
            """
            CASE
                WHEN customer_id % 10 = 0 THEN 'Delhi'
                WHEN customer_id % 10 = 1 THEN 'Maharashtra'
                WHEN customer_id % 10 = 2 THEN 'Maharashtra'
                WHEN customer_id % 10 = 3 THEN 'Karnataka'
                WHEN customer_id % 10 = 4 THEN 'Telangana'
                WHEN customer_id % 10 = 5 THEN 'Tamil Nadu'
                WHEN customer_id % 10 = 6 THEN 'West Bengal'
                WHEN customer_id % 10 = 7 THEN 'Rajasthan'
                WHEN customer_id % 10 = 8 THEN 'Haryana'
                ELSE 'Uttar Pradesh'
            END
            """
        )
    )
    .withColumn(
        "customer_segment",
        expr(
            """
            CASE
                WHEN customer_id % 3 = 0 THEN 'Premium'
                WHEN customer_id % 3 = 1 THEN 'Standard'
                ELSE 'Basic'
            END
            """
        )
    )
)
    # ---------------------------------------------------------
    # Products
    # ---------------------------------------------------------

    products = (
        spark.range(1, product_count + 1)
        .withColumnRenamed("id", "product_id")
        .withColumn(
            "product_name",
            expr(
                "concat('Product_', cast(product_id as string))"
            )
        )
        .withColumn(
            "category",
            expr(
                """
                CASE
                    WHEN product_id % 5 = 0 THEN 'Electronics'
                    WHEN product_id % 5 = 1 THEN 'Furniture'
                    WHEN product_id % 5 = 2 THEN 'Fashion'
                    WHEN product_id % 5 = 3 THEN 'Grocery'
                    ELSE 'Sports'
                END
                """
            )
        )
        .withColumn(
            "unit_price",
            (rand(seed=42) * 4900 + 100).cast("double")
        )
    )

    # ---------------------------------------------------------
    # Orders
    # ---------------------------------------------------------

    orders = (
        spark.range(1, order_count + 1)
        .withColumnRenamed("id", "order_id")
        .withColumn(
            "customer_id",
            (rand(seed=1) * customer_count + 1)
            .cast("long")
        )
        .withColumn(
            "product_id",
            (rand(seed=2) * product_count + 1)
            .cast("long")
        )
        .withColumn(
            "quantity",
            (rand(seed=3) * 5 + 1)
            .cast("int")
        )
        .withColumn(
            "amount",
            (rand(seed=4) * 4900 + 100).cast("double")
        )
        .withColumn(
            "order_timestamp",
            expr(
                """
                timestampadd(
                    SECOND,
                    cast(rand(5) * 31536000 as int),
                    timestamp('2026-01-01 00:00:00')
                )
                """
            )
        )
        .withColumn(
            "payment_method",
            expr(
                """
                CASE
                    WHEN order_id % 4 = 0 THEN 'UPI'
                    WHEN order_id % 4 = 1 THEN 'CARD'
                    WHEN order_id % 4 = 2 THEN 'NET_BANKING'
                    ELSE 'COD'
                END
                """
            )
        )
        .withColumn(
            "status",
            expr(
                """
                CASE
                    WHEN order_id % 20 = 0 THEN 'CANCELLED'
                    WHEN order_id % 15 = 0 THEN 'RETURNED'
                    ELSE 'COMPLETED'
                END
                """
            )
        )
    )

    # ---------------------------------------------------------
    # Write raw data as partitioned Parquet
    # ---------------------------------------------------------

    CUSTOMERS_DIR.mkdir(parents=True, exist_ok=True)
    PRODUCTS_DIR.mkdir(parents=True, exist_ok=True)
    ORDERS_DIR.mkdir(parents=True, exist_ok=True)

    customers.write.mode("overwrite").parquet(str(CUSTOMERS_DIR))
    products.write.mode("overwrite").parquet(str(PRODUCTS_DIR))

    (
        orders
        .withColumn("order_date", expr("to_date(order_timestamp)"))
        .write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(str(ORDERS_DIR))
    )

    # ---------------------------------------------------------
    # Basic validation
    # ---------------------------------------------------------

    print("Customers:", customers.count())
    print("Products:", products.count())
    print("Orders:", orders.count())

    print(
        "Order partitions:",
        orders.rdd.getNumPartitions()
    )

    print("Data generation complete.")

    spark.stop()


if __name__ == "__main__":
    main()
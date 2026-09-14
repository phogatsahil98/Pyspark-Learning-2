from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    row_number,
    sum,
)
from pyspark.sql.window import Window


BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "output"


def main():
    spark = (
        SparkSession.builder
        .appName("EcommercePipelineV1")
        .master("local[2]")
        .getOrCreate()
    )

    print("=" * 70)
    print("SPARK VERSION:", spark.version)
    print("APPLICATION ID:", spark.sparkContext.applicationId)
    print("=" * 70)

    # ---------------------------------------------------------
    # 1. READ RAW DATA
    # ---------------------------------------------------------

    orders = spark.read.parquet(
        str(RAW_DIR / "orders")
    )

    customers = spark.read.parquet(
        str(RAW_DIR / "customers")
    )

    products = spark.read.parquet(
        str(RAW_DIR / "products")
    )

    print("\n===== INPUT INFORMATION =====")

    print("Orders rows:", orders.count())
    print("Orders partitions:", orders.rdd.getNumPartitions())

    print("Customers rows:", customers.count())
    print("Customers partitions:", customers.rdd.getNumPartitions())

    print("Products rows:", products.count())
    print("Products partitions:", products.rdd.getNumPartitions())

    print("\n===== ORDER SCHEMA =====")
    orders.printSchema()

    # ---------------------------------------------------------
    # 2. DATA QUALITY
    # ---------------------------------------------------------

    invalid_orders = orders.filter(
        col("order_id").isNull()
        | col("customer_id").isNull()
        | col("product_id").isNull()
        | col("amount").isNull()
        | (col("amount") < 0)
    )

    invalid_count = invalid_orders.count()

    print("\nInvalid orders:", invalid_count)

    valid_orders = orders.filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") >= 0)
    )

    # ---------------------------------------------------------
    # 3. ADD PROCESSING METADATA
    # ---------------------------------------------------------

    valid_orders = valid_orders.withColumn(
        "processed_at",
        current_timestamp()
    )

    # ---------------------------------------------------------
    # 4. JOIN CUSTOMER DIMENSION
    # ---------------------------------------------------------

    enriched_orders = valid_orders.join(
        customers,
        "customer_id",
        "left"
    )

    # ---------------------------------------------------------
    # 5. JOIN PRODUCT DIMENSION
    # ---------------------------------------------------------

    enriched_orders = enriched_orders.join(
        products,
        "product_id",
        "left"
    )

    # ---------------------------------------------------------
    # 6. CITY METRICS
    # ---------------------------------------------------------

    city_metrics = (
        enriched_orders
        .groupBy("city")
        .agg(
            sum("amount").alias("total_revenue"),
            count("*").alias("order_count"),
            countDistinct("customer_id")
                .alias("unique_customers"),
            avg("amount")
                .alias("avg_order_value")
        )
    )

    # ---------------------------------------------------------
    # 7. PRODUCT METRICS
    # ---------------------------------------------------------

    product_metrics = (
        enriched_orders
        .groupBy(
            "city",
            "product_id",
            "product_name",
            "category"
        )
        .agg(
            sum("amount").alias("revenue"),
            sum("quantity").alias("units")
        )
    )

    # ---------------------------------------------------------
    # 8. WINDOW RANKING
    # ---------------------------------------------------------

    ranking_window = (
        Window
        .partitionBy("city")
        .orderBy(col("revenue").desc())
    )

    ranked_products = (
        product_metrics
        .withColumn(
            "rank",
            row_number().over(ranking_window)
        )
        .filter(col("rank") <= 10)
    )

    # ---------------------------------------------------------
    # 9. WRITE RESULTS
    # ---------------------------------------------------------

    city_metrics.write \
        .mode("overwrite") \
        .parquet(
            str(OUTPUT_DIR / "city_metrics")
        )

    ranked_products.write \
        .mode("overwrite") \
        .parquet(
            str(OUTPUT_DIR / "product_metrics")
        )

    # ---------------------------------------------------------
    # 10. OUTPUT VALIDATION
    # ---------------------------------------------------------

    print("\n===== OUTPUT =====")

    print(
        "City metric rows:",
        city_metrics.count()
    )

    print(
        "Ranked product rows:",
        ranked_products.count()
    )

    print("\n===== CITY METRICS =====")
    city_metrics.show(20, truncate=False)

    print("\n===== PRODUCT METRICS =====")
    ranked_products.show(20, truncate=False)

    # ---------------------------------------------------------
    # 11. EXPLAIN PLANS
    # ---------------------------------------------------------

    print("\n===== CITY METRICS PLAN =====")
    city_metrics.explain(mode="formatted")

    print("\n===== PRODUCT METRICS PLAN =====")
    ranked_products.explain(mode="formatted")

    input("\nPress Enter to stop Spark...")

    spark.stop()


if __name__ == "__main__":
    main()
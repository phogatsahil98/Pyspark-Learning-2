from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    input_file_name,
    lit,
    row_number,
    sum,
    when,
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

import logging


# ============================================================
# 1. PROJECT PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

ORDERS_RAW_DIR = RAW_DIR / "orders"
CUSTOMERS_RAW_DIR = RAW_DIR / "customers"
PRODUCTS_RAW_DIR = RAW_DIR / "products"

ORDERS_PROCESSED_DIR = PROCESSED_DIR / "orders"
ORDERS_QUARANTINE_DIR = QUARANTINE_DIR / "orders"

CITY_METRICS_DIR = OUTPUT_DIR / "city_metrics"
PRODUCT_METRICS_DIR = OUTPUT_DIR / "product_metrics"


# ============================================================
# 2. CREATE REQUIRED DIRECTORIES
# ============================================================

for directory in [
    PROCESSED_DIR,
    QUARANTINE_DIR,
    OUTPUT_DIR,
    LOG_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)


# ============================================================
# 3. LOGGING SETUP
# ============================================================

run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

log_file = LOG_DIR / f"pipeline_run_{run_timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("EcommercePipelineV2")


# ============================================================
# 4. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommercePipelineOptimizedV2")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

logger.info("============================================================")
logger.info("E-COMMERCE PIPELINE V2 STARTED")
logger.info("============================================================")

logger.info(f"Spark Version: {spark.version}")
logger.info(f"Application ID: {spark.sparkContext.applicationId}")
logger.info(f"Log File: {log_file}")

logger.info(f"Raw Directory: {RAW_DIR}")
logger.info(f"Processed Directory: {PROCESSED_DIR}")
logger.info(f"Quarantine Directory: {QUARANTINE_DIR}")
logger.info(f"Output Directory: {OUTPUT_DIR}")


# ============================================================
# 5. READ RAW DATA
# ============================================================

logger.info("Reading raw orders...")

orders = (
    spark.read
    .parquet(str(ORDERS_RAW_DIR))
)

logger.info("Reading raw customers...")

customers = (
    spark.read
    .parquet(str(CUSTOMERS_RAW_DIR))
)

logger.info("Reading raw products...")

products = (
    spark.read
    .parquet(str(PRODUCTS_RAW_DIR))
)


# ============================================================
# 6. BASIC INPUT INFORMATION
# ============================================================

logger.info("------------------------------------------------------------")
logger.info("INPUT INFORMATION")
logger.info("------------------------------------------------------------")

orders_partition_count = orders.rdd.getNumPartitions()
customers_partition_count = customers.rdd.getNumPartitions()
products_partition_count = products.rdd.getNumPartitions()

logger.info(f"Orders partitions: {orders_partition_count}")
logger.info(f"Customers partitions: {customers_partition_count}")
logger.info(f"Products partitions: {products_partition_count}")


orders.printSchema()


# ============================================================
# 7. COLUMN PRUNING
#
# We only keep columns actually required downstream.
# ============================================================

orders_required = orders.select(
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "amount",
    "order_timestamp",
    "payment_method",
    "status",
    "order_date",
)

customers_required = customers.select(
    "customer_id",
    "customer_name",
    "city",
    "state",
    "customer_segment",
)

products_required = products.select(
    "product_id",
    "product_name",
    "category",
    "unit_price",
)


# ============================================================
# 8. VALIDATION / QUARANTINE
# ============================================================

logger.info("Validating orders...")

invalid_condition = (
    col("order_id").isNull()
    | col("customer_id").isNull()
    | col("product_id").isNull()
    | col("amount").isNull()
    | (col("amount") < 0)
)

invalid_orders = (
    orders_required
    .filter(invalid_condition)
    .withColumn(
        "quarantine_reason",
        concat_ws(
            "; ",
            when(col("order_id").isNull(), lit("NULL_ORDER_ID")),
            when(col("customer_id").isNull(), lit("NULL_CUSTOMER_ID")),
            when(col("product_id").isNull(), lit("NULL_PRODUCT_ID")),
            when(col("amount").isNull(), lit("NULL_AMOUNT")),
            when(col("amount") < 0, lit("NEGATIVE_AMOUNT")),
        ),
    )
    .withColumn("quarantined_at", current_timestamp())
    .withColumn("source_file", input_file_name())
)

valid_orders = (
    orders_required
    .filter(~invalid_condition)
    .withColumn("processed_at", current_timestamp())
    .withColumn("source_file", input_file_name())
)


# ============================================================
# 9. COUNT VALID / INVALID DATA
# ============================================================

invalid_count = invalid_orders.count()
valid_count = valid_orders.count()

logger.info(f"Invalid orders: {invalid_count}")
logger.info(f"Valid orders: {valid_count}")

total_orders = valid_count + invalid_count

logger.info(f"Total evaluated orders: {total_orders}")

if total_orders > 0:
    invalid_percentage = (invalid_count / total_orders) * 100
else:
    invalid_percentage = 0.0

logger.info(f"Invalid percentage: {invalid_percentage:.4f}%")


# ============================================================
# 10. WRITE QUARANTINE DATA
#
# If invalid records exist, store them separately.
# Otherwise leave quarantine empty.
# ============================================================

if invalid_count > 0:

    logger.info("Writing invalid orders to quarantine...")

    (
        invalid_orders.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(str(ORDERS_QUARANTINE_DIR))
    )

    logger.info(
        f"Quarantine write complete: {ORDERS_QUARANTINE_DIR}"
    )

else:

    logger.info(
        "No invalid orders found. Quarantine remains empty."
    )


# ============================================================
# 11. WRITE PROCESSED DATA
#
# This contains only validated orders.
# ============================================================

logger.info("Writing validated orders to processed/...")

(
    valid_orders.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet(str(ORDERS_PROCESSED_DIR))
)

logger.info(
    f"Processed orders written to: {ORDERS_PROCESSED_DIR}"
)


# ============================================================
# 12. PREPARE DATA FOR ENRICHMENT
#
# IMPORTANT:
# We explicitly broadcast the smaller dimension tables.
#
# Products = 10,000 rows
# Customers = 500,000 rows
#
# We verify the physical plans later.
# ============================================================

logger.info("Preparing dimension tables for broadcast joins...")

broadcast_customers = broadcast(customers_required)

broadcast_products = broadcast(products_required)


# ============================================================
# 13. ENRICH ORDERS
# ============================================================

logger.info("Joining orders with customers...")

enriched_orders = (
    valid_orders
    .join(
        broadcast_customers,
        on="customer_id",
        how="left",
    )
)

logger.info("Joining orders with products...")

enriched_orders = (
    enriched_orders
    .join(
        broadcast_products,
        on="product_id",
        how="left",
    )
)


# ============================================================
# 14. SELECT ONLY COLUMNS REQUIRED BY DOWNSTREAM LOGIC
#
# This makes the persisted dataset significantly slimmer.
# ============================================================

enriched_orders = enriched_orders.select(
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "amount",
    "city",
    "product_name",
    "category",
)


# ============================================================
# 15. PERSIST ENRICHED DATA
#
# Why?
#
# enriched_orders is reused by:
#
#     1. city_metrics
#     2. product_metrics
#
# Without persistence Spark may recompute the join lineage.
#
# MEMORY_AND_DISK is safer than MEMORY_ONLY for a large dataset.
# ============================================================

logger.info(
    "Persisting enriched dataset using MEMORY_AND_DISK..."
)

enriched_orders = enriched_orders.persist(
    StorageLevel.MEMORY_AND_DISK
)


# ============================================================
# 16. MATERIALIZE CACHE
#
# persist() itself is lazy.
# count() forces Spark to materialize the persisted dataset.
# ============================================================

logger.info("Materializing persisted enriched dataset...")

enriched_count = enriched_orders.count()

logger.info(
    f"Enriched orders successfully materialized: {enriched_count}"
)


# ============================================================
# 17. CITY-LEVEL METRICS
# ============================================================

logger.info("Building city metrics...")

city_metrics = (
    enriched_orders
    .groupBy("city")
    .agg(
        sum("amount").alias("total_revenue"),
        count("*").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("amount").alias("avg_order_value"),
    )
)


# ============================================================
# 18. PRODUCT-LEVEL METRICS
# ============================================================

logger.info("Building product metrics...")

product_metrics = (
    enriched_orders
    .groupBy(
        "city",
        "product_id",
        "product_name",
        "category",
    )
    .agg(
        sum("amount").alias("revenue"),
        sum("quantity").alias("units"),
    )
)


# ============================================================
# 19. TOP 10 PRODUCTS PER CITY
# ============================================================

logger.info("Applying window ranking...")

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


# ============================================================
# 20. CACHE/PERSISTATION VALIDATION
# ============================================================

logger.info("------------------------------------------------------------")
logger.info("PHYSICAL PLAN: CITY METRICS")
logger.info("------------------------------------------------------------")

city_metrics.explain(mode="formatted")


logger.info("------------------------------------------------------------")
logger.info("PHYSICAL PLAN: RANKED PRODUCTS")
logger.info("------------------------------------------------------------")

ranked_products.explain(mode="formatted")


# ============================================================
# 21. WRITE GOLD OUTPUTS
# ============================================================

logger.info("Writing city metrics...")

(
    city_metrics.write
    .mode("overwrite")
    .parquet(str(CITY_METRICS_DIR))
)

logger.info(
    f"City metrics written to: {CITY_METRICS_DIR}"
)


logger.info("Writing ranked product metrics...")

(
    ranked_products.write
    .mode("overwrite")
    .parquet(str(PRODUCT_METRICS_DIR))
)

logger.info(
    f"Product metrics written to: {PRODUCT_METRICS_DIR}"
)


# ============================================================
# 22. OUTPUT VALIDATION
# ============================================================

city_metric_count = city_metrics.count()
ranked_product_count = ranked_products.count()

logger.info("------------------------------------------------------------")
logger.info("OUTPUT VALIDATION")
logger.info("------------------------------------------------------------")

logger.info(
    f"City metric rows: {city_metric_count}"
)

logger.info(
    f"Ranked product rows: {ranked_product_count}"
)


# ============================================================
# 23. DISPLAY SAMPLE RESULTS
# ============================================================

logger.info("Sample city metrics:")

city_metrics.orderBy(
    col("total_revenue").desc()
).show(20, truncate=False)


logger.info("Sample top products:")

ranked_products.orderBy(
    "city",
    "rank"
).show(50, truncate=False)


# ============================================================
# 24. FINAL VALIDATION
# ============================================================

if enriched_count != valid_count:

    logger.warning(
        "WARNING: Enriched row count differs from valid order count."
    )

else:

    logger.info(
        "Validation passed: enriched row count matches valid order count."
    )


# ============================================================
# 25. RELEASE PERSISTED DATA
# ============================================================

enriched_orders.unpersist()

logger.info("Persisted enriched dataset released.")


# ============================================================
# 26. FINAL SUMMARY
# ============================================================

logger.info("============================================================")
logger.info("PIPELINE V2 COMPLETE")
logger.info("============================================================")

logger.info(f"Total raw orders evaluated: {total_orders}")
logger.info(f"Valid orders: {valid_count}")
logger.info(f"Invalid orders: {invalid_count}")
logger.info(f"Enriched orders: {enriched_count}")
logger.info(f"City metrics: {city_metric_count}")
logger.info(f"Ranked products: {ranked_product_count}")
logger.info(f"Log file: {log_file}")

logger.info("Processed directory:")
logger.info(str(ORDERS_PROCESSED_DIR))

logger.info("Quarantine directory:")
logger.info(str(ORDERS_QUARANTINE_DIR))

logger.info("Output directory:")
logger.info(str(OUTPUT_DIR))


# ============================================================
# 27. KEEP SPARK ALIVE FOR UI INSPECTION
# ============================================================

input(
    "\nPipeline complete. Inspect Spark UI and press Enter to stop Spark..."
)

spark.stop()

logger.info("Spark session stopped.")
logger.info("Pipeline process exited successfully.")
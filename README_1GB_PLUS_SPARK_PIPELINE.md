# 1GB+ E-Commerce PySpark Data Engineering Pipeline

A hands-on PySpark project designed to move from **Spark fundamentals** to a realistic, production-oriented batch pipeline and then into Delta Lake.

The project intentionally contains a **Version 1 baseline** and an **Optimized Version 2** so that performance improvements are based on measured Spark behavior rather than blindly applying optimizations.

---

## 1. Project Objective

Build and understand an end-to-end e-commerce data pipeline capable of processing a large synthetic dataset with PySpark while learning:

- Spark execution model
- DataFrame transformations and actions
- Partitioning and shuffles
- Joins and join strategies
- Aggregations
- Window functions
- Spark SQL / Catalyst physical plans
- Parquet I/O
- Data validation and quarantine
- Logging and audit information
- Reusable intermediate datasets
- Performance optimization
- Spark UI based performance diagnosis
- V1 vs V2 benchmarking
- Production-oriented pipeline structure
- Preparation for Delta Lake / Bronze-Silver-Gold architecture

---

# 2. Dataset Created

The synthetic e-commerce dataset currently contains:

| Dataset | Rows | Purpose |
|---|---:|---|
| Customers | 500,000 | Customer dimension |
| Products | 10,000 | Product dimension |
| Orders | 10,000,000 | Main fact table |

### Orders columns

```text
order_id
customer_id
product_id
quantity
amount
order_timestamp
payment_method
status
order_date
```

### Customers columns

```text
customer_id
customer_name
city
state
customer_segment
```

### Products columns

```text
product_id
product_name
category
unit_price
```

The orders dataset is physically partitioned by `order_date` when written to Parquet.

This allows date-based queries to benefit from **partition pruning** when a filter is applied to `order_date`.

---

# 3. Project Architecture

## Raw → Processed/Quarantine → Enrichment → Metrics

```text
                         RAW DATA
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
      customers          products           orders
          │                 │                 │
          └─────────────────┼─────────────────┘
                            │
                            ▼
                    Schema / Validation
                            │
                 ┌──────────┴──────────┐
                 │                     │
              VALID                  INVALID
                 │                     │
                 ▼                     ▼
          processed/orders      quarantine/orders
                 │
                 ▼
          Column Pruning
                 │
                 ▼
       Broadcast Dimension Joins
                 │
                 ▼
          Enriched Orders
                 │
              PERSIST
                 │
        ┌────────┴─────────┐
        │                  │
        ▼                  ▼
  City Metrics      Product Metrics
                           │
                           ▼
                    Window Ranking
                           │
                           ▼
                         OUTPUT
```

---

# 4. Directory Structure

Recommended project structure:

```text
pyspark-learning/
│
├── data/
│   ├── raw/
│   │   ├── customers/
│   │   ├── products/
│   │   └── orders/
│   │
│   ├── processed/
│   │   └── orders/
│   │
│   └── quarantine/
│       └── orders/
│
├── output/
│   ├── city_metrics/
│   └── product_metrics/
│
├── logs/
│   ├── pipeline_run_YYYYMMDD_HHMMSS.log
│   └── ...
│
└── scripts/
    ├── data_generator.py
    ├── pipeline_v1.py
    └── pipeline_optimized_V2.py
```

---

# 5. Environment Used

The project was tested with:

```text
OS: Windows
Python: 3.11.9
Java: 17.0.20.1 LTS
PySpark: 3.5.9
Py4J: 0.10.9.9
Spark master for local learning: local[2]
```

Python 3.11 is intentionally used for this Spark environment instead of the system Python 3.14 installation.

---

# 6. How to Run the Project

Open PowerShell in:

```powershell
cd "C:\Data Engineer\pyspark-learning"
```

Activate the virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Verify Python:

```powershell
python --version
```

Verify PySpark:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

Verify Java:

```powershell
java --version
```

The project uses Java 17.

---

# 7. Data Generation

The generator creates the large synthetic dataset.

Run:

```powershell
python .\scripts\data_generator.py
```

Expected high-level result:

```text
Customers: 500000
Products: 10000
Orders: 10000000
```

The raw datasets are written under:

```text
 data/raw/
```

The orders data is written with `order_date` as the physical Parquet partition column.

---

# 8. Spark Fundamentals Already Practiced

Before building the large pipeline, the project covered the core Spark execution model.

## Spark application components

```text
Driver
  │
  ├── creates execution plan
  ├── coordinates jobs/stages/tasks
  └── communicates with executors

Executors
  │
  └── execute tasks and hold/cache data
```

Important terms:

- **Driver** — coordinator / control process.
- **Executor** — executes tasks.
- **Task** — computation against one partition for a stage.
- **Stage** — group of tasks separated by shuffle boundaries.
- **Job** — computation triggered by an action.
- **Partition** — chunk of distributed data.
- **DAG** — dependency graph of computation.
- **Transformation** — lazy operation that constructs computation.
- **Action** — requests a result and triggers execution.
- **Shuffle** — redistribution of records across partitions.

---

# 9. Lazy Evaluation and Execution Planning

A DataFrame transformation does not immediately execute the entire computation.

Example:

```python
df2 = df.filter(col("amount") > 100)
df3 = df2.select("order_id", "amount")
```

Nothing equivalent to a full result computation is required merely because the transformations were declared.

An action such as:

```python
df3.show()
```

causes Spark to execute the required work.

Conceptually:

```text
DataFrame code
      ↓
Logical Plan
      ↓
Catalyst Optimization
      ↓
Optimized Logical Plan
      ↓
Physical Plan
      ↓
Jobs / Stages / Tasks
      ↓
Executors
```

`df.explain()` is therefore a key debugging and learning tool.

---

# 10. Important Spark API Concepts Covered

## DataFrame inspection

```python
df.columns
df.dtypes
df.schema
df.printSchema()
df.show()
df.count()
df.first()
df.head()
df.take(5)
df.collect()
df.rdd.getNumPartitions()
df.explain()
```

### Important warning

`collect()` brings all results to the Driver and can cause Driver-side memory problems for large datasets.

---

## Selection and expressions

```python
from pyspark.sql.functions import col


df.select("name", "salary")


df.select(
    col("salary"),
    (col("salary") * 12).alias("annual_salary")
)
```

Also covered:

- `selectExpr()`
- `alias()`
- `lit()`
- Column expressions

---

## Filtering

```python
df.filter(col("amount") > 100)
df.where(col("amount") > 100)
```

Spark Column boolean expressions use:

```python
&   # AND
|   # OR
~   # NOT
```

Do not replace them with Python `and`, `or`, and `not` for Spark Column expressions.

---

## Column modification

Covered:

```python
withColumn()
when()
otherwise()
drop()
withColumnRenamed()
```

`withColumn()` is lazy and creates or replaces a column expression in the DataFrame plan.

---

## Distinct / deduplication

```python
df.distinct()
df.dropDuplicates()
```

These operations may require a shuffle depending on the execution plan.

---

# 11. Aggregations

Covered:

```python
groupBy()
agg()
sum()
avg()
min()
max()
count()
countDistinct()
```

Example:

```python
df.groupBy("city").agg(
    sum("amount").alias("total_revenue"),
    count("*").alias("order_count")
)
```

Important concept:

```text
groupBy() → defines groups
agg()     → defines calculations
```

Spark may perform local/partial aggregation before the shuffle and a final aggregation afterward.

A physical plan may show operators such as:

```text
HashAggregate
Exchange
HashAggregate
```

---

# 12. Joins

Covered:

- inner join
- left join
- right join
- full join
- left semi join
- left anti join
- aliases
- explicit join conditions
- broadcast joins
- sort-merge joins

Example:

```python
o = orders.alias("o")
c = customers.alias("c")

joined = o.join(
    c,
    col("o.customer_id") == col("c.customer_id"),
    "left"
)
```

### Left semi

Returns rows from the left side where a matching row exists on the right.

### Left anti

Returns rows from the left side where no matching row exists on the right.

### Broadcast join

```python
from pyspark.sql.functions import broadcast

joined = large_df.join(
    broadcast(small_df),
    "key",
    "left"
)
```

Broadcast can avoid shuffling the large side, but it consumes executor memory. A table being small in row count does not by itself prove that broadcasting is safe; physical size and execution behavior matter.

The physical plan should be checked for the resulting join strategy.

---

# 13. Window Functions

Covered:

```python
Window.partitionBy()
Window.orderBy()
Window.rowsBetween()
```

Functions covered:

```text
row_number
rank
dense_rank
lag
lead
first_value
last_value
```

Patterns practiced:

- ranking within groups
- top-N per group
- latest record per customer
- running totals
- moving calculations

### Rank difference

```text
row_number() → unique sequence
rank()       → ties + gaps
 dense_rank() → ties + no gaps
```

### Important distinction

`Window.partitionBy()` is a logical grouping boundary for a window calculation.

It is **not the same thing** as Spark's physical partitioning of data and not the same as Parquet `write.partitionBy()`.

---

# 14. Complex and Real-World Data Types

Covered Spark functions and concepts for:

### Strings

```text
lower
upper
trim
ltrim
rtrim
regexp_replace
split
```

### Arrays

```text
size
array_contains
array_distinct
array_union
explode
```

### Maps

```python
col("attributes")["key"]
```

### Structs

```python
col("customer.address.city")
```

### Dates / timestamps

```text
to_date
to_timestamp
date_format
year
month
dayofmonth
datediff
date_add
date_sub
current_date
current_timestamp
```

### NULL handling

```text
isNull
isNotNull
fillna
dropna
coalesce
```

NULL is not equivalent to:

```text
0
""
False
```

Arithmetic involving NULL often results in NULL.

---

# 15. Schema and Data Types

Covered:

```text
StringType
IntegerType
LongType
DoubleType
FloatType
BooleanType
DateType
TimestampType
ArrayType
MapType
StructType
StructField
```

Explicit schemas are preferred when data quality and reproducibility matter.

Example:

```python
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
])
```

Also covered:

```python
df.withColumn("amount", col("amount").cast("double"))
```

---

# 16. Parquet and File I/O

Parquet is the primary raw/output format in this project.

Why Parquet is useful for analytics:

- columnar storage
- compression
- typed schema
- column pruning
- predicate pushdown support
- efficient analytical scans

Write modes practiced:

```text
overwrite
append
ignore
error / errorifexists
```

A Parquet write typically creates a directory containing one or more `part-*` files rather than one monolithic file.

---

# 17. Three Different Meanings of Partitioning

This project specifically emphasized the difference between:

```text
1. Spark physical partitions
2. Window.partitionBy()
3. DataFrameWriter.partitionBy()
```

### 1. Spark physical partitions

Chunks of distributed data processed by tasks.

### 2. Window partitionBy

Defines the logical group over which a window function operates.

### 3. write.partitionBy

Defines the directory layout of partitioned files.

Example:

```text
data/raw/orders/
    order_date=2026-01-01/
    order_date=2026-01-02/
    order_date=2026-01-03/
```

Filtering on `order_date` can allow Spark to skip irrelevant directory partitions.

---

# 18. Spark SQL

Covered:

```python
createOrReplaceTempView()
createOrReplaceGlobalTempView()
spark.sql()
```

SQL features practiced:

- SELECT
- WHERE
- GROUP BY
- HAVING
- ORDER BY
- DISTINCT
- LIMIT
- CASE
- CTEs
- subqueries
- JOIN
- window functions
- UNION
- UNION ALL

Conceptual SQL processing order:

```text
FROM
 ↓
WHERE
 ↓
GROUP BY
 ↓
HAVING
 ↓
SELECT
 ↓
ORDER BY
 ↓
LIMIT
```

DataFrame API and Spark SQL ultimately use Spark's structured query engine and Catalyst optimizer.

---

# 19. Performance Concepts Practiced Before V2

Core optimization principle:

> **Move less data, process less data, repeat less work.**

Topics covered:

- partition sizing
- too few vs too many partitions
- `repartition()`
- `coalesce()`
- `cache()`
- `persist()`
- broadcast joins
- data skew
- Adaptive Query Execution (AQE)
- predicate pushdown
- column pruning
- partition pruning
- small file problem
- avoiding blind `coalesce(1)`

Important APIs:

```python
df.repartition(n)
df.coalesce(n)
df.cache()
df.persist()
df.unpersist()
```

`repartition()` generally causes a shuffle.

`coalesce()` primarily reduces the number of partitions and can avoid a full shuffle.

`persist()` is lazy; an action is needed to materialize the persisted data.

---

# 20. Version 1 — Baseline Pipeline

File:

```text
scripts/pipeline_v1.py
```

V1 is intentionally **not highly optimized**.

Its job is to establish a measurable baseline before changing anything.

## V1 flow

```text
Read raw Parquet
      ↓
Validate orders
      ↓
Filter valid orders
      ↓
Join customers
      ↓
Join products
      ↓
City aggregation
      ↓
Product aggregation
      ↓
Window ranking
      ↓
Write Parquet outputs
```

### Why V1 exists

Without a baseline, optimization becomes guesswork.

We want to be able to answer:

- Did V2 become faster?
- Which stage improved?
- Did shuffle decrease?
- Did the join strategy change?
- Did recomputation decrease?
- Did the physical plan improve?
- Did output behavior change?

---

# 21. V1 Baseline Investigation

The V1 run was used as a diagnostic experiment, not merely as a successful pipeline execution.

We inspected:

```text
Spark UI
SQL tab
Jobs
Stages
Tasks
Shuffle Read
Shuffle Write
Physical plan
Exchange operators
Join operators
Aggregation operators
Window operators
```

### Evidence collected / discussed

#### Product table

The products dimension contains:

```text
10,000 rows
```

This makes it a strong candidate for broadcast, subject to verifying physical size and execution behavior.

#### Window output observation

One observed result was:

```text
25 rows
```

This should be interpreted as output cardinality, not as a direct measure of window cost.

#### Expense of a window operation

There is no universal formula such as:

```text
window cost = rows × constant
```

For:

```python
Window.partitionBy("city").orderBy(col("revenue").desc())
```

important runtime costs can include:

```text
shuffle / redistribution
sorting
memory pressure
spill to disk
uneven task durations due to skew
```

The practical evidence comes from Spark UI and the physical plan, particularly operators such as:

```text
Exchange
Sort
Window
```

---

# 22. Version 2 — Optimized Pipeline

File:

```text
scripts/pipeline_optimized_V2.py
```

V2 introduces production-oriented improvements that were selected from the baseline analysis.

## V2 flow

```text
Read raw Parquet
      ↓
Column pruning
      ↓
Validation
      ├──────────────► quarantine/orders
      │
      ▼
Valid orders
      │
      ├──────────────► processed/orders
      │
      ▼
Broadcast customer dimension
      +
Broadcast product dimension
      ↓
Enriched orders
      ↓
Persist MEMORY_AND_DISK
      │
      ├──────────────► City metrics
      │
      └──────────────► Product metrics
                              ↓
                         Window ranking
                              ↓
                            Outputs
```

---

# 23. V2 Optimization #1 — Early Column Pruning

Instead of carrying every column through every operation, V2 explicitly selects only required columns.

Example:

```python
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
```

The same is done for dimensions.

### Why?

Less data carried through joins and subsequent stages can reduce:

- memory usage
- serialization/deserialization work
- network transfer
- shuffle volume
- execution overhead

Principle:

> Do not carry columns downstream that you do not need.

---

# 24. V2 Optimization #2 — Validate Before Expensive Joins

V2 first separates invalid orders from valid orders.

Validation condition:

```python
invalid_condition = (
    col("order_id").isNull()
    | col("customer_id").isNull()
    | col("product_id").isNull()
    | col("amount").isNull()
    | (col("amount") < 0)
)
```

Invalid records receive:

```text
quarantine_reason
quarantined_at
source_file
```

Valid records receive:

```text
processed_at
source_file
```

### Why validate early?

Bad rows should not travel unnecessarily through expensive enrichment and aggregation steps.

---

# 25. V2 Optimization #3 — Quarantine Invalid Data

Invalid data is sent to:

```text
data/quarantine/orders/
```

The quarantine records include the reason for rejection.

Possible reasons include:

```text
NULL_ORDER_ID
NULL_CUSTOMER_ID
NULL_PRODUCT_ID
NULL_AMOUNT
NEGATIVE_AMOUNT
```

At the time of the current dataset run:

```text
Invalid orders = 0
```

That is a valid production outcome. We do **not** invent bad records merely to populate the quarantine directory.

Therefore an empty quarantine directory is expected when all input rows pass validation.

---

# 26. V2 Optimization #4 — Processed Data Layer

Validated orders are written to:

```text
data/processed/orders/
```

The flow is:

```text
raw
 │
 ├── invalid ──► quarantine
 │
 └── valid ────► processed
```

This creates a clean boundary between raw input and data that has passed the pipeline's validation rules.

---

# 27. V2 Optimization #5 — Broadcast Dimension Tables

The dimension tables are prepared as:

```python
broadcast_customers = broadcast(customers_required)
broadcast_products = broadcast(products_required)
```

Then used in joins.

```python
enriched_orders = valid_orders.join(
    broadcast_customers,
    on="customer_id",
    how="left",
)
```

and:

```python
enriched_orders = enriched_orders.join(
    broadcast_products,
    on="product_id",
    how="left",
)
```

### Why?

For a sufficiently small dimension, broadcasting can avoid redistributing the large fact dataset for the join.

### Important warning

Do not conclude:

```text
small row count = always safe to broadcast
```

Actual size matters, and executor memory matters.

The correct engineering approach is:

```text
hypothesis
   ↓
apply broadcast
   ↓
explain()
   ↓
verify join strategy
   ↓
measure runtime/shuffle
```

Look for an execution strategy such as:

```text
BroadcastHashJoin
```

rather than assuming the broadcast hint produced the intended plan.

---

# 28. V2 Optimization #6 — Slim Enriched Dataset

After the joins, V2 selects only columns required for the downstream outputs:

```python
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
```

This is another explicit example of:

> Move less data.

---

# 29. V2 Optimization #7 — Persist Shared Intermediate Data

The enriched dataset feeds two downstream branches:

```text
enriched_orders
      │
      ├── city_metrics
      │
      └── product_metrics
```

V2 therefore uses:

```python
enriched_orders = enriched_orders.persist(
    StorageLevel.MEMORY_AND_DISK
)
```

### Why MEMORY_AND_DISK?

The enriched dataset is large enough that keeping everything purely in memory is not guaranteed to be safe.

`MEMORY_AND_DISK` allows Spark to spill persisted partitions to disk when necessary.

### Important detail

`persist()` is lazy.

Therefore V2 executes:

```python
enriched_count = enriched_orders.count()
```

to materialize the persisted dataset.

Later:

```python
enriched_orders.unpersist()
```

releases the persistence resources.

### Why persist here?

Because the enriched lineage is reused by multiple downstream calculations.

The objective is:

> Do expensive work once and reuse the result when justified.

---

# 30. V2 Aggregations

## City metrics

```python
city_metrics = enriched_orders.groupBy("city").agg(
    sum("amount").alias("total_revenue"),
    count("*").alias("order_count"),
    countDistinct("customer_id").alias("unique_customers"),
    avg("amount").alias("avg_order_value"),
)
```

Output fields:

```text
city
total_revenue
order_count
unique_customers
avg_order_value
```

## Product metrics

```python
product_metrics = enriched_orders.groupBy(
    "city",
    "product_id",
    "product_name",
    "category",
).agg(
    sum("amount").alias("revenue"),
    sum("quantity").alias("units"),
)
```

---

# 31. V2 Window Ranking

Top 10 products per city are generated using:

```python
ranking_window = (
    Window
    .partitionBy("city")
    .orderBy(col("revenue").desc())
)
```

Then:

```python
ranked_products = (
    product_metrics
    .withColumn(
        "rank",
        row_number().over(ranking_window)
    )
    .filter(col("rank") <= 10)
)
```

### Why the order matters

The operation logically requires:

```text
Group by city for window calculation
       ↓
Order products within each city
       ↓
Assign row_number
       ↓
Keep top 10
```

Potential physical costs include shuffle and sorting.

---

# 32. V2 Logging

The pipeline creates a timestamped log file under:

```text
logs/
```

Example:

```text
pipeline_run_20260914_101500.log
```

Logs record important operational information such as:

- Spark version
- application ID
- input directories
- partition counts
- valid row count
- invalid row count
- invalid percentage
- enriched row count
- output row counts
- output paths
- validation results
- pipeline completion status

This makes the pipeline easier to debug and audit without relying solely on terminal output.

---

# 33. Audit Columns Added in V2

### Valid data

```text
processed_at
source_file
```

### Quarantined data

```text
quarantine_reason
quarantined_at
source_file
```

These columns help answer:

```text
When was the record processed?
Where did it come from?
Why was it rejected?
```

This is a foundation for production observability.

---

# 34. Idempotency Consideration

V2 intentionally uses overwrite for its deterministic batch output locations.

Examples:

```python
.write.mode("overwrite")
```

This is useful for the learning project because rerunning the same batch should not blindly append duplicate output records.

In a real production pipeline, idempotency often requires stronger batch identifiers, merge logic, checkpointing or transactional storage depending on architecture.

This project will address those patterns more deeply when Delta Lake is introduced.

---

# 35. Spark UI Investigation Checklist

When Spark is running and paused before `spark.stop()`, open:

```text
http://localhost:4040
```

Inspect the following.

## Jobs tab

Ask:

```text
How many jobs were created?
Which actions triggered them?
Which jobs take the most time?
```

## Stages tab

Ask:

```text
Which stages are expensive?
Are task durations balanced?
Is there significant shuffle?
Is there spill?
```

## SQL tab

Look for:

```text
Exchange
Sort
HashAggregate
BroadcastHashJoin
SortMergeJoin
Window
```

### Important clue

`Exchange` is a strong physical-plan indicator that Spark is redistributing data.

Do not assume that only `groupBy()` can cause a shuffle.

---

# 36. How to Think About Shuffle

Shuffle can involve:

```text
network transfer
serialization
CPU
memory
local disk
```

Wide operations commonly associated with shuffle include:

```text
groupBy
join
distinct
dropDuplicates
orderBy
repartition
```

The exact execution behavior should always be verified using the physical plan and Spark UI.

---

# 37. Important Performance Investigation Questions

Before changing configuration, ask:

### Data movement

```text
How much data is being shuffled?
Can I reduce it?
```

### Input volume

```text
Am I reading columns I do not need?
Am I reading dates I do not need?
```

### Joins

```text
Can a safe small dimension be broadcast?
Is Spark using the expected join strategy?
```

### Re-computation

```text
Is the same expensive lineage being executed multiple times?
Would persistence be justified?
```

### Partitioning

```text
Are there too few tasks?
Are there too many tiny tasks?
Are task sizes unbalanced?
```

### Window operations

```text
Is there a costly sort?
Is there a shuffle?
Is there skew?
Are tasks spilling?
```

---

# 38. V1 vs V2 Comparison Framework

Do not compare versions only by wall-clock time.

Use this matrix:

| Metric | V1 | V2 | What It Tells Us |
|---|---:|---:|---|
| Total runtime | TBD | TBD | Overall performance |
| Jobs | TBD | TBD | Execution structure |
| Stages | TBD | TBD | Pipeline complexity |
| Shuffle Read | TBD | TBD | Data movement into stages |
| Shuffle Write | TBD | TBD | Data movement produced |
| Join strategy | TBD | TBD | Broadcast vs shuffle join |
| Persistence reuse | No/limited | Yes | Re-computation reduction |
| Columns carried | Wider | Slimmer | Memory/data movement |
| Invalid-data handling | Basic | Quarantine | Data quality |
| Processed layer | No | Yes | Clean-data boundary |
| Logging | Basic | Timestamped logs | Observability |
| Output validation | Basic | Explicit | Reliability |

Replace the `TBD` entries after running the two versions.

---

# 39. V1 vs V2 — What Changed and Why

| Change | V1 | V2 | Why |
|---|---|---|---|
| Column pruning | Limited | Explicit | Reduce unnecessary data movement |
| Early validation | Basic | Explicit quarantine flow | Reject bad data before enrichment |
| Processed dataset | No | Yes | Create validated data boundary |
| Quarantine dataset | No dedicated flow | Yes | Preserve bad records with reason |
| Customer join | Normal join | Broadcast candidate | Reduce large-side redistribution when safe |
| Product join | Normal join | Broadcast candidate | Product dimension is only 10k rows |
| Enriched columns | Wider lineage | Slim explicit select | Reduce memory and processing |
| Shared enriched data | Recomputed risk | `persist(MEMORY_AND_DISK)` | Reuse expensive enrichment |
| Audit metadata | Limited | Added | Traceability |
| Logging | Limited | Timestamped log file | Operational observability |
| Validation | Basic | Explicit counts/checks | Data reliability |

---

# 40. Mistakes / Corrections Learned During the Project

These are important interview and engineering lessons.

### Mistake: Assuming `groupBy()` is the only source of shuffle

Correction:

> Shuffle should be reasoned from the physical execution plan, with `Exchange` being a strong clue.

---

### Mistake: Treating `partitionBy()` as one single concept

Correction:

```text
Spark physical partitioning
Window.partitionBy
write.partitionBy
```

are different concepts.

---

### Mistake: Assuming broadcast only from row count

Correction:

> Row count is evidence, not a guarantee. Actual serialized size and executor memory matter.

---

### Mistake: Treating the number of window output rows as window cost

Correction:

> Measure shuffle, sorting, task duration, spills, and the physical plan.

---

### Mistake: Optimizing without a baseline

Correction:

> V1 establishes the evidence; V2 tests a hypothesis.

---

### Mistake: Blindly changing Spark configuration

Correction:

> Measure first, then change one meaningful variable at a time.

---

# 41. Production Principles Practiced

The project intentionally applies these principles:

```text
1. Validate data early.
2. Preserve invalid records instead of silently dropping them.
3. Keep raw and processed data separate.
4. Carry only required columns.
5. Reduce unnecessary data movement.
6. Broadcast only when justified.
7. Persist only when reuse justifies it.
8. Verify physical plans instead of assuming behavior.
9. Measure performance before and after changes.
10. Add operational logging and audit metadata.
11. Design outputs so reruns are deterministic where appropriate.
12. Keep data quality and performance concerns visible separately.
```

---

# 42. Key Interview Questions From This Project

Be able to answer these without looking at notes.

### Spark architecture

- What is the role of the Driver?
- What does an Executor do?
- What is a Task?
- What is a Stage?
- What is a Job?
- What is a Partition?
- What causes stage boundaries?

### Execution

- What is lazy evaluation?
- What is the difference between transformation and action?
- What is Catalyst?
- What is a logical plan?
- What is an optimized logical plan?
- What is a physical plan?
- What does `Exchange` indicate?

### Performance

- What is shuffle?
- Why is shuffle expensive?
- What is a narrow transformation?
- What is a wide transformation?
- When should you broadcast a table?
- What are the risks of broadcast joins?
- When should you cache/persist?
- What is data skew?
- What does AQE solve?
- What is predicate pushdown?
- What is partition pruning?
- Why can too many partitions hurt performance?
- Why can too few partitions hurt performance?

### Data engineering

- Why quarantine bad records?
- What is idempotency?
- Why add `source_file`?
- Why add `processed_at`?
- How would you handle incremental data?
- How would you recover from a partially failed batch?
- How would you design a Bronze/Silver/Gold architecture?

---

# 43. Important Commands for Future Reference

## Run V1

```powershell
python .\scripts\pipeline_v1.py
```

## Run V2

```powershell
python .\scripts\pipeline_optimized_V2.py
```

## Spark UI

```text
http://localhost:4040
```

## Check PySpark version

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

## Check Java

```powershell
java --version
```

---

# 44. Current Project Status

## Completed

- [x] Java 17 environment configured
- [x] Python 3.11 virtual environment configured
- [x] PySpark 3.5.9 installed
- [x] Spark smoke test
- [x] Spark UI exploration
- [x] Spark architecture
- [x] DataFrame inspection
- [x] DataFrame selection / expressions
- [x] Filtering / `withColumn()` / conditionals
- [x] Aggregations
- [x] Joins
- [x] Window functions
- [x] String/date/array/struct/null handling
- [x] Schema and casting
- [x] Parquet I/O
- [x] Spark SQL
- [x] Performance optimization concepts
- [x] 1GB+ synthetic dataset generation
- [x] 500k customer dimension
- [x] 10k product dimension
- [x] 10M order fact table
- [x] Date partitioned order storage
- [x] V1 baseline pipeline
- [x] V2 optimized pipeline design
- [x] Logging design
- [x] Quarantine flow
- [x] Processed-data flow

## Current milestone

- [ ] Run V2 end-to-end
- [ ] Capture V1 vs V2 runtime
- [ ] Capture V1 vs V2 shuffle metrics
- [ ] Verify broadcast join operators
- [ ] Verify persistence reuse in Spark UI / SQL plan
- [ ] Compare task distribution
- [ ] Compare output file counts and sizes
- [ ] Complete V1 vs V2 performance post-mortem

## Next major milestone

After the V1 → V2 comparison:

```text
Delta Lake
   ↓
ACID
   ↓
Schema enforcement
   ↓
Schema evolution
   ↓
UPDATE
   ↓
DELETE
   ↓
MERGE / UPSERT
   ↓
Time Travel
   ↓
OPTIMIZE / VACUUM
   ↓
Bronze / Silver / Gold
```

---

# 45. Study Method Used in This Project

Every major experiment follows:

```text
WHY
 ↓
CODE
 ↓
WHAT THE CODE DOES
 ↓
EXPECTED BEHAVIOR
 ↓
RUN
 ↓
OBSERVE SPARK UI / PLAN
 ↓
EXPLAIN THE OBSERVATION
 ↓
WRITE DOWN THE LESSON
 ↓
INTERVIEW QUESTION
```

The objective is not to memorize PySpark syntax.

The objective is to be able to look at a pipeline and reason:

```text
What is the data doing?
Where is data moving?
Where can Spark avoid movement?
Where is data being recomputed?
Where can memory become a problem?
Where can skew occur?
Why did this stage become slow?
What evidence proves my optimization helped?
```

---

# 46. Golden Rule for This Project

> **Never optimize Spark because an optimization sounds good. Optimize because the data, physical plan, Spark UI, and measurements show a specific bottleneck.**

That is the difference between knowing PySpark syntax and thinking like a Data Engineer.

---

# 47. GitHub Commit Recommendation

After adding this README and the pipeline scripts, a useful commit structure is:

```powershell
git status

git add README.md scripts/ data/ logs/

git commit -m "Add 1GB+ PySpark pipeline V1 and V2 documentation"
```

Do not commit generated bulk data or runtime log files if the repository is intended to stay lightweight. Prefer `.gitignore` entries for large generated datasets and transient logs.

A typical `.gitignore` should include:

```gitignore
.venv/
__pycache__/
*.pyc

# Generated large datasets
data/raw/
data/processed/
data/quarantine/
data/delta/

# Generated pipeline outputs
output/

# Runtime logs
logs/

# Spark / temporary files
*.tmp
```

Keep the generator script and pipeline source code in GitHub; regenerate the synthetic data locally when needed.

---

# 48. Personal Revision Snapshot

When revisiting this project later, remember the complete story:

```text
We started by learning Spark's distributed execution model.

Then we generated a realistic e-commerce dataset:
500k customers + 10k products + 10M orders.

We built V1 intentionally as a baseline.

We used Spark UI and physical plans to identify:
- data movement
- join behavior
- aggregation behavior
- window cost indicators
- lineage reuse opportunities

Then V2 introduced:
- column pruning
- early validation
- quarantine
- processed data
- broadcast join candidates
- slimmer enriched data
- persistence for shared lineage
- audit columns
- logging
- output validation

Now we compare V1 vs V2 using evidence.

After that, we move to Delta Lake to solve the next level:
reliable transactional data management and incremental processing.
```

This README should evolve with the project. Add measured V1/V2 numbers after the benchmark rather than replacing the experimental history.

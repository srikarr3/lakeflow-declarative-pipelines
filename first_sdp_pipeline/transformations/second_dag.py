from pyspark import pipelines as dp 
from pyspark.sql.functions import col, to_date, sum

# ==============================================================================
# STREAMING MEDALLION PIPELINE DAG
# ==============================================================================
# This pipeline demonstrates an incremental/streaming ETL pipeline using 
# Spark Declarative Pipelines (SDP), Temporary Views, and Streaming Tables. 
#
# Unlike batch pipelines, streaming pipelines process data incrementally and 
# maintain state, allowing for low latency updates using `spark.readStream`.
# ==============================================================================

# 1. BRONZE LAYER: Pipeline-Scoped Temporary Streaming View
# We define a temporary streaming view. It does NOT persist the data to storage.
# It is used for in-memory, intermediate transformations within this DAG run.
@dp.temporary_view(
    name="src_sales_view",
    comment="Intermediate pipeline-scoped stream that converts dates."
)
def src_sales_stream():
    """
    Reads incoming records as an active stream from sales source table,
    performs date transformation, and makes it available as an in-memory view.
    """
    df = spark.readStream.table("sdp_catalog.source.sales")
    # Formats the raw string date into a standard DATE type incrementally
    df = df.withColumn("date", to_date(col("date"), "MM-dd-yyyy"))
    return df


# 2. SILVER LAYER: Incremental Streaming Table
# We use @dp.table to create a persistent streaming table in storage.
# This processes incoming records from the temporary view incrementally.
@dp.table(
    name="enr_sales_stream",
    comment="Persistent streaming table with 5% revenue markup."
)
def enr_sales():
    """
    Reads from the intermediate 'src_sales_view' stream, applies 
    a 5% revenue markup, and writes records into a persistent streaming table.
    """
    df = spark.readStream.table("src_sales_view")
    df = df.withColumn("revenue", col("revenue") * 1.05)
    return df


# 3. GOLD LAYER: Streaming Aggregation Table
# Continually updates aggregated counts of daily revenue as new records flow.
@dp.table(
    name="cur_sales_stream",
    comment="Gold layer persistent streaming table for aggregated daily revenue."
)
def cur_sales():
    """
    Reads incrementally from the enriched 'enr_sales_stream' table,
    performs daily aggregations, and updates the final target table.
    """
    df = spark.readStream.table("enr_sales_stream")
    # Computes running sum of daily revenue incrementally
    df = df.groupBy("date").agg(sum("revenue").alias("total_revenue"))
    return df

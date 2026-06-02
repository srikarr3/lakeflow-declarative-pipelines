from pyspark import pipelines as dp 
from pyspark.sql.functions import col, to_date, sum

# ==============================================================================
# BATCH MEDALLION PIPELINE DAG
# ==============================================================================
# This pipeline demonstrates a basic batch-oriented ETL pipeline using 
# Spark Declarative Pipelines (SDP) and Materialized Views. 
#
# Lineage is resolved dynamically by Spark based on table references in spark.read.table.
# ==============================================================================

# 1. BRONZE LAYER: Ingestion and Basic Schema Alignment
# We define a Materialized View to read raw sales data from Unity Catalog 
# and cast string dates to a proper DATE type.
@dp.materialized_view(
    name="src_sales",
    comment="Bronze layer sales table with aligned date format."
)
def src_sales():
    """
    Reads the raw sales data from the catalog, converts the date string 
    into a standardized date format, and stores it as a materialized view.
    """
    df = spark.read.table("sdp_catalog.source.sales")
    # Aligns the string date from MM-dd-yyyy into a standard date object
    df = df.withColumn("date", to_date(col("date"), "MM-dd-yyyy"))
    return df


# 2. SILVER LAYER: Data Enrichment
# We reference the previously defined 'src_sales' materialized view.
# Here we enrich the data by applying a markup/tax adjustment (5%) to the revenue.
@dp.materialized_view(
    name="enr_sales",
    comment="Silver layer enriched sales table with 5% revenue markup applied."
)
def enr_sales():
    """
    Reads from the 'src_sales' materialized view, performs a mathematical 
    enrichment on the revenue column, and registers a new materialized view.
    """
    df = spark.read.table("src_sales")
    # Multiplies the revenue column by 1.05 to reflect a 5% markup
    df = df.withColumn("revenue", col("revenue") * 1.05)
    return df


# 3. GOLD LAYER: Aggregations & Business Reporting
# Summarizes sales data by date to calculate total daily revenue.
@dp.materialized_view(
    name="cur_sales",
    comment="Gold layer daily revenue aggregation."
)
def cur_sales():
    """
    Reads from the enriched 'enr_sales' view, groups the data by date, 
    and computes the daily sum of revenue for analytics and dashboards.
    """
    df = spark.read.table("enr_sales")
    # Groups the records by 'date' and sums the 'revenue' column
    df = df.groupBy("date").agg(sum("revenue").alias("total_revenue"))
    return df
